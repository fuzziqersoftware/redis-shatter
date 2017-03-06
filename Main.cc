#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <phosg/Filesystem.hh>
#include <phosg/Network.hh>
#include <phosg/Strings.hh>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "Proxy.hh"

using namespace std;


bool should_exit = false;

void sigint_handler(int signum) {
  should_exit = true;
}


struct Options {
  size_t num_threads;

  string listen_addr;
  int port;
  int listen_fd;

  vector<string> backend_netlocs;
  unordered_set<string> commands_to_disable;

  int hash_begin_delimiter;
  int hash_end_delimiter;

  Options() : num_threads(1), listen_addr(""), port(6379), listen_fd(-1),
      hash_begin_delimiter(-1), hash_end_delimiter(-1) { }

  void print(FILE* stream) const {
    fprintf(stream, "proxy configuration:\n");
    fprintf(stream, "  worker threads       : %zu\n", this->num_threads);

    if (!this->listen_addr.empty()) {
      fprintf(stream, "  service address      : %s\n", this->listen_addr.c_str());
    } else {
      fprintf(stream, "  service address      : NULL\n");
    }
    fprintf(stream, "  service port         : %d\n", this->port);
    fprintf(stream, "  inherited socket     : %d\n", this->listen_fd);

    for (const auto& backend_netloc : this->backend_netlocs) {
      fprintf(stream, "  backend              : %s\n", backend_netloc.c_str());
    }

    if (this->hash_begin_delimiter) {
      fprintf(stream, "  hash begin delimiter : %c\n", this->hash_begin_delimiter);
    } else {
      fprintf(stream, "  hash begin delimiter : NULL\n");
    }
    if (this->hash_end_delimiter) {
      fprintf(stream, "  hash end delimiter   : %c\n", this->hash_end_delimiter);
    } else {
      fprintf(stream, "  hash end delimiter   : NULL\n");
    }
  }

  void execute_option(const char* option) {
    if (!strncmp(option, "--port=", 7)) {
      this->port = atoi(&option[7]);

    } else if (!strncmp(option, "--interface=", 12)) {
      this->listen_addr = &option[12];

    } else if (!strncmp(option, "--listen-fd=", 12)) {
      this->listen_fd = atoi(&option[12]);

    } else if (!strncmp(option, "--parallel=", 11)) {
      this->num_threads = atoi(&option[11]);

    } else if (!strncmp(option, "--config-file=", 14)) {
      this->execute_options_from_file(&option[14]);

    } else if (!strncmp(option, "--hash-field-begin=", 19)) {
      this->hash_begin_delimiter = option[19];

    } else if (!strncmp(option, "--hash-field-end=", 17)) {
      this->hash_end_delimiter = option[17];

    } else if (!strncmp(option, "--backend=", 10)) {
      this->backend_netlocs.emplace_back(&option[10]);

    } else if (!strncmp(option, "--disable-command=", 18)) {
      this->commands_to_disable.emplace(&option[18]);

    } else {
      throw invalid_argument(string_printf("unrecognized option: \"%s\"\n", option));
    }
  }

  void execute_options_from_file(const char* filename) {
    auto f = fopen_unique(filename, "rt");

    // this should be enough for any reasonable options
    const int line_buffer_size = 512;
    char line_buffer[line_buffer_size];
    while (fgets(line_buffer, line_buffer_size, f.get())) {
      // get rid of comments first
      int x;
      for (x = 0; line_buffer[x]; x++) {
        if (line_buffer[x] == '#') {
          break;
        }
      }
      line_buffer[x] = 0;

      // get rid of trailing whitespace
      for (x--; x >= 0 && (line_buffer[x] == ' ' || line_buffer[x] == '\t' || line_buffer[x] == '\r' || line_buffer[x] == '\n'); x--);
      if (x >= 0) {
        line_buffer[x + 1] = 0;
        this->execute_option(line_buffer);
      }
    }
  }

  void execute_options_from_command_line(int argc, char** argv) {
    if (argc == 1) {
      log(INFO, "no command-line options given; using redis-shatter.conf");
      this->execute_options_from_file("redis-shatter.conf");
    } else {
      int x;
      for (x = 1; x < argc; x++) {
        this->execute_option(argv[x]);
      }
    }
  }
};



int main(int argc, char** argv) {

  log(INFO, "> fuzziqer software redis-shatter");

  // parse command-line args
  Options opt;
  opt.execute_options_from_command_line(argc, argv);
  opt.print(stderr);

  // sanity-check options
  if (opt.backend_netlocs.empty()) {
    log(ERROR, "no backends specified");
    return 2;
  }
  if (opt.num_threads < 1) {
    log(ERROR, "at least 1 thread must be running");
    return 2;
  }

  srand(getpid() ^ time(NULL));
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, sigint_handler);

  // if there's no listening socket from a parent process, open a new one
  if (opt.listen_fd == -1) {
    opt.listen_fd = listen(opt.listen_addr, opt.port, SOMAXCONN);
    if (!opt.listen_addr.empty()) {
      log(INFO, "opened server socket %d on %s:%d", opt.listen_fd,
          opt.listen_addr.c_str(), opt.port);
    } else {
      log(INFO, "opened server socket %d on port %d", opt.listen_fd, opt.port);
    }

  } else {
    fprintf(stderr, "note: inherited server socket %d from parent process\n",
        opt.listen_fd);
  }

  evutil_make_socket_nonblocking(opt.listen_fd);

  // if there's only one thread, just have the main thread do the work
  auto hosts = ConsistentHashRing::Host::parse_netloc_list(opt.backend_netlocs,
      6379);
  shared_ptr<Proxy::Stats> stats(new Proxy::Stats());
  if (opt.num_threads == 1) {
    Proxy p(opt.listen_fd, hosts, opt.hash_begin_delimiter,
        opt.hash_end_delimiter, stats, 0);
    fprintf(stderr, "ready for connections\n");
    p.serve();

  } else {
    fprintf(stderr, "starting %zu proxy instances\n", opt.num_threads);
    vector<thread> threads;
    vector<unique_ptr<Proxy>> proxies;
    while (threads.size() < opt.num_threads) {
      proxies.emplace_back(new Proxy(opt.listen_fd, hosts,
          opt.hash_begin_delimiter, opt.hash_end_delimiter, stats,
          proxies.size()));
      threads.emplace_back(&Proxy::serve, proxies.back().get());
    }

    fprintf(stderr, "ready for connections\n");
    sigset_t sigset;
    sigemptyset(&sigset);
    while (!should_exit) {
      sigsuspend(&sigset);
    }

    fprintf(stderr, "stopping proxy instances\n");
    for (auto& p : proxies) {
      p->stop();
    }

    fprintf(stderr, "waiting for proxy instances to terminate\n");
    for (auto& t : threads) {
      t.join();
    }
  }

  return 0;
}
