#define _STDC_FORMAT_MACROS
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach/thread_policy.h>
#include <mach/thread_act.h>
#endif

#include <phosg/Filesystem.hh>
#include <phosg/JSON.hh>
#include <phosg/Network.hh>
#include <phosg/Strings.hh>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "Proxy.hh"

using namespace std;


bool set_thread_affinity(pthread_t thread, int64_t cpu_id) {
#ifdef __APPLE__
  thread_affinity_policy_data_t pd;
  pd.affinity_tag = cpu_id + 1;
  return thread_policy_set(pthread_mach_thread_np(thread),
      THREAD_AFFINITY_POLICY, (thread_policy_t)&pd,
      THREAD_AFFINITY_POLICY_COUNT) == 0;

#else // Linux
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_id, &cpuset);
  return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset) == 0;
#endif
}


bool should_exit = false;

void sigint_handler(int signum) {
  should_exit = true;
}


struct Options {
  struct ProxyOptions {
    size_t num_threads;
    int64_t affinity_cpus;

    string listen_addr;
    int port;
    int listen_fd;

    vector<string> backend_netlocs;
    unordered_set<string> commands_to_disable;

    int hash_begin_delimiter;
    int hash_end_delimiter;

    ProxyOptions() : num_threads(1), affinity_cpus(0), listen_addr(""),
        port(6379), listen_fd(-1), backend_netlocs(), commands_to_disable(),
        hash_begin_delimiter(-1), hash_end_delimiter(-1) { }

    void print(FILE* stream, const char* name) const {
      fprintf(stream, "[%s] %zu worker thread(s)\n", name, this->num_threads);
      if (this->affinity_cpus) {
        fprintf(stream, "[%s] set thread affinity for cores with mask %016" PRIX64 "\n",
            name, this->affinity_cpus);
      } else {
        fprintf(stream, "[%s] don\'t set thread affinity\n", name);
      }
      if (this->listen_fd >= 0) {
        fprintf(stream, "[%s] accept connections on fd %d\n", name,
            this->listen_fd);
      } else if (!this->listen_addr.empty()) {
        fprintf(stream, "[%s] listen on %s:%d\n", name,
            this->listen_addr.c_str(), this->port);
      } else {
        fprintf(stream, "[%s] listen on port %d on all interfaces\n", name,
            this->port);
      }

      for (const auto& backend_netloc : this->backend_netlocs) {
        fprintf(stream, "[%s] register backend %s\n", name,
            backend_netloc.c_str());
      }

      for (const auto& command : this->commands_to_disable) {
        fprintf(stream, "[%s] disable command %s\n", name, command.c_str());
      }

      if (this->hash_begin_delimiter) {
        fprintf(stream, "[%s] hash begin delimiter is %c\n", name,
            this->hash_begin_delimiter);
      }
      if (this->hash_end_delimiter) {
        fprintf(stream, "[%s] hash end delimiter is %c\n", name,
            this->hash_end_delimiter);
      }
    }

    void validate() const {
      if (this->backend_netlocs.empty()) {
        throw invalid_argument("no backends specified");
      }
    }
  };

  unordered_map<string, ProxyOptions> name_to_proxy_options;

  Options() = delete;
  Options(Options&&) = default;
  Options(const Options&) = default;
  Options(const char* filename) {
    string json;
    if (!strcmp(filename, "-")) {
      scoped_fd fd(0);
      json = read_all(fd);
    } else {
      scoped_fd fd(filename, O_RDONLY);
      json = read_all(fd);
    }
    shared_ptr<JSONObject> config = JSONObject::parse(json);

    if (!config->is_dict()) {
      throw invalid_argument("configuration is not a dictionary");
    }

    for (const auto& proxy_config_it : config->as_dict()) {
      const string& proxy_name = proxy_config_it.first;
      const auto& proxy_config = proxy_config_it.second->as_dict();

      ProxyOptions& options = this->name_to_proxy_options.emplace(
          piecewise_construct, forward_as_tuple(proxy_name), forward_as_tuple())
          .first->second;

      try {
        options.num_threads = proxy_config.at("num_threads")->as_int();
        if (options.num_threads == 0) {
          options.num_threads = thread::hardware_concurrency();
        }
      } catch (const JSONObject::key_error& e) { }

      try {
        options.affinity_cpus = proxy_config.at("affinity_cpus")->as_int();
      } catch (const JSONObject::key_error& e) { }

      try {
        options.listen_addr = proxy_config.at("interface")->as_string();
      } catch (const JSONObject::key_error& e) { }

      try {
        options.port = proxy_config.at("port")->as_int();
      } catch (const JSONObject::key_error& e) { }

      try {
        const auto& s = proxy_config.at("hash_field_begin")->as_string();
        if (s.size() != 1) {
          throw invalid_argument("hash_field_begin is not a 1-char string");
        }
        options.hash_begin_delimiter = s[0];
      } catch (const JSONObject::key_error& e) { }

      try {
        const auto& s = proxy_config.at("hash_field_end")->as_string();
        if (s.size() != 1) {
          throw invalid_argument("hash_field_end is not a 1-char string");
        }
        options.hash_end_delimiter = s[0];
      } catch (const JSONObject::key_error& e) { }

      try {
        for (const auto& command : proxy_config.at("disable_commands")->as_list()) {
          options.commands_to_disable.emplace(command->as_string());
        }
      } catch (const JSONObject::key_error& e) { }

      try {
        for (const auto& backend_it : proxy_config.at("backends")->as_dict()) {
          const auto& backend_name = backend_it.first;
          const auto& backend_netloc = backend_it.second->as_string();

          options.backend_netlocs.emplace_back(string_printf("%s@%s",
              backend_netloc.c_str(), backend_name.c_str()));
        }
      } catch (const JSONObject::key_error& e) { }
    }
  }

  void print(FILE* stream) const {
    fprintf(stream, "%zu proxy instance(s) defined\n",
        this->name_to_proxy_options.size());
    for (const auto& it : this->name_to_proxy_options) {
      it.second.print(stream, it.first.c_str());
    }
  }

  void validate() const {
    for (const auto& it : this->name_to_proxy_options) {
      it.second.validate();
    }
  }
};



int main(int argc, char** argv) {

  log(INFO, "> fuzziqer software redis-shatter");

  // parse command-line args
  if (argc > 2) {
    log(ERROR, "usage: %s [config-filename]", argv[0]);
    return 1;
  }
  const char* config_filename = (argc == 2) ? argv[1] : "redis-shatter.conf.json";
  Options opt(config_filename);
  opt.print(stderr);
  opt.validate();

  srand(getpid() ^ time(NULL));
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, sigint_handler);

  vector<thread> threads;
  vector<unique_ptr<Proxy>> proxies;

  // start all the proxies
  vector<size_t> cpu_to_thread_count(thread::hardware_concurrency());
  for (auto& proxy_options_it : opt.name_to_proxy_options) {
    const char* proxy_name = proxy_options_it.first.c_str();
    auto& proxy_options = proxy_options_it.second;

    // if there's no listening socket from a parent process, open a new one
    if (proxy_options.listen_fd == -1) {
      proxy_options.listen_fd = listen(proxy_options.listen_addr,
          proxy_options.port, SOMAXCONN);
      if (!proxy_options.listen_addr.empty()) {
        log(INFO, "[%s] opened server socket %d on %s:%d", proxy_name,
            proxy_options.listen_fd, proxy_options.listen_addr.c_str(),
            proxy_options.port);
      } else {
        log(INFO, "[%s] opened server socket %d on port %d", proxy_name,
            proxy_options.listen_fd, proxy_options.port);
      }

    } else {
      fprintf(stderr, "[%s] using server socket %d from parent process\n",
          proxy_name, proxy_options.listen_fd);
    }

    evutil_make_socket_nonblocking(proxy_options.listen_fd);

    auto hosts = ConsistentHashRing::Host::parse_netloc_list(
        proxy_options.backend_netlocs, 6379);
    shared_ptr<Proxy::Stats> stats(new Proxy::Stats());

    fprintf(stderr, "[%s] starting %zu proxy instances\n", proxy_name,
        proxy_options.num_threads);
    while (threads.size() < proxy_options.num_threads) {
      proxies.emplace_back(new Proxy(proxy_options.listen_fd, hosts,
          proxy_options.hash_begin_delimiter, proxy_options.hash_end_delimiter,
          stats, proxies.size()));
      for (const auto& command : proxy_options.commands_to_disable) {
        proxies.back()->disable_command(command);
      }

      // run the thread on the least-loaded cpu
      int64_t min_load_cpu = -1;
      for (int64_t cpu_id = 0; cpu_id < cpu_to_thread_count.size(); cpu_id++) {
        if ((proxy_options.affinity_cpus & (1 << cpu_id)) &&
            ((min_load_cpu < 0) ||
             (cpu_to_thread_count[cpu_id] < cpu_to_thread_count[min_load_cpu]))) {
          min_load_cpu = cpu_id;
        }
      }

      threads.emplace_back(&Proxy::serve, proxies.back().get());
      if (min_load_cpu >= 0) {
        if (set_thread_affinity(threads.back().native_handle(), min_load_cpu)) {
          cpu_to_thread_count[min_load_cpu]++;
          fprintf(stderr, "[%s] created worker thread on core %" PRId64 "\n",
              proxy_name, min_load_cpu);
        } else {
          fprintf(stderr, "[%s] created worker thread, but failed to bind to core %" PRId64 "\n",
              proxy_name, min_load_cpu);
        }
      } else {
        fprintf(stderr, "[%s] created worker thread\n", proxy_name);
      }
    }
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

  return 0;
}
