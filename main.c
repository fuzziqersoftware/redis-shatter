#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "debug.h"
#include "network.h"

#include "redis_proxy.h"



struct options {
  struct resource res;

  int num_bad_arguments;

  int num_backends;
  int num_processes;
  int process_num;
  int port;
  int listen_fd;
  char** backend_netlocs;
};

static void free_options(struct options* opt) {
  if (opt->backend_netlocs)
    free(opt->backend_netlocs);
  free(opt);
}

// yay mutual recursion
int execute_options_from_file(struct options* opt, const char* filename);

void execute_option(struct options* opt, const char* option) {
  if (!strncmp(option, "--port=", 7))
    opt->port = atoi(&option[7]);

  else if (!strncmp(option, "--listen-fd=", 12))
    opt->listen_fd = atoi(&option[12]);

  else if (!strncmp(option, "--parallel=", 11))
    opt->num_processes = atoi(&option[11]);

  else if (!strncmp(option, "--config-file=", 14)) {
    if (!execute_options_from_file(opt, &option[14])) {
      printf("error: can\'t read from config file %s\n", &option[14]);
      opt->num_bad_arguments++;
    }

  } else if (!strncmp(option, "--backend=", 10)) {
    opt->num_backends++;
    opt->backend_netlocs = (char**)realloc(opt->backend_netlocs,
        sizeof(char*) * opt->num_backends);
    opt->backend_netlocs[opt->num_backends - 1] = (char*)resource_malloc(opt,
        strlen(&option[10]) + 1, free);
    strcpy(opt->backend_netlocs[opt->num_backends - 1], &option[10]);

  } else {
    printf("error: unrecognized option: \"%s\"\n", option);
    opt->num_bad_arguments++;
  }
}

int execute_options_from_file(struct options* opt, const char* filename) {
  FILE* f = fopen(filename, "rt");
  if (!f)
    return 0;

  // this should be enough for any reasonable options
  const int line_buffer_size = 512;
  char line_buffer[line_buffer_size];
  while (fgets(line_buffer, line_buffer_size, f)) {
    // get rid of comments first
    int x;
    for (x = 0; line_buffer[x]; x++)
      if (line_buffer[x] == '#')
        break;
    line_buffer[x] = 0;

    // get rid of trailing whitespace
    for (x--; x >= 0 && (line_buffer[x] == ' ' || line_buffer[x] == '\t' || line_buffer[x] == '\r' || line_buffer[x] == '\n'); x--);
    if (x >= 0) {
      line_buffer[x + 1] = 0;
      execute_option(opt, line_buffer);
    }
  }

  fclose(f);
  return 1;
}

void execute_options_from_command_line(struct options* opt, int argc,
    char** argv) {

  if (argc == 1) {
    printf("note: no command-line options given; using redis-shatter.conf\n");
    execute_options_from_file(opt, "redis-shatter.conf");
  } else {
    int x;
    for (x = 1; x < argc; x++)
      execute_option(opt, argv[x]);
  }
}



int main(int argc, char **argv) {

  printf("> fuzziqer software redis-shatter\n");

  // parse command-line args
  struct options* opt = (struct options*)resource_calloc(NULL,
      sizeof(struct options), free_options);
  opt->num_processes = 1;
  opt->port = DEFAULT_REDIS_PORT;
  opt->listen_fd = -1;
  execute_options_from_command_line(opt, argc, argv);

  // sanity-check options
  if (opt->num_bad_arguments)
    return 1;
  if (opt->num_backends == 0) {
    printf("error: no backends specified\n");
    return 2;
  }
  if (opt->num_processes < 1) {
    printf("error: at least 1 process must be running\n");
    return 2;
  }

  // do static initialization
  signal(SIGPIPE, SIG_IGN);
  if (build_command_definitions()) {
    printf("static initialization failure; change parameters and recompile\n");
    return 3;
  }

  // if there's no listening socket from a parent process, open a new one
  if (opt->listen_fd == -1) {
    opt->listen_fd = network_listen(NULL, opt->port, SOMAXCONN);
    if (opt->listen_fd < 0) {
      printf("error: can\'t open server socket: %s\n",
          network_error_str(opt->listen_fd));
      return opt->listen_fd;
    }
    printf("opened server socket %d on port %d\n", opt->listen_fd, opt->port);

  } else
    printf("note: inherited server socket %d from parent process\n",
        opt->listen_fd);

  // fork child workers if desired
  if (opt->num_processes > 1) {
    for (opt->process_num = 0; opt->process_num < opt->num_processes;
        opt->process_num++) {
      pid_t child_pid = fork();
      if (child_pid == 0)
        break;
      printf("started worker process %d\n", child_pid);
    }
    if (opt->process_num == opt->num_processes) {
      while (opt->num_processes) {
        // master process monitors the children and doesn't serve anything
        int exit_status;
        pid_t terminated_pid = wait(&exit_status);
        if (WIFSIGNALED(exit_status))
          printf("worker %d terminated due to signal %d\n", terminated_pid,
              WTERMSIG(exit_status));
        else if (WIFEXITED(exit_status))
          printf("worker %d exited with code %d\n", terminated_pid,
              WEXITSTATUS(exit_status));
        else
          printf("worker %d terminated for unknown reasons; exit_status = %d\n",
              terminated_pid, exit_status);
      }
      printf("all workers have terminated; exiting\n");
      exit(0);
    }
  }

  // create the proxy and serve
  struct redis_proxy* proxy = redis_proxy_create(NULL, opt->listen_fd,
      (const char**)opt->backend_netlocs, opt->num_backends);
  if (!proxy) {
    printf("error: couldn\'t start proxy\n");
    return -1;
  }
  proxy->num_processes = opt->num_processes;
  proxy->process_num = opt->process_num;

  printf("proxy is now ready\n");
  redis_proxy_serve(proxy);

  resource_delete(proxy, 1);
  resource_delete(opt, 1);
  return 0;
}
