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


int main(int argc, char **argv) {

  printf("> fuzziqer software redis-shatter\n");

  // parse command-line args
  int port = DEFAULT_REDIS_PORT, listen_fd = -1;
  int num_backends = 0;
  int num_processes = 1;
  const char** backend_netlocs = NULL;
  int x, num_bad_arguments = 0;
  for (x = 1; x < argc; x++) {
    if (argv[x][0] == '-') {
      if (!strncmp(argv[x], "--port=", 7))
        port = atoi(&argv[x][7]);
      else if (!strncmp(argv[x], "--listen-fd=", 12))
        listen_fd = atoi(&argv[x][12]);
      else if (!strncmp(argv[x], "--parallel=", 11))
        num_processes = atoi(&argv[x][11]);
      else {
        printf("error: unrecognized command-line option: %s\n", argv[x]);
        num_bad_arguments++;
      }
    } else {
      num_backends++;
      backend_netlocs = (const char**)realloc(backend_netlocs,
          sizeof(const char*) * num_backends);
      backend_netlocs[num_backends - 1] = argv[x];
    }
  }

  // make sure there were no bad args and we have at least one backend
  if (num_bad_arguments)
    return 1;
  if (num_backends == 0) {
    printf("error: no backends specified\n");
    return 2;
  }
  if (num_processes < 1) {
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
  if (listen_fd == -1) {
    listen_fd = network_listen(NULL, port, SOMAXCONN);
    if (listen_fd < 0) {
      printf("error: can\'t open server socket: %s\n",
          network_error_str(listen_fd));
      return listen_fd;
    }
    printf("opened server socket %d on port %d\n", listen_fd, port);

  } else
    printf("note: inherited server socket %d from parent process\n", listen_fd);

  // fork child workers if desired
  int process_num = 0;
  if (num_processes > 1) {
    for (process_num = 0; process_num < num_processes; process_num++) {
      pid_t child_pid = fork();
      if (child_pid == 0)
        break;
      printf("started worker process %d\n", child_pid);
    }
    if (process_num == num_processes) {
      while (num_processes) {
        // master process monitors the children and doesn't serve anything
        int exit_status;
        pid_t terminated_pid = wait(&exit_status);
        if (WIFSIGNALED(exit_status))
          printf("worker %d terminated due to signal %d\n", terminated_pid, WTERMSIG(exit_status));
        else if (WIFEXITED(exit_status))
          printf("worker %d exited with code %d\n", terminated_pid, WEXITSTATUS(exit_status));
        else
          printf("worker %d terminated for unknown reasons; exit_status = %d\n", terminated_pid, exit_status);
      }
      printf("all workers have terminated; exiting\n");
      exit(0);
    }
  }

  // create the proxy and serve
  struct redis_proxy* proxy = redis_proxy_create(NULL, listen_fd,
      backend_netlocs, num_backends);
  if (!proxy) {
    printf("error: couldn\'t start proxy\n");
    return -1;
  }
  proxy->num_processes = num_processes;
  proxy->process_num = process_num;

  printf("proxy is now ready\n");
  redis_proxy_serve(proxy);
  return 0;
}
