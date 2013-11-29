#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "debug.h"

#include "redis_socket.h"
#include "redis_server.h"
#include "redis_multiclient.h"



void sigquit(int sig) {
  redis_interrupt_server();
}

int main(int argc, char* argv[]) {

  printf("> fuzziqer software redis-shatter\n");

  int port = DEFAULT_REDIS_PORT;
  int num_backends = 0;
  const char** backend_netlocs = NULL;
  int x, num_bad_arguments = 0;
  for (x = 1; x < argc; x++) {
    if (argv[x][0] == '-') {
      if (!strncmp(argv[x], "--port=", 7))
        port = atoi(&argv[x][7]);
      else {
        printf("error: unrecognized command-line option: %s\n", argv[x]);
        num_bad_arguments++;
      }
    } else {
      num_backends++;
      backend_netlocs = (const char**)realloc(backend_netlocs, sizeof(const char*) * num_backends);
      backend_netlocs[num_backends - 1] = argv[x];
    }
  }

  if (num_bad_arguments)
    return 1;
  if (num_backends == 0) {
    printf("error: no backends specified\n");
    return 2;
  }

  signal(SIGPIPE, SIG_IGN);
  if (build_command_definitions()) {
    printf("static initialization failure; change parameters and recompile\n");
    return 3;
  }

  redis_multiclient* mc = redis_multiclient_create(NULL, num_backends, backend_netlocs);
  free(backend_netlocs);

  printf("listening on port %d\n", port);
  signal(SIGQUIT, sigquit);
  redis_listen(port, redis_server_thread, mc, 1);
  printf("shutdown complete\n");
  return 0;
}
