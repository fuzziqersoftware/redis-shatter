#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "redis_protocol.h"
#include "redis_server.h"
#include "debug.h"


int main(int argc, char* argv[]) {

  if (build_command_definitions()) {
    printf("static initialization failure; change parameters and recompile\n");
    return -1;
  }

  signal(SIGPIPE, SIG_IGN);
  redis_listen(6379, redis_server_thread);
  return 0;
}
