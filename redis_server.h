#ifndef __REDIS_SERVER_H
#define __REDIS_SERVER_H

#include "redis_socket.h"

int build_command_definitions();

void redis_server_thread(redis_socket* sock);

#endif // __REDIS_SERVER_H
