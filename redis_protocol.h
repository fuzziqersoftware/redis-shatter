#include <stdint.h>

#include "resource.h"
#include "redis_socket.h"

#define ERROR_UNKNOWN_PROTOCOL     1001
#define ERROR_INCOMPLETE_LINE      1002
#define ERROR_PROTOCOL             1003

typedef struct {
  void* data;
  int size;
} redis_argument;

typedef struct {
	resource res;

  int num_args;
  redis_argument args[0];
} redis_command;

redis_command* redis_command_new(int num_args);
void redis_command_free(redis_command*);

redis_command* redis_receive_command(redis_socket* sock);
void redis_receive_reply(redis_socket* sock);

void redis_send_command(redis_socket* sock, redis_command* cmd);

void redis_send_string_response(redis_socket* sock, const char* string, int error);
