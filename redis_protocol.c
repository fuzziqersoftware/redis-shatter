#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "debug.h"

#include "redis_socket.h"
#include "redis_protocol.h"



// command functions

redis_command* redis_command_new(int num_args) {
  redis_command* cmd = (redis_command*)malloc(sizeof(redis_command) + sizeof(redis_argument) * num_args);
  cmd->num_args = num_args;
  printf("created command %p\n", cmd);
  return cmd;
}

void redis_command_free(redis_command* cmd) {
  printf("deleting command %p\n", cmd);
  int x;
  for (x = 0; x < cmd->num_args; x++)
    free(cmd->args[x].data);
  free(cmd);
}



// reading functions

int64_t redis_receive_int_line(redis_socket* sock, char expected_sentinel) {
  char int_buffer[24];
  redis_socket_read_line(sock, int_buffer, 24);
  if (expected_sentinel) {
    if (int_buffer[0] != expected_sentinel)
      redis_error(sock, ERROR_UNKNOWN_PROTOCOL, int_buffer[0]);
    return strtoull(&int_buffer[1], NULL, 0);
  }

  return strtoull(int_buffer, NULL, 0);
}

redis_command* redis_receive_command(redis_socket* sock) {
  int64_t x, num_args = redis_receive_int_line(sock, '*');
  int16_t rn;

  redis_command* cmd = redis_command_new(num_args);
  if (!cmd)
    redis_error(sock, ERROR_OUT_OF_MEMORY, 0);
  resource_create(sock, cmd, redis_command_free);
  for (x = 0; x < num_args; x++) {
    cmd->args[x].size = redis_receive_int_line(sock, '$');
    cmd->args[x].data = malloc(cmd->args[x].size);
    if (!cmd->args[x].data)
      redis_error(sock, ERROR_OUT_OF_MEMORY, cmd->args[x].size); // TODO: this leaks the memory for 'cmd'
    redis_socket_read(sock, cmd->args[x].data, cmd->args[x].size);

    redis_socket_read(sock, &rn, 2); // expect a \r\n
    if (rn != 0x0A0D) // little endian
      redis_error(sock, ERROR_INCOMPLETE_LINE, rn); // TODO: this also leaks
  }

  return cmd;
}

void redis_receive_reply(redis_socket* sock) {
  char reply_type;
  redis_socket_read(sock, &reply_type, 1);
  switch (reply_type) {
    case '+': // ok
    case '-': // error
      // TODO
      // redis_socket_read_line(sock, );
      break;

    case ':': { // integer
      // TODO
      // int64_t num = redis_receive_int_line(sock, 0);
      break; }

    case '$': { // data
      // TODO
      break; }
    case '*': { // compound
      // TODO
      break; }
    default:
      redis_error(sock, ERROR_PROTOCOL, reply_type);
  }
}



// writing functions

void redis_send_command(redis_socket* sock, redis_command* cmd) {
  char size_buffer[24];
  redis_socket_write(sock, size_buffer,
      sprintf(size_buffer, "*%d\r\n", cmd->num_args));

  int x;
  for (x = 0; x < cmd->num_args; x++) {
    redis_socket_write(sock, size_buffer,
      sprintf(size_buffer, "$%d\r\n", cmd->args[x].size));
    redis_socket_write(sock, cmd->args[x].data, cmd->args[x].size);
    redis_socket_write(sock, "\r\n", 2);
  }
}

// TODO void redis_forward_reply(redis_socket* sock, redis_socket* dest_socket);

void redis_send_string_response(redis_socket* sock, const char* string, int error) {
  char sentinel = error ? '-' : '+';
  redis_socket_write(sock, &sentinel, 1);
  redis_socket_write(sock, string, strlen(string));
  redis_socket_write(sock, "\r\n", 2);
}
