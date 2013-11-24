#ifndef __REDIS_PROTOCOL_H
#define __REDIS_PROTOCOL_H

#include <stdint.h>

#include "resource.h"
#include "redis_socket.h"

#define ERROR_UNKNOWN_PROTOCOL     1001
#define ERROR_INCOMPLETE_LINE      1002
#define ERROR_PROTOCOL             1003

#define RESPONSE_STATUS    '+'
#define RESPONSE_ERROR     '-'
#define RESPONSE_INTEGER   ':'
#define RESPONSE_DATA      '$'
#define RESPONSE_MULTI     '*'

#define STATUS_REPLY_BUFFER_LEN   512

typedef struct {
  void* data;
  int size;
} redis_argument;

typedef struct {
  resource res;

  int num_args;
  redis_argument args[0];
} redis_command;

typedef struct _redis_response {
  resource res;

  uint8_t response_type;
  union {
    char status_str[0]; // for RESPONSE_STATUS and RESPONSE_ERROR
    int64_t int_value; // for RESPONSE_INTEGER
    struct {
      int64_t size;
      uint8_t data[0];
    } data_value;
    struct {
      int64_t num_fields;
      struct _redis_response* fields[0];
    } multi_value;
  };
} redis_response;

redis_command* redis_command_create(void* resource_parent, int num_args);
void redis_command_delete(redis_command*);

redis_response* redis_response_create(void* resource_parent, uint8_t type, int64_t size);
redis_response* redis_response_printf(void* resource_parent, uint8_t type, const char* fmt, ...);
void redis_response_delete(redis_response*);

redis_command* redis_receive_command(void* resource_parent, redis_socket* sock);
redis_response* redis_receive_response(void* resource_parent, redis_socket* sock);

void redis_send_command(redis_socket* sock, redis_command* cmd);
void redis_send_response(redis_socket* sock, redis_response* resp);

void redis_send_string_response(redis_socket* sock, const char* string, char sentinel);

#endif // __REDIS_PROTOCOL_H
