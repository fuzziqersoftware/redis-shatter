#ifndef __REDIS_PROTOCOL_H
#define __REDIS_PROTOCOL_H

#include <event2/buffer.h>
#include <stdint.h>

#include "resource.h"

#define ERROR_NONE              0
#define ERROR_UNEXPECTED_INPUT  1
#define ERROR_MEMORY            2
#define ERROR_UNKNOWN_STATE     3
#define ERROR_BUFFER_COPY       4

#define RESPONSE_STATUS    '+'
#define RESPONSE_ERROR     '-'
#define RESPONSE_INTEGER   ':'
#define RESPONSE_DATA      '$'
#define RESPONSE_MULTI     '*'


////////////////////////////////////////////////////////////////////////////////
// command/response manipulation

struct redis_argument {
  void* data;
  int size;
  int annotation;
};

struct redis_command {
  struct resource res;

  int external_arg_data;

  int num_args;
  struct redis_argument args[0];
};

struct redis_response {
  struct resource res;

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
      struct redis_response* fields[0];
    } multi_value;
  };
};

struct redis_command* redis_command_create(void* resource_parent, int num_args);
void redis_command_print(struct redis_command*, int indent);

struct redis_response* redis_response_create(void* resource_parent, uint8_t type, int64_t size);
struct redis_response* redis_response_printf(void* resource_parent, uint8_t type, const char* fmt, ...);
void redis_response_print(struct redis_response*, int indent);

int redis_responses_equal(struct redis_response* a, struct redis_response* b);


////////////////////////////////////////////////////////////////////////////////
// command/response input

struct redis_command_parser {
  struct resource res;

  int state;
  int error;

  int num_command_args;
  struct redis_command* command_in_progress;
  int arg_in_progress;
  int arg_in_progress_read_bytes;
};

struct redis_command_parser* redis_command_parser_create(
    void* resource_parent);
struct redis_command* redis_command_parser_continue(void* resource_parent,
    struct redis_command_parser* st, struct evbuffer* buffer);
void redis_command_parser_print(struct redis_command_parser* p, int indent);


struct redis_response_parser {
  struct resource res;

  int state;
  int error;

  struct redis_response* response_in_progress;
  int data_response_bytes_read;

  struct redis_response_parser* multi_in_progress;
  int multi_response_current_field;
};

struct redis_response_parser* redis_response_parser_create(
    void* resource_parent);
struct redis_response* redis_response_parser_continue(void* resource_parent,
    struct redis_response_parser* st, struct evbuffer* buffer);
void redis_response_parser_print(struct redis_response_parser* p, int indent);


////////////////////////////////////////////////////////////////////////////////
// command/response output

void redis_write_command(struct evbuffer* buf, struct redis_command* cmd);

void redis_write_string_response(struct evbuffer* buf, const char* string, char sentinel);
void redis_write_int_response(struct evbuffer* buf, int64_t value, char sentinel);
void redis_write_response(struct evbuffer* buf, struct redis_response* resp);


#endif // __REDIS_PROTOCOL_H
