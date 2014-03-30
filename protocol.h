#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include <event2/buffer.h>
#include <stdint.h>

#include "resource.h"

#define DEFAULT_REDIS_PORT  6379

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

struct argument {
  void* data;
  int size;
  int annotation;
};

struct command {
  struct resource res;

  int external_arg_data;

  int num_args;
  struct argument args[0];
};

struct response {
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
      struct response* fields[0];
    } multi_value;
  };
};

struct command* command_create(void* resource_parent, int num_args);
void command_print(const struct command*, int indent);

struct response* response_create(void* resource_parent, uint8_t type, int64_t size);
struct response* response_printf(void* resource_parent, uint8_t type, const char* fmt, ...);
void response_print(const struct response*, int indent);

int responses_equal(const struct response* a, const struct response* b);


////////////////////////////////////////////////////////////////////////////////
// command/response input

struct command_parser {
  struct resource res;

  int state;
  int error;

  int num_command_args;
  struct command* command_in_progress;
  int arg_in_progress;
  int arg_in_progress_read_bytes;

  struct command* (*resume)(void* resource_parent,
      struct command_parser* st, struct evbuffer* buffer);
};

struct command_parser* command_parser_create(void* resource_parent);
void command_parser_print(const struct command_parser* p, int indent);
struct command* command_parser_continue(void* resource_parent,
    struct command_parser* st, struct evbuffer* buf);


struct response_parser {
  struct resource res;

  int state;
  int error;

  struct response* response_in_progress;
  int data_response_bytes_read;

  struct response_parser* multi_in_progress;
  int multi_response_current_field;

  int forward_items_remaining;

  struct response* (*resume)(void* resource_parent,
      struct response_parser* st, struct evbuffer* buffer);
  int (*resume_forward)(struct response_parser* st,
      struct evbuffer* buffer, struct evbuffer* output_buffer);
};

struct response_parser* response_parser_create(void* resource_parent);
void response_parser_print(const struct response_parser* p, int indent);
struct response* response_parser_continue(void* resource_parent,
    struct response_parser* st, struct evbuffer* buf);
int response_parser_continue_forward(struct response_parser* st,
    struct evbuffer* buf, struct evbuffer* output_buffer);


////////////////////////////////////////////////////////////////////////////////
// command/response output

void command_write(struct evbuffer* buf, const struct command* cmd);

void response_write_string(struct evbuffer* buf, const char* string,
    char sentinel);
void response_write_int(struct evbuffer* buf, int64_t value, char sentinel);
void response_write(struct evbuffer* buf, const struct response* resp);

#endif // __PROTOCOL_H
