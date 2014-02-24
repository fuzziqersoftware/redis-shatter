#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "debug.h"
#include "redis_protocol.h"


#define STATUS_REPLY_BUFFER_LEN  512


////////////////////////////////////////////////////////////////////////////////
// command/response manipulation

static void redis_command_delete(struct redis_command* cmd) {
  if (!cmd->external_arg_data) {
    int x;
    for (x = 0; x < cmd->num_args; x++)
      free(cmd->args[x].data);
  }
  free(cmd);
}

struct redis_command* redis_command_create(void* resource_parent, int num_args) {
  struct redis_command* cmd = (struct redis_command*)malloc(sizeof(struct redis_command) + sizeof(struct redis_argument) * num_args);
  if (!cmd)
    return NULL;
  memset(cmd, 0, sizeof(struct redis_command));
  cmd->num_args = num_args;
  resource_create(resource_parent, cmd, redis_command_delete);
  resource_annotate(cmd, "redis_command[%d]", num_args);
  return cmd;
}

void redis_command_print(struct redis_command* cmd) {
  int x, y;
  printf("Command@%p[", cmd);
  for (x = 0; x < cmd->num_args; x++) {
    printf("Arg[%x, %x, %p, ", cmd->args[x].size, cmd->args[x].annotation, cmd->args[x].data);
    for (y = 0; y < cmd->args[x].size; y++) {
      int ch = *((char*)cmd->args[x].data + y);
      if (ch < 0x20 || ch > 0x7F)
        printf("\\x%02X", ch);
      else
        printf("%c", ch);
    }
    printf("], ");
  }
  printf("]\n");
}

struct redis_response* redis_response_create(void* resource_parent, uint8_t type, int64_t size) {
  struct redis_response* response;
  switch (type) {
    case RESPONSE_STATUS:
    case RESPONSE_ERROR: {
      response = (struct redis_response*)malloc(sizeof(struct redis_response) + size + 1);
      if (!response)
        return NULL;
      response->response_type = type;
      break; }

    case RESPONSE_INTEGER: {
      response = (struct redis_response*)malloc(sizeof(struct redis_response));
      if (!response)
        return NULL;
      response->response_type = type;
      break; }

    case RESPONSE_DATA: {
      // size can be negative for a RESPONSE_DATA response, indicating a null value
      response = (struct redis_response*)malloc(sizeof(struct redis_response) + (size < 0 ? 0 : size));
      if (!response)
        return NULL;
      response->response_type = type;
      response->data_value.size = size;
      break; }

    case RESPONSE_MULTI: {
      // size can be negative for a RESPONSE_MULTI response, indicating a null value
      response = (struct redis_response*)malloc(sizeof(struct redis_response) + ((size < 0 ? 0 : size) * sizeof(struct redis_command*)));
      if (!response)
        return NULL;
      response->response_type = type;
      response->multi_value.num_fields = size;
      break; }

    default:
      return NULL;
  }

  resource_create(resource_parent, response, free);
  resource_annotate(response, "redis_response[%d, %016llX]", type, size);
  return response;
}

struct redis_response* redis_response_printf(void* resource_parent, uint8_t type, const char* fmt, ...) {
  const int buffer_size = STATUS_REPLY_BUFFER_LEN;
  struct redis_response* resp = redis_response_create(resource_parent, type, buffer_size);

  va_list va;
  va_start(va, fmt);
  if (type == RESPONSE_DATA) {
    resp->data_value.size = vsnprintf((char*)&resp->data_value.data[0], buffer_size, fmt, va);
  } else {
    vsnprintf(resp->status_str, buffer_size, fmt, va);
  }
  va_end(va);

  return resp;
}

void redis_response_print(struct redis_response* resp) {

  if (!resp) {
    printf("NULL\n");
    return;
  }

  int x;
  switch (resp->response_type) {
    case RESPONSE_STATUS:
      printf("StatusResponse@%p[%s]\n", resp, resp->status_str);
      break;

    case RESPONSE_ERROR:
      printf("ErrorResponse@%p[%s]\n", resp, resp->status_str);
      break;

    case RESPONSE_INTEGER:
      printf("IntegerResponse@%p[%lld]\n", resp, resp->int_value);
      break;

    case RESPONSE_DATA:
      if (resp->data_value.size < 0)
        printf("NullDataResponse@%p\n", resp);
      else {
        printf("DataResponse@%p[%lld, ", resp, resp->data_value.size);
        for (x = 0; x < resp->data_value.size; x++) {
          int ch = *((char*)resp->data_value.data + x);
          if (ch < 0x20 || ch > 0x7F)
            printf("\\x%02X", ch);
          else
            printf("%c", ch);
        }
        printf("]\n");
      }
      break;

    case RESPONSE_MULTI:
      if (resp->data_value.size < 0)
        printf("NullMultiResponse@%p\n", resp);
      else {
        printf("MultiResponse@%p[%lld, \n", resp, resp->multi_value.num_fields);
        for (x = 0; x < resp->multi_value.num_fields; x++) {
          redis_response_print(resp->multi_value.fields[x]);
        }
        printf("]\n");
      }
      break;
  }
}


////////////////////////////////////////////////////////////////////////////////
// command/response input

#define CMDSTATE_INITIAL                      0
#define CMDSTATE_READ_ARG_SIZE                1
#define CMDSTATE_READ_ARG_DATA                2
#define CMDSTATE_READ_NEWLINE_AFTER_ARG_DATA  3

struct redis_command_parser_state* redis_command_parser_create(
    void* resource_parent) {

  struct redis_command_parser_state* st = (struct redis_command_parser_state*)
      calloc(1, sizeof(struct redis_command_parser_state));
  if (!st)
    return NULL;
  resource_create(resource_parent, st, free);
  resource_annotate(st, "redis_command_parser_state[]");
  return st;
}

struct redis_command* redis_command_parser_continue(void* resource_parent,
    struct redis_command_parser_state* st, struct evbuffer* buf) {

  char *input_line;
  size_t len;
  int can_continue = 1;
  struct redis_command* cmd_to_return = NULL;
  while (!st->error && can_continue && !cmd_to_return) {

    switch (st->state) {

      case CMDSTATE_INITIAL: // expect a line like "*NUM_ARGS\r\n"
        input_line = evbuffer_readln(buf, &len, EVBUFFER_EOL_CRLF);
        if (!input_line) {
          can_continue = 0;
          break; // complete line not yet available
        }

        if (input_line[0] != '*')
          st->error = ERROR_UNEXPECTED_INPUT;

        else {
          int num_command_args = atoi(&input_line[1]);
          if (num_command_args > 0) {
            st->command_in_progress = redis_command_create(st, num_command_args);
            if (!st->command_in_progress)
              st->error = ERROR_MEMORY;
            st->arg_in_progress = 0;
            st->state = CMDSTATE_READ_ARG_SIZE;
          }
        }
        break;

      case CMDSTATE_READ_ARG_SIZE: // expect a line like "$ARG_SIZE\r\n"
        input_line = evbuffer_readln(buf, &len, EVBUFFER_EOL_CRLF);
        if (!input_line) {
          can_continue = 0;
          break; // complete line not yet available
        }

        if (input_line[0] != '$')
          st->error = ERROR_UNEXPECTED_INPUT;
        else {
          struct redis_argument* arg = &st->command_in_progress->args[st->arg_in_progress];
          arg->size = atoi(&input_line[1]);
          arg->data = malloc(arg->size);
          if (!arg->data)
            st->error = ERROR_MEMORY;
          st->arg_in_progress_read_bytes = 0;
          st->state = CMDSTATE_READ_ARG_DATA;
        }
        break;

      case CMDSTATE_READ_ARG_DATA: { // copy data directly to the forehead
        struct redis_argument* arg = &st->command_in_progress->args[st->arg_in_progress];
        size_t data_to_read = evbuffer_get_length(buf);
        if (data_to_read > arg->size - st->arg_in_progress_read_bytes)
          data_to_read = arg->size - st->arg_in_progress_read_bytes;
        int res = evbuffer_remove(buf, (char*)arg->data + st->arg_in_progress_read_bytes, data_to_read);
        if (res == -1)
          st->error = ERROR_BUFFER_COPY;
        else
          st->arg_in_progress_read_bytes += res;

        if (st->arg_in_progress_read_bytes == arg->size)
          st->state = CMDSTATE_READ_NEWLINE_AFTER_ARG_DATA;
        break; }

      case CMDSTATE_READ_NEWLINE_AFTER_ARG_DATA:
        input_line = evbuffer_readln(buf, &len, EVBUFFER_EOL_CRLF);
        if (!input_line) {
          can_continue = 0;
          break; // complete line not yet available
        }

        // this arg is done; get ready for the next one
        st->arg_in_progress++;
        if (st->arg_in_progress == st->command_in_progress->num_args) {
          st->state = CMDSTATE_INITIAL;
          // change the parent to what was given
          resource_add_ref(resource_parent, st->command_in_progress);
          resource_delete_ref(st, st->command_in_progress);
          cmd_to_return = st->command_in_progress;
        } else
          st->state = CMDSTATE_READ_ARG_SIZE;
        break;

      default:
        st->error = ERROR_UNKNOWN_STATE;
    }

    if (input_line)
      free(input_line);
    input_line = NULL;
  }
  return cmd_to_return; // no complete command was available
}



#define RSPSTATE_INITIAL                            0
#define RSPSTATE_MULTI_RECURSIVE                    1
#define RSPSTATE_READ_DATA_RESPONSE                 2
#define RSPSTATE_READ_NEWLINE_AFTER_DATA_RESPONSE   3


struct redis_response_parser_state* redis_response_parser_create(
    void* resource_parent) {

  struct redis_response_parser_state* st = (struct redis_response_parser_state*)
      calloc(1, sizeof(struct redis_response_parser_state));
  if (!st)
    return NULL;
  resource_create(resource_parent, st, free);
  resource_annotate(st, "redis_response_parser_state[]");
  return st;
}

struct redis_response* redis_response_parser_continue(void* resource_parent,
    struct redis_response_parser_state* st, struct evbuffer* buf) {

  char *input_line;
  size_t len;
  int can_continue = 1;
  struct redis_response* resp_to_return = NULL;
  while (!st->error && can_continue && !resp_to_return) {

    switch (st->state) {

      case RSPSTATE_INITIAL:
        input_line = evbuffer_readln(buf, &len, EVBUFFER_EOL_CRLF);
        if (!input_line) {
          can_continue = 0;
          break; // complete line not yet available
        }

        switch (input_line[0]) {
          case RESPONSE_STATUS:
          case RESPONSE_ERROR: {
            resp_to_return = redis_response_create(resource_parent,
                input_line[0], len);
            strcpy(resp_to_return->status_str, &input_line[1]);
            break; }

          case RESPONSE_INTEGER: {
            resp_to_return = redis_response_create(resource_parent,
                RESPONSE_INTEGER, 0);
            resp_to_return->int_value = strtoll(&input_line[1], NULL, 0);
            break; }

          case RESPONSE_DATA: {
            int64_t size = strtoll(&input_line[1], NULL, 0);
            st->response_in_progress = redis_response_create(resource_parent,
                RESPONSE_DATA, size);
            if (size < 0)
              resp_to_return = st->response_in_progress;
            else {
              st->data_response_bytes_read = 0;
              st->state = RSPSTATE_READ_DATA_RESPONSE;
            }
            break; }

          case RESPONSE_MULTI: {
            int64_t num_fields = strtoll(&input_line[1], NULL, 0);
            st->response_in_progress = redis_response_create(resource_parent,
                RESPONSE_MULTI, num_fields);
            if (num_fields <= 0)
              resp_to_return = st->response_in_progress;
            else {
              st->multi_in_progress = redis_response_parser_create(st);
              st->multi_response_current_field = 0;
              st->state = RSPSTATE_MULTI_RECURSIVE;
            }
            break; }

          default:
            st->error = ERROR_UNEXPECTED_INPUT;
        }
        break; // RSPSTATE_INITIAL

      case RSPSTATE_MULTI_RECURSIVE: {
        struct redis_response* field = redis_response_parser_continue(
            resource_parent, st->multi_in_progress, buf);
        if (field) {
          st->response_in_progress->multi_value.fields[st->multi_response_current_field] = field;
          st->multi_response_current_field++;
          if (st->multi_response_current_field ==
              st->response_in_progress->multi_value.num_fields) {
            st->state = RSPSTATE_INITIAL;
            resp_to_return = st->response_in_progress;
          }
        }
        break; }

      case RSPSTATE_READ_DATA_RESPONSE: {
        size_t data_to_read = evbuffer_get_length(buf);
        size_t response_bytes_remaining = st->response_in_progress->data_value.size - st->data_response_bytes_read;
        if (data_to_read > response_bytes_remaining)
          data_to_read = response_bytes_remaining;

        int res = evbuffer_remove(buf, st->response_in_progress->data_value.data + st->data_response_bytes_read, data_to_read);
        if (res == -1)
          st->error = ERROR_BUFFER_COPY;
        else
          st->data_response_bytes_read += res;

        if (st->data_response_bytes_read ==
            st->response_in_progress->data_value.size)
          st->state = RSPSTATE_READ_NEWLINE_AFTER_DATA_RESPONSE;
        break; }

      case RSPSTATE_READ_NEWLINE_AFTER_DATA_RESPONSE:
        input_line = evbuffer_readln(buf, &len, EVBUFFER_EOL_CRLF);
        if (!input_line)
          can_continue = 0;
        else {
          st->state = RSPSTATE_INITIAL;
          resp_to_return = st->response_in_progress;
        }
        break;

      default:
        st->error = ERROR_UNKNOWN_STATE;
    }

    if (input_line)
      free(input_line);
    input_line = NULL;
  }
  return resp_to_return;
}


////////////////////////////////////////////////////////////////////////////////
// command/response output

void redis_write_command(struct evbuffer* buf, struct redis_command* cmd) {
  evbuffer_add_printf(buf, "*%d\r\n", cmd->num_args);

  int x;
  for (x = 0; x < cmd->num_args; x++) {
    evbuffer_add_printf(buf, "$%d\r\n", cmd->args[x].size);
    evbuffer_add(buf, cmd->args[x].data, cmd->args[x].size);
    evbuffer_add(buf, "\r\n", 2);
  }
}

void redis_write_string_response(struct evbuffer* buf, const char* string, char sentinel) {
  evbuffer_add_printf(buf, "%c%s\r\n", sentinel, string);
}

void redis_write_int_response(struct evbuffer* buf, int64_t value, char sentinel) {
  evbuffer_add_printf(buf, "%c%lld\r\n", sentinel, value);
}

void redis_write_response(struct evbuffer* buf, struct redis_response* resp) {

  int64_t x;
  switch (resp->response_type) {

    case RESPONSE_STATUS:
    case RESPONSE_ERROR:
      redis_write_string_response(buf, resp->status_str, resp->response_type);
      break;

    case RESPONSE_INTEGER:
      redis_write_int_response(buf, resp->int_value, ':');
      break;

    case RESPONSE_DATA:
      redis_write_int_response(buf, resp->data_value.size, resp->response_type);
      if (resp->data_value.size >= 0) {
        evbuffer_add(buf, resp->data_value.data, resp->data_value.size);
        evbuffer_add(buf, "\r\n", 2);
      }
      break;

    case RESPONSE_MULTI:
      redis_write_int_response(buf, resp->multi_value.num_fields,
          resp->response_type);
      if (resp->multi_value.num_fields >= 0)
        for (x = 0; x < resp->multi_value.num_fields; x++)
          redis_write_response(buf, resp->multi_value.fields[x]);
      break;
  }
}
