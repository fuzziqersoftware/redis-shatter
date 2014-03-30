#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "debug.h"
#include "protocol.h"


#define STATUS_REPLY_BUFFER_LEN  512



////////////////////////////////////////////////////////////////////////////////
// command/response manipulation

static void command_delete(struct command* cmd) {
  if (!cmd->external_arg_data) {
    int x;
    for (x = 0; x < cmd->num_args; x++)
      free(cmd->args[x].data);
  }
  free(cmd);
}

struct command* command_create(void* resource_parent, int num_args) {
  struct command* cmd = (struct command*)resource_calloc(
      resource_parent, sizeof(struct command) +
        sizeof(struct argument) * num_args, command_delete);
  if (!cmd)
    return NULL;
  resource_annotate(cmd, "command[%d]", num_args);
  cmd->num_args = num_args;
  return cmd;
}

void command_print(const struct command* cmd, int indent) {

  if (indent < 0)
    indent = -indent;
  else
    print_indent(indent);

  if (!cmd) {
    printf("command@NULL");
    return;
  }

  int x, y;
  printf("command@%p[args=[", cmd);
  for (x = 0; x < cmd->num_args; x++) {
    printf("\n");
    print_indent(indent + 1);
    printf("argument[size=%x, annotation=%x, data=%p, data_preview=", cmd->args[x].size, cmd->args[x].annotation, cmd->args[x].data);
    for (y = 0; y < cmd->args[x].size; y++) {
      int ch = *((char*)cmd->args[x].data + y);
      if (ch < 0x20 || ch > 0x7F)
        printf("\\x%02X", ch);
      else
        printf("%c", ch);
    }
    printf("],");
  }
  printf("]]");
}

struct response* response_create(void* resource_parent, uint8_t type, int64_t size) {
  struct response* response;
  switch (type) {
    case RESPONSE_STATUS:
    case RESPONSE_ERROR: {
      response = (struct response*)resource_malloc(resource_parent,
          sizeof(struct response) + size + 1, free);
      if (!response)
        return NULL;
      response->response_type = type;
      break; }

    case RESPONSE_INTEGER: {
      response = (struct response*)resource_malloc(resource_parent,
          sizeof(struct response), free);
      if (!response)
        return NULL;
      response->response_type = type;
      break; }

    case RESPONSE_DATA: {
      // size can be negative for a RESPONSE_DATA response, indicating a null value
      response = (struct response*)resource_malloc(resource_parent,
          sizeof(struct response) + (size < 0 ? 0 : size), free);
      if (!response)
        return NULL;
      response->response_type = type;
      response->data_value.size = size;
      break; }

    case RESPONSE_MULTI: {
      // size can be negative for a RESPONSE_MULTI response, indicating a null value
      response = (struct response*)resource_calloc(resource_parent,
          sizeof(struct response) +
            ((size < 0 ? 0 : size) * sizeof(struct command*)), free);
      if (!response)
        return NULL;
      response->response_type = type;
      response->multi_value.num_fields = size;
      break; }

    default:
      return NULL;
  }

  resource_annotate(response, "response[%d, %016llX]", type, size);
  return response;
}

struct response* response_printf(void* resource_parent, uint8_t type, const char* fmt, ...) {
  const int buffer_size = STATUS_REPLY_BUFFER_LEN;
  struct response* resp = response_create(resource_parent, type, buffer_size);

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

int responses_equal(const struct response* a, const struct response* b) {
  if (!a && !b)
    return 1;
  if (!a || !b)
    return 0;
  if (a->response_type != b->response_type)
    return 0;

  switch (a->response_type) {
    case RESPONSE_STATUS:
    case RESPONSE_ERROR:
      return !strcmp(a->status_str, b->status_str);

    case RESPONSE_INTEGER:
      return (a->int_value == b->int_value);

    case RESPONSE_DATA:
      return (a->data_value.size == b->data_value.size) &&
          !memcmp(a->data_value.data, b->data_value.data, a->data_value.size);

    case RESPONSE_MULTI: {}
      if (a->multi_value.num_fields != b->multi_value.num_fields)
        return 0;

      int x;
      for (x = 0; x < a->multi_value.num_fields; x++)
        if (!responses_equal(a->multi_value.fields[x], b->multi_value.fields[x]))
          return 0;
      return 1;

    default:
      return 0;
  }
}

void response_print(const struct response* resp, int indent) {

  if (indent < 0)
    indent = -indent;
  else
    print_indent(indent);

  if (!resp) {
    printf("response@NULL");
    return;
  }

  int x;
  switch (resp->response_type) {
    case RESPONSE_STATUS:
      printf("response@%p[type=STATUS, status_str=%s]", resp, resp->status_str);
      break;

    case RESPONSE_ERROR:
      printf("response@%p[type=ERROR, status_str=%s]", resp, resp->status_str);
      break;

    case RESPONSE_INTEGER:
      printf("response@%p[type=INTEGER, int_value=%lld]", resp, resp->int_value);
      break;

    case RESPONSE_DATA:
      if (resp->data_value.size < 0)
        printf("response@%p[type=DATA, null]\n", resp);
      else {
        printf("response@%p[type=DATA, size=%lld, data_preview=", resp, resp->data_value.size);
        for (x = 0; x < resp->data_value.size; x++) {
          int ch = *((char*)resp->data_value.data + x);
          if (ch < 0x20 || ch > 0x7F)
            printf("\\x%02X", ch);
          else
            printf("%c", ch);
        }
        printf("]");
      }
      break;

    case RESPONSE_MULTI:
      if (resp->data_value.size < 0)
        printf("response@%p[type=MULTI, null]", resp);
      else {
        printf("response@%p[type=MULTI, num_fields=%lld, fields=[\n", resp, resp->multi_value.num_fields);
        for (x = 0; x < resp->multi_value.num_fields; x++) {
          response_print(resp->multi_value.fields[x], indent + 1);
          printf(",\n");
        }
        print_indent(indent);
        printf("]");
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

struct command_parser* command_parser_create(
    void* resource_parent) {

  struct command_parser* st = (struct command_parser*)
      resource_calloc(resource_parent, sizeof(struct command_parser),
      free);
  if (!st)
    return NULL;
  resource_annotate(st, "command_parser[]");
  return st;
}

struct command* command_parser_continue(void* resource_parent,
    struct command_parser* st, struct evbuffer* buf) {

  char *input_line;
  size_t len;
  int can_continue = 1;
  struct command* cmd_to_return = NULL;
  while (!st->error && can_continue && !cmd_to_return) {

    switch (st->state) {

      case CMDSTATE_INITIAL: // expect "*NUM_ARGS\r\n", or inline command
        input_line = evbuffer_readln(buf, &len, EVBUFFER_EOL_CRLF);
        if (!input_line) {
          can_continue = 0;
          break; // complete line not yet available
        }

        if (input_line[0] != '*') {
          // this is an inline command. first find out how many args there are
          // we could use strtok/strsep here but they're destructive
          int x, num_args = 1;
          for (x = 0; input_line[x];) {
            for (; input_line[x] && (input_line[x] != ' '); x++);
            if (input_line[x])
              num_args++;
            for (; input_line[x] && (input_line[x] == ' '); x++);
          }

          // now parse the command
          st->arg_in_progress = 0;
          st->command_in_progress = command_create(resource_parent, num_args);
          int arg_start_offset = 0;
          for (x = 0; input_line[x];) {
            for (; input_line[x] && (input_line[x] != ' '); x++);

            struct argument* arg = &st->command_in_progress->args[st->arg_in_progress];
            arg->size = x - arg_start_offset;
            arg->data = malloc(arg->size);
            memcpy(arg->data, &input_line[arg_start_offset], arg->size);
            st->arg_in_progress++;

            for (; input_line[x] && (input_line[x] == ' '); x++);
            arg_start_offset = x;
          }

          cmd_to_return = st->command_in_progress;

        } else {
          int num_command_args = atoi(&input_line[1]);
          if (num_command_args > 0) {
            st->command_in_progress = command_create(st, num_command_args);
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
          struct argument* arg = &st->command_in_progress->args[st->arg_in_progress];
          arg->size = atoi(&input_line[1]);
          arg->data = malloc(arg->size);
          if (!arg->data)
            st->error = ERROR_MEMORY;
          st->arg_in_progress_read_bytes = 0;
          st->state = CMDSTATE_READ_ARG_DATA;
        }
        break;

      case CMDSTATE_READ_ARG_DATA: { // copy data directly to the forehead
        struct argument* arg = &st->command_in_progress->args[st->arg_in_progress];
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
  return cmd_to_return;
}

void command_parser_print(const struct command_parser* p, int indent) {
  if (indent < 0)
    indent = -indent;
  else
    print_indent(indent);

  if (!p) {
    printf("command_parser@NULL");
    return;
  }

  printf("command_parser@%p[state=%d, error=%d, num_command_args=%d, arg_in_progress=%d, arg_in_progress_read_bytes=%d, command_in_progress=", p, p->state, p->error, p->num_command_args, p->arg_in_progress, p->arg_in_progress_read_bytes);
  command_print(p->command_in_progress, -(indent + 1));
  printf("]");
}



#define RSPSTATE_INITIAL                            0
#define RSPSTATE_MULTI_RECURSIVE                    1
#define RSPSTATE_READ_DATA_RESPONSE                 2
#define RSPSTATE_READ_NEWLINE_AFTER_DATA_RESPONSE   3

struct response_parser* response_parser_create(void* resource_parent) {

  struct response_parser* st = (struct response_parser*)
      resource_calloc(resource_parent, sizeof(struct response_parser),
      free);
  if (!st)
    return NULL;
  resource_annotate(st, "response_parser[]");
  return st;
}

struct response* response_parser_continue(void* resource_parent,
    struct response_parser* st, struct evbuffer* buf) {

  char* input_line;
  size_t len;
  int can_continue = 1;
  struct response* resp_to_return = NULL;
  while (!st->error && can_continue && !resp_to_return) {

    input_line = NULL;
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
            resp_to_return = response_create(resource_parent,
                input_line[0], len);
            strcpy(resp_to_return->status_str, &input_line[1]);
            break; }

          case RESPONSE_INTEGER: {
            resp_to_return = response_create(resource_parent,
                RESPONSE_INTEGER, 0);
            resp_to_return->int_value = strtoll(&input_line[1], NULL, 0);
            break; }

          case RESPONSE_DATA: {
            int64_t size = strtoll(&input_line[1], NULL, 0);
            st->response_in_progress = response_create(resource_parent,
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
            st->response_in_progress = response_create(resource_parent,
                RESPONSE_MULTI, num_fields);
            if (num_fields <= 0)
              resp_to_return = st->response_in_progress;
            else {
              if (st->multi_in_progress)
                resource_delete_ref(st, st->multi_in_progress);
              st->multi_in_progress = response_parser_create(st);
              st->multi_response_current_field = 0;
              st->state = RSPSTATE_MULTI_RECURSIVE;
            }
            break; }

          default:
            st->error = ERROR_UNEXPECTED_INPUT;
        }
        break; // RSPSTATE_INITIAL

      case RSPSTATE_MULTI_RECURSIVE: {
        struct response* field = response_parser_continue(
            st->response_in_progress, st->multi_in_progress, buf);
        if (!field) {
          if (st->multi_in_progress->error)
            st->error = st->multi_in_progress->error;
          can_continue = 0;

        } else {
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
        if (!data_to_read)
          can_continue = 0;

        else {
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
        }
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

int response_parser_continue_forward(struct response_parser* st,
    struct evbuffer* buf, struct evbuffer* output_buffer) {

  // output_buffer can be NULL if the client has already disconnected. in this
  // case, we just don't write to the output buffer (discard the response).

  char* input_line;
  size_t len;
  int can_continue = 1;
  int complete_response_read = 0;
  while (!st->error && can_continue && !complete_response_read) {

    input_line = NULL;
    switch (st->state) {

      case RSPSTATE_INITIAL:
        input_line = evbuffer_readln(buf, &len, EVBUFFER_EOL_CRLF);
        if (!input_line) {
          can_continue = 0;
          break; // complete line not yet available
        }

        // forward the line to the client immediately
        if (output_buffer)
          evbuffer_add_printf(output_buffer, "%s\r\n", input_line);

        switch (input_line[0]) {
          case RESPONSE_STATUS:
          case RESPONSE_ERROR:
          case RESPONSE_INTEGER:
            complete_response_read = 1;
            break;

          case RESPONSE_DATA:
            // we add 2 here for the trailing \r\n
            st->forward_items_remaining = strtoll(&input_line[1], NULL, 0) + 2;
            if (st->forward_items_remaining < 2)
              complete_response_read = 1;
            else
              st->state = RSPSTATE_READ_DATA_RESPONSE;
            break;

          case RESPONSE_MULTI:
            st->forward_items_remaining = strtoll(&input_line[1], NULL, 0);
            if (st->forward_items_remaining <= 0)
              complete_response_read = 1;
            else {
              if (st->multi_in_progress)
                resource_delete_ref(st, st->multi_in_progress);
              st->multi_in_progress = response_parser_create(st);
              st->state = RSPSTATE_MULTI_RECURSIVE;
            }
            break;

          default:
            st->error = ERROR_UNEXPECTED_INPUT;
        }
        break; // RSPSTATE_INITIAL

      case RSPSTATE_MULTI_RECURSIVE: {
        int full_response = response_parser_continue_forward(
            st->multi_in_progress, buf, output_buffer);
        if (!full_response) {
          if (st->multi_in_progress->error)
            st->error = st->multi_in_progress->error;
          can_continue = 0;

        } else {
          st->forward_items_remaining--;
          if (st->forward_items_remaining == 0) {
            st->state = RSPSTATE_INITIAL;
            complete_response_read = 1;
          }
        }
        break; }

      case RSPSTATE_READ_DATA_RESPONSE: {
        size_t data_to_read = evbuffer_get_length(buf);
        if (!data_to_read)
          can_continue = 0;

        else {
          if (data_to_read > st->forward_items_remaining)
            data_to_read = st->forward_items_remaining;

          // TODO: can we use evbuffer_add_buffer_reference here?
          int res = data_to_read;
          if (output_buffer)
            res = evbuffer_remove_buffer(buf, output_buffer, data_to_read);
          else
            evbuffer_drain(buf, data_to_read);

          if (res == -1)
            st->error = ERROR_BUFFER_COPY;
          else
            st->forward_items_remaining -= res;

          if (st->forward_items_remaining == 0) {
            st->state = RSPSTATE_INITIAL;
            complete_response_read = 1;
          }
        }
        break; }

      default:
        st->error = ERROR_UNKNOWN_STATE;
    }

    if (input_line)
      free(input_line);
    input_line = NULL;
  }
  return complete_response_read;
}

void response_parser_print(const struct response_parser* p, int indent) {
  if (indent < 0)
    indent = -indent;
  else
    print_indent(indent);

  if (!p) {
    printf("response_parser@NULL");
    return;
  }

  printf("response_parser@%p[state=%d, error=%d, data_response_bytes_read=%d, response_in_progress=", p, p->state, p->error, p->data_response_bytes_read);
  response_print(p->response_in_progress, -(indent + 1));
  printf(", multi_response_current_field=%d, multi_in_progress=", p->multi_response_current_field);
  response_parser_print(p->multi_in_progress, -(indent + 1));
  printf("]");
}


////////////////////////////////////////////////////////////////////////////////
// command/response output

void command_write(struct evbuffer* buf, const struct command* cmd) {
  if (!buf)
    return;

#ifdef DEBUG_COMMAND_IO
  printf("OUTPUT COMMAND TO BACKEND:");
  command_print(cmd, 0);
  printf("\n");
#endif

  evbuffer_add_printf(buf, "*%d\r\n", cmd->num_args);

  int x;
  for (x = 0; x < cmd->num_args; x++) {
    evbuffer_add_printf(buf, "$%d\r\n", cmd->args[x].size);
    evbuffer_add(buf, cmd->args[x].data, cmd->args[x].size);
    evbuffer_add(buf, "\r\n", 2);
  }
}

#ifdef DEBUG_COMMAND_IO
static int _recursion_in_write_response = 0;
#endif

void response_write_string(struct evbuffer* buf, const char* string,
    char sentinel) {
  if (!buf)
    return;

#ifdef DEBUG_COMMAND_IO
  if (_recursion_in_write_response == 0)
    printf("OUTPUT RESPONSE TO CLIENT: STRING[%c%s]\n", sentinel, string);
#endif

  evbuffer_add_printf(buf, "%c%s\r\n", sentinel, string);
}

void response_write_int(struct evbuffer* buf, int64_t value,
    char sentinel) {
  if (!buf)
    return;

#ifdef DEBUG_COMMAND_IO
  if (_recursion_in_write_response == 0)
    printf("OUTPUT RESPONSE TO CLIENT: INTEGER[%c%lld]\n", sentinel, value);
#endif

  evbuffer_add_printf(buf, "%c%lld\r\n", sentinel, value);
}

void response_write(struct evbuffer* buf, const struct response* resp) {

  if (!buf)
    return;

#ifdef DEBUG_COMMAND_IO
  if (_recursion_in_write_response == 0) {
    printf("OUTPUT RESPONSE TO CLIENT:");
    response_print(resp, 0);
    printf("\n");
  }
  _recursion_in_write_response++;
#endif

  int64_t x;
  switch (resp->response_type) {

    case RESPONSE_STATUS:
    case RESPONSE_ERROR:
      response_write_string(buf, resp->status_str, resp->response_type);
      break;

    case RESPONSE_INTEGER:
      response_write_int(buf, resp->int_value, ':');
      break;

    case RESPONSE_DATA:
      response_write_int(buf, resp->data_value.size, resp->response_type);
      if (resp->data_value.size >= 0) {
        evbuffer_add(buf, resp->data_value.data, resp->data_value.size);
        evbuffer_add(buf, "\r\n", 2);
      }
      break;

    case RESPONSE_MULTI:
      response_write_int(buf, resp->multi_value.num_fields,
          resp->response_type);
      if (resp->multi_value.num_fields >= 0)
        for (x = 0; x < resp->multi_value.num_fields; x++)
          response_write(buf, resp->multi_value.fields[x]);
      break;
  }
#ifdef DEBUG_COMMAND_IO
  _recursion_in_write_response--;
#endif
}
