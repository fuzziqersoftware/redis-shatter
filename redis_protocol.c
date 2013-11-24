#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "debug.h"

#include "redis_socket.h"
#include "redis_protocol.h"



// command functions

redis_command* redis_command_create(void* resource_parent, int num_args) {
  redis_command* cmd = (redis_command*)malloc(sizeof(redis_command) + sizeof(redis_argument) * num_args);
  if (!cmd)
    return NULL;
  cmd->num_args = num_args;
  resource_create(resource_parent, cmd, redis_command_delete);
  return cmd;
}

void redis_command_delete(redis_command* cmd) {
  if (!cmd->external_arg_data) {
    int x;
    for (x = 0; x < cmd->num_args; x++)
      free(cmd->args[x].data);
  }
  free(cmd);
}

void redis_command_print(redis_command* cmd) {
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

redis_response* redis_response_create(void* resource_parent, uint8_t type, int64_t size) {
  redis_response* response;
  switch (type) {
    case RESPONSE_STATUS:
    case RESPONSE_ERROR: {
      response = (redis_response*)malloc(sizeof(redis_response) + size + 1);
      if (!response)
        return NULL;
      response->response_type = type;
      break; }

    case RESPONSE_INTEGER: {
      response = (redis_response*)malloc(sizeof(redis_response));
      if (!response)
        return NULL;
      response->response_type = type;
      break; }

    case RESPONSE_DATA: {
      // size can be negative for a RESPONSE_DATA response, indicating a null value
      response = (redis_response*)malloc(sizeof(redis_response) + (size < 0 ? 0 : size));
      if (!response)
        return NULL;
      response->response_type = type;
      response->data_value.size = size;
      break; }

    case RESPONSE_MULTI: {
      // size can be negative for a RESPONSE_MULTI response, indicating a null value
      response = (redis_response*)malloc(sizeof(redis_response) + ((size < 0 ? 0 : size) * sizeof(redis_command*)));
      if (!response)
        return NULL;
      response->response_type = type;
      response->multi_value.num_fields = size;
      break; }

    default:
      return NULL;
  }

  resource_create(resource_parent, response, redis_response_delete);
  return response;
}

redis_response* redis_response_printf(void* resource_parent, uint8_t type, const char* fmt, ...) {
  const int buffer_size = STATUS_REPLY_BUFFER_LEN;
  redis_response* resp = redis_response_create(resource_parent, type, buffer_size);

  va_list va;
  va_start(va, fmt);
  vsnprintf(resp->status_str, buffer_size, fmt, va);
  va_end(va);

  return resp;
}

void redis_response_delete(redis_response* r) {
  // subfields for RESPONSE_MULTI type should be linked by resource_add_ref,
  // so resource framework should take care of everything
  free(r);
}

void redis_response_print(redis_response* resp) {

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

void redis_receive_fixed_length_line(redis_socket* sock, void* data, int64_t size) {
  if (size > 0)
    redis_socket_read(sock, data, size);

  int16_t rn;
  redis_socket_read(sock, &rn, 2);
  if (rn != 0x0A0D) // little endian
    redis_error(sock, ERROR_INCOMPLETE_LINE, rn);
}

redis_command* redis_receive_command(void* resource_parent, redis_socket* sock) {
  int64_t x, num_args = redis_receive_int_line(sock, '*');
  redis_command* cmd = redis_command_create(resource_parent, num_args);
  if (!cmd)
    redis_error(sock, ERROR_OUT_OF_MEMORY, 0);
  for (x = 0; x < num_args; x++) {
    cmd->args[x].size = redis_receive_int_line(sock, '$');
    cmd->args[x].data = malloc(cmd->args[x].size);
    if (!cmd->args[x].data)
      redis_error(sock, ERROR_OUT_OF_MEMORY, cmd->args[x].size);
    redis_receive_fixed_length_line(sock, cmd->args[x].data, cmd->args[x].size);
  }

  return cmd;
}

redis_response* redis_receive_response(void* resource_parent, redis_socket* sock) {
  uint8_t response_type;
  int64_t size;
  redis_response* response = NULL;

  redis_socket_read(sock, &response_type, 1);
  switch (response_type) {

    case RESPONSE_STATUS:
    case RESPONSE_ERROR: {
      response = redis_response_create(resource_parent, response_type, STATUS_REPLY_BUFFER_LEN);
      redis_socket_read_line(sock, response->status_str, STATUS_REPLY_BUFFER_LEN);
      break; }

    case RESPONSE_INTEGER: {
      response = redis_response_create(resource_parent, response_type, 0);
      response->int_value = redis_receive_int_line(sock, 0);
      break; }

    case RESPONSE_DATA: {
      size = redis_receive_int_line(sock, 0);
      response = redis_response_create(resource_parent, response_type, size);
      if (response->data_value.size >= 0)
        redis_receive_fixed_length_line(sock, response->data_value.data, response->data_value.size);
      break; }

    case RESPONSE_MULTI: {
      size = redis_receive_int_line(sock, 0);
      response = redis_response_create(resource_parent, response_type, size);
      int64_t x;
      for (x = 0; x < response->multi_value.num_fields; x++)
        response->multi_value.fields[x] = redis_receive_response(response, sock);
      break; }

    default:
      redis_error(sock, ERROR_PROTOCOL, response_type);
  }

  return response;
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

void redis_send_response(redis_socket* sock, redis_response* resp) {

  int64_t x;
  char size_buffer[24];

  switch (resp->response_type) {

    case RESPONSE_STATUS:
    case RESPONSE_ERROR:
      redis_send_string_response(sock, resp->status_str, resp->response_type);
      break;

    case RESPONSE_INTEGER:
      sprintf(size_buffer, "%lld", resp->int_value);
      redis_send_string_response(sock, size_buffer, resp->response_type);
      break;

    case RESPONSE_DATA:
      sprintf(size_buffer, "%lld", resp->data_value.size);
      redis_send_string_response(sock, size_buffer, resp->response_type);
      if (resp->data_value.size >= 0) {
        redis_socket_write(sock, resp->data_value.data, resp->data_value.size);
        redis_socket_write(sock, "\r\n", 2);
      }
      break;

    case RESPONSE_MULTI:
      sprintf(size_buffer, "%lld", resp->multi_value.num_fields);
      redis_send_string_response(sock, size_buffer, resp->response_type);
      if (resp->multi_value.num_fields >= 0)
        for (x = 0; x < resp->multi_value.num_fields; x++)
          redis_send_response(sock, resp->multi_value.fields[x]);
      break;
  }
}

// TODO void redis_forward_reply(redis_socket* sock, redis_socket* dest_socket);

void redis_send_string_response(redis_socket* sock, const char* string, char sentinel) {
  redis_socket_write(sock, &sentinel, 1);
  redis_socket_write(sock, string, strlen(string));
  redis_socket_write(sock, "\r\n", 2);
}

void redis_send_int_response(redis_socket* sock, int64_t value) {
  char int_buffer[24];
  redis_socket_write(sock, int_buffer, sprintf(int_buffer, ":%lld\r\n", value));
}
