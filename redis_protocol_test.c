#include <errno.h>
#include <event2/buffer.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "debug.h"

#include "redis_protocol.h"

#define test_assert(cond) { \
  if (!(cond)) { \
    num_failures++; \
    printf("failed (%s:%d): %s\n", __FILE__, __LINE__, (#cond)); \
  } \
}

#define check_counts(extra_count, extra_refs) { \
  test_assert(base_num_resources + extra_count == resource_count()); \
  test_assert(base_resource_refs + extra_refs == resource_refcount()); }

#define check_counts_and_size(extra_count, extra_refs) { \
  check_counts(extra_count, extra_refs); \
  test_assert(base_resource_size == resource_size()); }

int main(int argc, char* argv[]) {

  printf("redis_protocol tests\n");
  int num_failures = 0;

  int64_t base_num_resources = resource_count();
  int64_t base_resource_refs = resource_refcount();
  int64_t base_resource_size = resource_size();

  {
    printf("-- create & delete a command\n");
    struct redis_command* cmd = redis_command_create(NULL, 10);
    check_counts(1, 0); // the args aren't resources
    test_assert(cmd->external_arg_data == 0);
    test_assert(cmd->num_args == 10);
    resource_delete(cmd, 1);
    check_counts_and_size(0, 0);
  }

  {
    printf("-- parse a command & serialize it again\n");

    const char* command_string = "*7\r\n$4\r\nMSET\r\n$1\r\nx\r\n$1\r\n1\r\n$1\r\ny\r\n$1\r\n2\r\n$1\r\nz\r\n$3\r\nlol\r\n";

    struct evbuffer* buf = evbuffer_new();
    evbuffer_add(buf, command_string, strlen(command_string));
    struct redis_command_parser* parser = redis_command_parser_create(NULL);
    struct redis_command* cmd = redis_command_parser_continue(parser, parser, buf);
    evbuffer_free(buf);

    // check that the args were parsed properly
    test_assert(cmd->num_args == 7);
    test_assert(cmd->args[0].size == 4 && !memcmp(cmd->args[0].data, "MSET", 4));
    test_assert(cmd->args[1].size == 1 && !memcmp(cmd->args[1].data, "x", 1));
    test_assert(cmd->args[2].size == 1 && !memcmp(cmd->args[2].data, "1", 1));
    test_assert(cmd->args[3].size == 1 && !memcmp(cmd->args[3].data, "y", 1));
    test_assert(cmd->args[4].size == 1 && !memcmp(cmd->args[4].data, "2", 1));
    test_assert(cmd->args[5].size == 1 && !memcmp(cmd->args[5].data, "z", 1));
    test_assert(cmd->args[6].size == 3 && !memcmp(cmd->args[6].data, "lol", 3));

    // check that the serialization matches the original command
    buf = evbuffer_new();
    redis_write_command(buf, cmd);
    struct evbuffer_ptr pos = evbuffer_search(buf, command_string, strlen(command_string), NULL);
    test_assert(pos.pos == 0);

    resource_delete(parser, 1);
    check_counts_and_size(0, 0);
  }

  {
    printf("-- parse a response & serialize it again\n");

    const char* resp_string = "*6\r\n+omg\r\n-bbq\r\n:284713592\r\n$-1\r\n*-1\r\n*1\r\n$20\r\nTo be or not to be, \r\n";

    struct evbuffer* buf = evbuffer_new();
    evbuffer_add(buf, resp_string, strlen(resp_string));
    struct redis_response_parser* parser = redis_response_parser_create(NULL);
    struct redis_response* r = redis_response_parser_continue(parser, parser, buf);
    evbuffer_free(buf);

    test_assert(r->response_type == RESPONSE_MULTI);
    test_assert(r->multi_value.num_fields == 6);

    test_assert(r->multi_value.fields[0]->response_type == RESPONSE_STATUS);
    test_assert(!strcmp(r->multi_value.fields[0]->status_str, "omg"));

    test_assert(r->multi_value.fields[1]->response_type == RESPONSE_ERROR);
    test_assert(!strcmp(r->multi_value.fields[1]->status_str, "bbq"));

    test_assert(r->multi_value.fields[2]->response_type == RESPONSE_INTEGER);
    test_assert(r->multi_value.fields[2]->int_value == 284713592);

    test_assert(r->multi_value.fields[3]->response_type == RESPONSE_DATA);
    test_assert(r->multi_value.fields[3]->data_value.size == -1);

    test_assert(r->multi_value.fields[4]->response_type == RESPONSE_MULTI);
    test_assert(r->multi_value.fields[4]->multi_value.num_fields == -1);

    test_assert(r->multi_value.fields[5]->response_type == RESPONSE_MULTI);
    test_assert(r->multi_value.fields[5]->multi_value.num_fields == 1);

    test_assert(r->multi_value.fields[5]->multi_value.fields[0]->response_type == RESPONSE_DATA);
    test_assert(r->multi_value.fields[5]->multi_value.fields[0]->data_value.size == 20);
    test_assert(!memcmp(r->multi_value.fields[5]->multi_value.fields[0]->data_value.data, "To be or not to be, ", 20));

    buf = evbuffer_new();
    redis_write_response(buf, r);
    struct evbuffer_ptr pos = evbuffer_search(buf, resp_string, strlen(resp_string), NULL);
    test_assert(pos.pos == 0);

    resource_delete(parser, 1);
    check_counts_and_size(0, 0);
  }

  if (num_failures)
    printf("%d failures during test run\n", num_failures);
  else
    printf("all tests passed\n");

  return 0;
}
