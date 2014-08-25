#include <errno.h>
#include <event2/buffer.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "debug.h"

#include "network.h"
#include "protocol.h"

int num_failures;

#define test_assert(cond) { \
  if (!(cond)) { \
    num_failures++; \
    printf("failed (%s:%d): %s\n", __FILE__, __LINE__, (#cond)); \
  } \
}

#define test_assert_message(cond, msg) { \
  if (!(cond)) { \
    num_failures++; \
    printf("failed (%s:%d): %s (%s)\n", __FILE__, __LINE__, (#cond), (msg)); \
  } \
}

struct response* parse_response(void* resource_parent, const char* contents) {
  struct evbuffer* buffer = evbuffer_new();
  evbuffer_add_printf(buffer, "%s", contents);
  struct response_parser* parser = response_parser_create(resource_parent);
  struct response* resp = response_parser_continue(resource_parent, parser, buffer);
  evbuffer_free(buffer);
  resource_delete_ref(resource_parent, parser);
  return resp;
}

struct response* test_expect_response(const char* host, int port,
    const char* expected_response, ...) {

  struct resource* local = resource_create_sentinel(NULL);

  va_list va;
  va_start(va, expected_response);
  int count;
  for (count = 0; va_arg(va, void*); count++);
  va_end(va);

  struct command* cmd = command_create(local, count);
  if (!cmd) {
    test_assert_message(0, "can\'t create command");
    resource_delete_ref(NULL, local);
    return NULL;
  }
  cmd->external_arg_data = 1;

  va_start(va, expected_response);
  int x;
  for (x = 0; x < count; x++) {
    cmd->args[x].data = va_arg(va, char*);
    cmd->args[x].size = strlen(cmd->args[x].data);
  }
  va_end(va);

  int fd = network_connect(host, port);
  if (fd < 0) {
    resource_delete_ref(NULL, local);
    test_assert_message(0, "can\'t connect to proxy");
    return NULL;
  }

  struct evbuffer* buffer = evbuffer_new();
  command_write(buffer, cmd);
  evbuffer_write(buffer, fd);
  evbuffer_drain(buffer, evbuffer_get_length(buffer));

  evbuffer_read(buffer, fd, 1024); // lol assumptions
  struct response_parser* parser = response_parser_create(local);
  struct response* resp = response_parser_continue(expected_response ? local : NULL, parser, buffer);
  evbuffer_free(buffer);

  close(fd);

  if (expected_response) {
    struct response* expected_resp = parse_response(local, expected_response);
    if (!responses_equal(resp, expected_resp)) {
      test_assert_message(0, "responses don\'t match");
      printf("cmd = ");
      command_print(cmd, 0);
      printf("\nexpected = ");
      response_print(expected_resp, 0);
      printf("\nactual   = ");
      response_print(resp, 0);
      printf("\n");
    }
  }

  resource_delete_ref(NULL, local);

  return expected_response ? NULL : resp;
}

int main(int argc, char* argv[]) {

  printf("functional tests\n");
  printf("we expect redis-shatter to be running with all backends connected\n");
  num_failures = 0;

  struct resource* local_res = resource_create_sentinel(NULL);

  {
    printf("-- unimplemented commands return PROXYERROR\n");

    const char* unimplemented_commands[] = {
      "AUTH", "BLPOP", "BRPOP", "BRPOPLPUSH", "DISCARD", "EXEC", "MONITOR",
      "MOVE", "MSETNX", "MULTI", "PSUBSCRIBE", "PUBSUB", "PUBLISH",
      "PUNSUBSCRIBE", "SELECT", "SHUTDOWN", "SLAVEOF", "SUBSCRIBE", "SYNC",
      "UNSUBSCRIBE", "UNWATCH", "WATCH", NULL};

    int x;
    for (x = 0; unimplemented_commands[x]; x++) {
      test_expect_response("localhost", 6379, "-PROXYERROR command not supported\r\n", unimplemented_commands[x], NULL);
    }
  }

  {
    printf("-- PING\n");
    test_expect_response("localhost", 6379, "+PONG\r\n", "PING", NULL);
  }

  {
    printf("-- ECHO\n");
    test_expect_response("localhost", 6379, "$3\r\nLOL\r\n", "ECHO", "LOL", NULL);
  }

  {
    printf("-- FLUSHALL, DBSIZE\n");
    test_expect_response("localhost", 6379, "+OK\r\n", "FLUSHALL", NULL);
    test_expect_response("localhost", 6379, ":0\r\n", "DBSIZE", NULL);
  }

  {
    printf("-- GET, SET, GETSET, MGET, MSET, DEL\n");
    test_expect_response("localhost", 6379, "$-1\r\n", "GET", "x", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "SET", "x", "23", NULL);
    test_expect_response("localhost", 6379, "$2\r\n23\r\n", "GET", "x", NULL);
    test_expect_response("localhost", 6379, "$2\r\n23\r\n", "GETSET", "x", "45", NULL);
    test_expect_response("localhost", 6379, "$2\r\n45\r\n", "GET", "x", NULL);
    test_expect_response("localhost", 6379, "*3\r\n$2\r\n45\r\n$-1\r\n$-1\r\n", "MGET", "x", "y", "z", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "MSET", "x", "1", "y", "2", "z", "3", NULL);
    test_expect_response("localhost", 6379, "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n", "MGET", "x", "y", "z", NULL);
    test_expect_response("localhost", 6379, ":2\r\n", "DEL", "x", "y", "w", NULL);
  }

  {
    printf("-- proxy commands: FORWARD, BACKENDS, BACKENDNUM\n");
    test_expect_response("localhost", 6379, "+PONG\r\n", "FORWARD", "0", "PING", NULL);

    struct response* resp = test_expect_response("localhost", 6379, NULL, "BACKENDS", NULL);
    test_assert(resp->response_type == RESPONSE_MULTI);
    int num_backends = resp->multi_value.num_fields;
    printf("---- note: there are %d backends\n", num_backends);
    resource_delete_ref(NULL, resp);

    resp = test_expect_response("localhost", 6379, NULL, "BACKENDNUM", "z", NULL);
    test_assert(resp->response_type == RESPONSE_INTEGER);
    int z_backend = resp->int_value;
    printf("---- note: \'z\' goes to backend %d\n", z_backend);
    resource_delete_ref(NULL, resp);

    char backend_num_str[24];
    sprintf(backend_num_str, "%d", z_backend);
    test_expect_response("localhost", 6379, "$1\r\n3\r\n", "GET", "z", NULL);
    test_expect_response("localhost", 6379, "$1\r\n3\r\n", "FORWARD", backend_num_str, "GET", "z", NULL);
  }

  {
    printf("-- FLUSHDB, DBSIZE\n");
    test_expect_response("localhost", 6379, "+OK\r\n", "FLUSHDB", NULL);
    test_expect_response("localhost", 6379, ":0\r\n", "DBSIZE", NULL);
    test_expect_response("localhost", 6379, "*3\r\n$-1\r\n$-1\r\n$-1\r\n", "MGET", "x", "y", "z", NULL);
  }

  {
    printf("-- RENAME\n");
    test_expect_response("localhost", 6379, "+OK\r\n", "SET", "x{abc}", "a", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "SET", "y{abc}", "b", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "SET", "z{abd}", "b", NULL);

    // make sure the keys are on the same backend
    struct response* backend_x_resp = test_expect_response("localhost", 6379, NULL, "BACKENDNUM", "x{abc}", NULL);
    struct response* backend_y_resp = test_expect_response("localhost", 6379, NULL, "BACKENDNUM", "y{abc}", NULL);
    struct response* backend_z_resp = test_expect_response("localhost", 6379, NULL, "BACKENDNUM", "z{abd}", NULL);
    test_assert(responses_equal(backend_x_resp, backend_y_resp));
    test_assert(!responses_equal(backend_x_resp, backend_z_resp));

    test_expect_response("localhost", 6379, "-PROXYERROR keys are on different backends\r\n", "RENAME", "x{abc}", "x{abd}", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "RENAME", "x{abc}", "y{abc}", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "RENAME", "y{abc}", "zxcvbnm{abc}", NULL);

    test_expect_response("localhost", 6379, "-PROXYERROR keys are on different backends\r\n", "RENAME", "z{abd}", "z{abc}", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "RENAME", "z{abd}", "y{abd}", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "RENAME", "y{abd}", "zxcvbnm{abd}", NULL);
  }

  // untested commands:
  // -------------------------------------------------
  // APPEND           -- same handler as GET
  // BGREWRITEAOF     -- same handler as FLUSHALL
  // BGSAVE           -- same handler as FLUSHALL
  // BITCOUNT         -- same handler as GET
  // BITOP            -- same handler as RENAME
  // BITPOS           -- same handler as GET
  // CLIENT           -- redis_command_CLIENT
  // CONFIG           -- redis_command_all_collect_responses
  // DEBUG            -- redis_command_DEBUG
  // DECR             -- same handler as GET
  // DECRBY           -- same handler as GET
  // DUMP             -- same handler as GET
  // EVAL             -- redis_command_EVAL
  // EVALSHA          -- redis_command_EVAL
  // EXISTS           -- same handler as GET
  // EXPIRE           -- same handler as GET
  // EXPIREAT         -- same handler as GET
  // GETBIT           -- same handler as GET
  // GETRANGE         -- same handler as GET
  // HDEL             -- same handler as GET
  // HEXISTS          -- same handler as GET
  // HGET             -- same handler as GET
  // HGETALL          -- same handler as GET
  // HINCRBY          -- same handler as GET
  // HINCRBYFLOAT     -- same handler as GET
  // HKEYS            -- same handler as GET
  // HLEN             -- same handler as GET
  // HMGET            -- same handler as GET
  // HMSET            -- same handler as GET
  // HSCAN            -- same handler as GET
  // HSET             -- same handler as GET
  // HSETNX           -- same handler as GET
  // HVALS            -- same handler as GET
  // INCR             -- same handler as GET
  // INCRBY           -- same handler as GET
  // INCRBYFLOAT      -- same handler as GET
  // INFO             -- redis_command_INFO
  // KEYS             -- redis_command_KEYS
  // LASTSAVE         -- redis_command_all_collect_responses
  // LINDEX           -- same handler as GET
  // LINSERT          -- same handler as GET
  // LLEN             -- same handler as GET
  // LPOP             -- same handler as GET
  // LPUSH            -- same handler as GET
  // LPUSHX           -- same handler as GET
  // LRANGE           -- same handler as GET
  // LREM             -- same handler as GET
  // LSET             -- same handler as GET
  // LTRIM            -- same handler as GET
  // MIGRATE          -- redis_command_MIGRATE
  // OBJECT           -- redis_command_OBJECT
  // PERSIST          -- same handler as GET
  // PEXPIRE          -- same handler as GET
  // PEXPIREAT        -- same handler as GET
  // PFADD            -- same handler as GET
  // PFCOUNT          -- same handler as RENAME
  // PFMERGE          -- same handler as RENAME
  // PSETEX           -- same handler as GET
  // PTTL             -- same handler as GET
  // QUIT             -- redis_command_QUIT
  // RANDOMKEY        -- redis_command_RANDOMKEY
  // RENAMENX         -- same handler as RENAME
  // RESTORE          -- same handler as GET
  // ROLE             -- redis_command_ROLE
  // RPOP             -- same handler as GET
  // RPOPLPUSH        -- same handler as RENAME
  // RPUSH            -- same handler as GET
  // RPUSHX           -- same handler as GET
  // SADD             -- same handler as GET
  // SAVE             -- same handler as FLUSHALL
  // SCAN             -- redis_command_SCAN
  // SCARD            -- same handler as GET
  // SCRIPT           -- redis_command_SCRIPT
  // SDIFF            -- same handler as RENAME
  // SDIFFSTORE       -- same handler as RENAME
  // SETBIT           -- same handler as GET
  // SETEX            -- same handler as GET
  // SETNX            -- same handler as GET
  // SETRANGE         -- same handler as GET
  // SINTER           -- same handler as RENAME
  // SINTERSTORE      -- same handler as RENAME
  // SISMEMBER        -- same handler as GET
  // SLOWLOG          -- redis_command_all_collect_responses
  // SMEMBERS         -- same handler as GET
  // SMOVE            -- same handler as RENAME
  // SORT             -- same handler as GET
  // SPOP             -- same handler as GET
  // SRANDMEMBER      -- same handler as GET
  // SREM             -- same handler as GET
  // SSCAN            -- same handler as GET
  // STRLEN           -- same handler as GET
  // SUNION           -- same handler as RENAME
  // SUNIONSTORE      -- same handler as RENAME
  // TIME             -- redis_command_all_collect_responses
  // TTL              -- same handler as GET
  // TYPE             -- same handler as GET
  // ZADD             -- same handler as GET
  // ZCARD            -- same handler as GET
  // ZCOUNT           -- same handler as GET
  // ZINCRBY          -- same handler as GET
  // ZINTERSTORE      -- redis_command_ZACTIONSTORE
  // ZLEXCOUNT        -- same handler as GET
  // ZRANGE           -- same handler as GET
  // ZRANGEBYLEX      -- same handler as GET
  // ZRANGEBYSCORE    -- same handler as GET
  // ZRANK            -- same handler as GET
  // ZREM             -- same handler as GET
  // ZREMRANGEBYLEX   -- same handler as GET
  // ZREMRANGEBYRANK  -- same handler as GET
  // ZREMRANGEBYSCORE -- same handler as GET
  // ZREVRANGE        -- same handler as GET
  // ZREVRANGEBYSCORE -- same handler as GET
  // ZREVRANK         -- same handler as GET
  // ZSCAN            -- same handler as GET
  // ZSCORE           -- same handler as GET
  // ZUNIONSTORE      -- redis_command_ZACTIONSTORE

  resource_delete_ref(NULL, local_res);

  if (num_failures)
    printf("%d failures during test run\n", num_failures);
  else
    printf("all tests passed\n");

  return num_failures;
}
