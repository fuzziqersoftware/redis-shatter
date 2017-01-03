#define _STDC_FORMAT_MACROS

#include <errno.h>
#include <event2/buffer.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <phosg/Filesystem.hh>
#include <phosg/Network.hh>
#include <phosg/Strings.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "Protocol.hh"

using namespace std;


shared_ptr<Response> parse_response(const char* contents) {
  unique_ptr<struct evbuffer, void(*)(struct evbuffer*)> buf(evbuffer_new(),
      evbuffer_free);
  evbuffer_add(buf.get(), contents, strlen(contents));
  return ResponseParser().resume(buf.get());
}

shared_ptr<Response> test_expect_response(const char* host, int port,
    const char* expected_response, ...) {

  DataCommand cmd;

  va_list va;
  va_start(va, expected_response);
  const char* arg;
  while ((arg = va_arg(va, const char*))) {
    cmd.args.emplace_back(arg);
  }
  va_end(va);

  shared_ptr<Response> r;
  {
    scoped_fd fd = connect(host, port, false); // not nonblocking
    expect_ge(fd, 0);

    unique_ptr<struct evbuffer, void(*)(struct evbuffer*)> buf(evbuffer_new(),
        evbuffer_free);
    cmd.write(buf.get());
    evbuffer_write(buf.get(), fd);
    evbuffer_drain(buf.get(), evbuffer_get_length(buf.get()));

    evbuffer_read(buf.get(), fd, 1024 * 128);
    r = ResponseParser().resume(buf.get());
  }

  if (expected_response) {
    shared_ptr<Response> expected_r = parse_response(expected_response);
    expect(expected_r.get()); // if this fails, the test itself is broken

    if (!r.get()) {
      fprintf(stderr, "cmd = ");
      cmd.print(stderr);
      fprintf(stderr, "\nexpected = ");
      expected_r->print(stderr);
      fprintf(stderr, "\nactual   = (not present)\n");
      expect(false);
    }
    if (*r != *expected_r) {
      fprintf(stderr, "cmd = ");
      cmd.print(stderr);
      fprintf(stderr, "\nexpected = ");
      expected_r->print(stderr);
      fprintf(stderr, "\nactual   = ");
      r->print(stderr);
      fprintf(stderr, "\n");
      expect(false);
    }
  }

  return expected_response ? NULL : r;
}

int main(int argc, char* argv[]) {

  printf("functional tests\n");
  printf("we expect redis-shatter to be running with all backends connected\n");

  {
    printf("-- unimplemented commands return PROXYERROR\n");

    const vector<string> unimplemented_commands = {
      "AUTH", "BLPOP", "BRPOP", "BRPOPLPUSH", "DISCARD", "EXEC", "MONITOR",
      "MOVE", "MULTI", "PSUBSCRIBE", "PUBSUB", "PUBLISH", "PUNSUBSCRIBE",
      "SELECT", "SLAVEOF", "SUBSCRIBE", "SYNC", "UNSUBSCRIBE", "UNWATCH",
      "WATCH"};

    for (const auto& cmd : unimplemented_commands) {
      test_expect_response("localhost", 6379,
          "-PROXYERROR command not supported\r\n", cmd.c_str(), NULL);
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

    auto r = test_expect_response("localhost", 6379, NULL, "BACKENDS", NULL);
    expect_eq(r->type, Response::Type::Multi);
    size_t num_backends = r->fields.size();
    printf("---- note: there are %zu backends\n", num_backends);

    r = test_expect_response("localhost", 6379, NULL, "BACKENDNUM", "z", NULL);
    expect_eq(r->type, Response::Type::Integer);
    int64_t z_backend = r->int_value;
    printf("---- note: \'z\' goes to backend %" PRId64 "\n", z_backend);

    string z_backend_str = string_printf("%" PRId64, z_backend);
    test_expect_response("localhost", 6379, "$1\r\n3\r\n", "GET", "z", NULL);
    test_expect_response("localhost", 6379, "$1\r\n3\r\n", "FORWARD",
        z_backend_str.c_str(), "GET", "z", NULL);
  }

  {
    printf("-- FLUSHDB, DBSIZE\n");
    test_expect_response("localhost", 6379, "+OK\r\n", "FLUSHDB", NULL);
    test_expect_response("localhost", 6379, ":0\r\n", "DBSIZE", NULL);
    test_expect_response("localhost", 6379, "*3\r\n$-1\r\n$-1\r\n$-1\r\n", "MGET", "x", "y", "z", NULL);
  }

  {
    printf("-- MSETNX, RENAME\n");
    test_expect_response("localhost", 6379, "-PROXYERROR keys are on different backends\r\n", "MSETNX", "x{abc}", "a", "y{abc}", "b", "z{abd}", "b", NULL);
    test_expect_response("localhost", 6379, ":1\r\n", "MSETNX", "x{abc}", "a", "y{abc}", "b", NULL);
    test_expect_response("localhost", 6379, ":0\r\n", "MSETNX", "x{abc}", "a", "y{abc}", "b", "z{abc}", "c", NULL);
    test_expect_response("localhost", 6379, ":1\r\n", "MSETNX", "z{abd}", "b", NULL);

    // make sure the keys are on the same backend
    auto backend_x_resp = test_expect_response("localhost", 6379, NULL, "BACKENDNUM", "x{abc}", NULL);
    auto backend_y_resp = test_expect_response("localhost", 6379, NULL, "BACKENDNUM", "y{abc}", NULL);
    auto backend_z_resp = test_expect_response("localhost", 6379, NULL, "BACKENDNUM", "z{abd}", NULL);
    expect_eq(*backend_x_resp, *backend_y_resp);
    expect_ne(*backend_x_resp, *backend_z_resp);

    test_expect_response("localhost", 6379, "-PROXYERROR keys are on different backends\r\n", "RENAME", "x{abc}", "x{abd}", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "RENAME", "x{abc}", "y{abc}", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "RENAME", "y{abc}", "zxcvbnm{abc}", NULL);

    test_expect_response("localhost", 6379, "-PROXYERROR keys are on different backends\r\n", "RENAME", "z{abd}", "z{abc}", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "RENAME", "z{abd}", "y{abd}", NULL);
    test_expect_response("localhost", 6379, "+OK\r\n", "RENAME", "y{abd}", "zxcvbnm{abd}", NULL);
  }

  printf("all tests passed\n");
  return 0;
}
