#include <errno.h>
#include <event2/buffer.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <phosg/UnitTest.hh>

#include "Protocol.hh"

using namespace std;


template <typename T>
static void check_serialization(const T& obj,
    const char* expected_serialization) {
  unique_ptr<struct evbuffer, void(*)(struct evbuffer*)> out_buf(
      evbuffer_new(), evbuffer_free);
  obj.write(out_buf.get());
  struct evbuffer_ptr pos = evbuffer_search(out_buf.get(),
      expected_serialization, strlen(expected_serialization), NULL);
  expect_eq(pos.pos, 0);
}

template <typename T>
static void check_serialization(shared_ptr<T> obj,
    const char* expected_serialization) {
  unique_ptr<struct evbuffer, void(*)(struct evbuffer*)> out_buf(
      evbuffer_new(), evbuffer_free);
  obj->write(out_buf.get());
  struct evbuffer_ptr pos = evbuffer_search(out_buf.get(),
      expected_serialization, strlen(expected_serialization), NULL);
  expect_eq(pos.pos, 0);
}


int main(int argc, char* argv[]) {

  {
    printf("-- parse a command & serialize it again\n");

    const char* command_string = "*7\r\n$4\r\nMSET\r\n$1\r\nx\r\n$1\r\n1\r\n$1\r\ny\r\n$1\r\n2\r\n$1\r\nz\r\n$3\r\nlol\r\n";

    unique_ptr<struct evbuffer, void(*)(struct evbuffer*)> in_buf(
        evbuffer_new(), evbuffer_free);
    evbuffer_add(in_buf.get(), command_string, strlen(command_string));
    auto cmd = CommandParser().resume(in_buf.get());

    // check that the args were parsed properly
    expect_eq(cmd->args.size(), 7);
    expect_eq(cmd->args[0], "MSET");
    expect_eq(cmd->args[1], "x");
    expect_eq(cmd->args[2], "1");
    expect_eq(cmd->args[3], "y");
    expect_eq(cmd->args[4], "2");
    expect_eq(cmd->args[5], "z");
    expect_eq(cmd->args[6], "lol");

    check_serialization(cmd, command_string);
  }

  {
    printf("-- parse a command (inline) & serialize it again\n");

    const char* command_string = "MSET x 1 y 2 z lol\r\n";
    const char* expected_serialization = "*7\r\n$4\r\nMSET\r\n$1\r\nx\r\n$1\r\n1\r\n$1\r\ny\r\n$1\r\n2\r\n$1\r\nz\r\n$3\r\nlol\r\n";

    unique_ptr<struct evbuffer, void(*)(struct evbuffer*)> in_buf(
        evbuffer_new(), evbuffer_free);
    evbuffer_add(in_buf.get(), command_string, strlen(command_string));
    auto cmd = CommandParser().resume(in_buf.get());

    // check that the args were parsed properly
    expect_eq(cmd->args.size(), 7);
    expect_eq(cmd->args[0], "MSET");
    expect_eq(cmd->args[1], "x");
    expect_eq(cmd->args[2], "1");
    expect_eq(cmd->args[3], "y");
    expect_eq(cmd->args[4], "2");
    expect_eq(cmd->args[5], "z");
    expect_eq(cmd->args[6], "lol");

    check_serialization(cmd, expected_serialization);
  }

  {
    printf("-- parse a response & serialize it again\n");

    const char* resp_string = "*6\r\n+omg\r\n-bbq\r\n:284713592\r\n$-1\r\n*-1\r\n*1\r\n$20\r\nTo be or not to be, \r\n";

    unique_ptr<struct evbuffer, void(*)(struct evbuffer*)> in_buf(
        evbuffer_new(), evbuffer_free);
    evbuffer_add(in_buf.get(), resp_string, strlen(resp_string));
    auto r = ResponseParser().resume(in_buf.get());

    expect_eq(r->type, Response::Type::Multi);
    expect_eq(r->fields.size(), 6);

    expect_eq(r->fields[0]->type, Response::Type::Status);
    expect_eq(r->fields[0]->data, "omg");

    expect_eq(r->fields[1]->type, Response::Type::Error);
    expect_eq(r->fields[1]->data, "bbq");

    expect_eq(r->fields[2]->type, Response::Type::Integer);
    expect_eq(r->fields[2]->int_value, 284713592);

    expect_eq(r->fields[3]->type, Response::Type::Data);
    expect_eq(r->fields[3]->int_value, -1);

    expect_eq(r->fields[4]->type, Response::Type::Multi);
    expect_eq(r->fields[4]->int_value, -1);

    expect_eq(r->fields[5]->type, Response::Type::Multi);
    expect_eq(r->fields[5]->fields.size(), 1);

    expect_eq(r->fields[5]->fields[0]->type, Response::Type::Data);
    expect_eq(r->fields[5]->fields[0]->data, "To be or not to be, ");

    check_serialization(r, resp_string);
  }

  {
    printf("-- check Response printf-like constructor\n");

    {
      Response r(Response::Type::Status,
          "This is response %d of %d; here\'s a string: %s.", 4, 10, "lol");
      const char* expected = "+This is response 4 of 10; here\'s a string: lol.\r\n";
      check_serialization(r, expected);
    }

    {
      Response r(Response::Type::Error,
          "This is response %d of %d; here\'s a string: %s.", 4, 10, "lol");
      const char* expected = "-This is response 4 of 10; here\'s a string: lol.\r\n";
      check_serialization(r, expected);
    }

    {
      Response r(Response::Type::Data,
        "This is response %d of %d; here\'s a string: %s.", 4, 10, "lol");
      const char* expected = "$47\r\nThis is response 4 of 10; here\'s a string: lol.\r\n";
      check_serialization(r, expected);
    }
  }

  printf("all tests passed\n");
  return 0;
}
