#pragma once

#include <event2/buffer.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>


// DataCommand and ReferenceCommand aren't subclasses of a parent Command class
// because this incurs a significant performance penalty (up to 7% in some
// cases).

struct DataCommand {
  std::vector<std::string> args;

  DataCommand() = default;
  explicit DataCommand(size_t num_args);
  ~DataCommand() = default;

  void print(FILE* stream, int indent_level = 0) const;
  std::string format() const;

  void write(struct evbuffer* buf) const;
};


struct ReferenceCommand {
  struct DataReference {
    const void* data;
    size_t size;

    DataReference();
    DataReference(const void* data, size_t size);
    DataReference(const std::string& data);
  };

  std::vector<DataReference> args;

  ReferenceCommand() = default;
  explicit ReferenceCommand(size_t num_args);
  ~ReferenceCommand() = default;

  void print(FILE* stream, int indent_level = 0) const;
  std::string format() const;

  void write(struct evbuffer* buf) const;
};


struct Response {
  enum Type {
    Status = '+',
    Error = '-',
    Integer = ':',
    Data = '$',
    Multi = '*',
  };
  Type type;

  std::string data; // Status, Error and Data
  int64_t int_value; // Integer
  std::vector<std::shared_ptr<Response>> fields; // Multi

  Response(Type type, int64_t size = 0);
  Response(Type type, const char* fmt, ...);
  Response(Type type, const void* data, size_t size);
  Response(Type type, const std::string& data);
  ~Response() = default;

  bool operator==(const Response& other) const;
  bool operator!=(const Response& other) const;

  void print(FILE* stream, int indent_level = 0) const;
  std::string format() const;

  void write(struct evbuffer* buf) const;
  static void write_string(struct evbuffer* buf, const char* s, char sentinel);
  static void write_string(struct evbuffer* buf, const void* s, size_t size,
      char sentinel);
  static void write_int(struct evbuffer* buf, int64_t value, char sentinel);
};


struct CommandParser {
  enum State {
    Initial = 0,
    ReadingArgumentSize,
    ReadingArgumentData,
    ReadingNewlineAfterArgumentData,
  };
  State state;
  const char* error_str;

  int64_t num_command_args;
  std::shared_ptr<DataCommand> command_in_progress;
  int64_t arguments_remaining;
  int64_t data_bytes_remaining;

  CommandParser();
  ~CommandParser() = default;

  std::shared_ptr<DataCommand> resume(struct evbuffer* buffer);

  const char* error() const;
};

struct ResponseParser {
  enum State {
    Initial = 0,
    MultiRecursive,
    ReadingData,
    ReadingNewlineAfterData,
  };
  State state;
  const char* error_str;

  std::shared_ptr<Response> response_in_progress;
  int64_t data_bytes_remaining;

  std::shared_ptr<ResponseParser> multi_in_progress;
  int64_t multi_fields_remaining;

  ResponseParser();
  ~ResponseParser() = default;

  std::shared_ptr<Response> resume(struct evbuffer* buffer);
  bool forward(struct evbuffer* buffer, struct evbuffer* output_buffer);

  const char* error() const;
};
