#include "Protocol.hh"

#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <phosg/Strings.hh>

using namespace std;


static string evbuffer_readln_alloc(struct evbuffer* buf,
    enum evbuffer_eol_style eol_style) {

  size_t eol_len;
  struct evbuffer_ptr ptr = evbuffer_search_eol(buf, NULL, &eol_len, eol_style);
  if (ptr.pos == -1) {
    throw out_of_range("no line available");
  }

  string ret(ptr.pos, 0);
  evbuffer_remove(buf, const_cast<char*>(ret.data()), ptr.pos);
  evbuffer_drain(buf, eol_len);
  return ret;
}



DataCommand::DataCommand(size_t num_args) {
  this->args.reserve(num_args);
}

void DataCommand::print(FILE* stream, int indent_level) const {

  if (indent_level < 0) {
    indent_level = -indent_level;
  } else {
    print_indent(stream, indent_level);
  }

  fprintf(stream, "DataCommand[\n");
  for (const auto& arg : this->args) {
    print_indent(stream, indent_level + 1);
    for (char ch : arg) {
      if (ch < 0x20 || ch > 0x7F) {
        fprintf(stream, "\\x%02X", ch);
      } else {
        fputc(ch, stream);
      }
    }
    fprintf(stream, ",\n");
  }
  fprintf(stream, "]]");
}

string DataCommand::format() const {
  string ret = "[";

  for (const auto& arg : this->args) {
    if (ret.size() > 1) {
      ret += ',';
    }
    ret += '\'';
    for (char ch : arg) {
      if (ch < 0x20 || ch > 0x7F) {
        ret += string_printf("\\x%02X", ch);
      } else if (ch == '\'') {
        ret += "\\\'";
      } else {
        ret += ch;
      }
    }
    ret += '\'';
  }
  ret += ']';

  return ret;
}

void DataCommand::write(struct evbuffer* buf) const {
  if (!buf) {
    return;
  }

  evbuffer_add_printf(buf, "*%zu\r\n", this->args.size());

  for (const auto& arg : this->args) {
    evbuffer_add_printf(buf, "$%zu\r\n", arg.size());
    evbuffer_add(buf, arg.data(), arg.size());
    evbuffer_add(buf, "\r\n", 2);
  }
}



ReferenceCommand::DataReference::DataReference(const void* data, size_t size) :
    data(data), size(size) { }

ReferenceCommand::DataReference::DataReference(const string& data) :
    data(data.data()), size(data.size()) { }

ReferenceCommand::ReferenceCommand(size_t num_args) {
  this->args.reserve(num_args);
}

void ReferenceCommand::print(FILE* stream, int indent_level) const {

  if (indent_level < 0) {
    indent_level = -indent_level;
  } else {
    print_indent(stream, indent_level);
  }

  fprintf(stream, "ReferenceCommand[\n");
  for (const auto& arg : this->args) {
    print_indent(stream, indent_level + 1);
    for (size_t x = 0; x < arg.size; x++) {
      char ch = ((const char*)arg.data)[x];
      if (ch < 0x20 || ch > 0x7F) {
        fprintf(stream, "\\x%02X", ch);
      } else {
        fputc(ch, stream);
      }
    }
    fprintf(stream, ",\n");
  }
  fprintf(stream, "]]");
}

string ReferenceCommand::format() const {
  string ret = "[";

  for (const auto& arg : this->args) {
    if (ret.size() > 1) {
      ret += ',';
    }
    ret += '\'';
    for (size_t x = 0; x < arg.size; x++) {
      char ch = ((const char*)arg.data)[x];
      if (ch < 0x20 || ch > 0x7F) {
        ret += string_printf("\\x%02X", ch);
      } else if (ch == '\'') {
        ret += "\\\'";
      } else {
        ret += ch;
      }
    }
    ret += '\'';
  }
  ret += ']';

  return ret;
}

void ReferenceCommand::write(struct evbuffer* buf) const {
  if (!buf) {
    return;
  }

  evbuffer_add_printf(buf, "*%zu\r\n", this->args.size());

  for (const auto& arg : this->args) {
    evbuffer_add_printf(buf, "$%zu\r\n", arg.size);
    evbuffer_add(buf, arg.data, arg.size);
    evbuffer_add(buf, "\r\n", 2);
  }
}



Response::Response(Response::Type type, int64_t size) : type(type),
    int_value(size) {
  switch (this->type) {
    case Type::Status:
    case Type::Error:
    case Type::Data:
      if (size > 0) {
        this->fields.reserve(size);
      }
      break;

    case Type::Integer:
      this->int_value = 0;
      break;

    case Type::Multi:
      if (size > 0) {
        this->data.reserve(size);
      }
  }
}

Response::Response(Type type, const char* fmt, ...) : type(type), int_value(0) {
  va_list va;
  va_start(va, fmt);
  this->data = string_vprintf(fmt, va);
  va_end(va);
}

Response::Response(Type type, const void* data, size_t size) : type(type),
    data((const char*)data, size), int_value(0) { }

Response::Response(Type type, const string& data) : type(type), data(data),
    int_value(0) { }

bool Response::operator==(const Response& other) const {
  if (this->type != other.type) {
    return false;
  }

  // check for nulls
  if (this->type == Type::Data || this->type == Type::Multi) {
    if ((this->int_value < 0) && (other.int_value < 0)) {
      return true; // both are null
    }
    if ((this->int_value < 0) || (other.int_value < 0)) {
      return false; // one is null but the other isn't
    }
  }

  switch (this->type) {
    case Type::Status:
    case Type::Error:
    case Type::Data:
      return this->data == other.data;

    case Type::Integer:
      return this->int_value == other.int_value;

    case Type::Multi:
      if (this->fields.size() != other.fields.size()) {
        return false;
      }
      for (size_t x = 0; x < this->fields.size(); x++) {
        if (*this->fields[x] != *other.fields[x]) {
          return false;
        }
      }
      return true;

    default:
      return false;
  }
}

bool Response::operator!=(const Response& other) const {
  return !(this->operator==(other));
}

void Response::print(FILE* stream, int indent_level) const {

  if (indent_level < 0) {
    indent_level = -indent_level;
  } else {
    print_indent(stream, indent_level);
  }

  switch (this->type) {
    case Type::Status:
      fprintf(stream, "Response[type=Status, data=%s]", this->data.c_str());
      break;

    case Type::Error:
      fprintf(stream, "Response[type=Error, data=%s]", this->data.c_str());
      break;

    case Type::Integer:
      fprintf(stream, "Response[type=Integer, int_value=%" PRId64 "]",
          this->int_value);
      break;

    case Type::Data:
      if (this->int_value < 0) {
        fprintf(stream, "Response[type=Data, null]\n");
      } else {
        fprintf(stream, "Response[type=Data, data=");
        for (char ch : data) {
          if (ch < 0x20 || ch > 0x7F) {
            fprintf(stream, "\\x%02X", ch);
          } else {
            fputc(ch, stream);
          }
        }
        fputc(']', stream);
      }
      break;

    case Type::Multi:
      if (this->int_value < 0) {
        fprintf(stream, "Response[type=Multi, null]");
      } else {
        fprintf(stream, "Response[type=MULTI, fields=[\n");
        for (const auto& resp : this->fields) {
          resp->print(stream, indent_level + 1);
          fprintf(stream, ",\n");
        }
        print_indent(stream, indent_level);
        fprintf(stream, "]");
      }
      break;

    default:
      fprintf(stream, "Response[type=Unknown]\n");
  }
}

string Response::format() const {

  switch (this->type) {
    case Type::Status:
      return "(Status) " + this->data;

    case Type::Error:
      return "(Error) " + this->data;

    case Type::Integer:
      return string_printf("%" PRId64, this->int_value);

    case Type::Data:
      if (this->int_value < 0) {
        return "(Null)";
      } else {
        string ret = "\'";
        for (char ch : this->data) {
          if (ch < 0x20 || ch > 0x7F) {
            ret += string_printf("\\x%02X", ch);
          } else if (ch == '\'') {
            ret += "\\\'";
          } else {
            ret += ch;
          }
        }
        ret += '\'';
        return ret;
      }
      break;

    case Type::Multi:
      if (this->int_value < 0) {
        return "(Null)";
      } else {
        string ret = "[";
        for (const auto& f : this->fields) {
          if (ret.size() > 1) {
            ret += ", ";
          }
          ret += f->format();
        }
        ret += "]";
        return ret;
      }
      break;

    default:
      return string_printf("(UnknownType:%02" PRIX8 ")", (uint8_t)this->type);
  }
}

void Response::write(struct evbuffer* buf) const {

  if (!buf) {
    return;
  }

  switch (this->type) {
    case Type::Status:
    case Type::Error:
      this->write_string(buf, this->data.data(), this->data.size(),
          (char)this->type);
      break;

    case Type::Integer:
      this->write_int(buf, this->int_value, (char)Type::Integer);
      break;

    case Type::Data:
      if (this->int_value >= 0) {
        this->write_int(buf, this->data.size(), (char)Type::Data);
        evbuffer_add(buf, this->data.data(), this->data.size());
        evbuffer_add(buf, "\r\n", 2);
      } else {
        evbuffer_add(buf, "$-1\r\n", 5);
      }
      break;

    case Type::Multi:
      if (this->int_value >= 0) {
        this->write_int(buf, this->fields.size(), (char)Type::Multi);
        for (const auto& field : this->fields) {
          field->write(buf);
        }
      } else {
        evbuffer_add(buf, "*-1\r\n", 5);
      }
      break;

    default:
      throw runtime_error("invalid response type in write()");
  }
}

void Response::write_string(struct evbuffer* buf, const char* string,
    char sentinel) {
  if (!buf) {
    return;
  }
  if (sentinel == Response::Type::Data) {
    evbuffer_add_printf(buf, "$%zu\r\n%s\r\n", strlen(string), string);
  } else {
    evbuffer_add_printf(buf, "%c%s\r\n", sentinel, string);
  }
}

void Response::write_string(struct evbuffer* buf, const void* string,
    size_t size, char sentinel) {
  if (!buf) {
    return;
  }
  if (sentinel == Response::Type::Data) {
    evbuffer_add_printf(buf, "$%zu\r\n", size);
  } else {
    evbuffer_add(buf, &sentinel, 1);
  }
  evbuffer_add(buf, string, size);
  evbuffer_add(buf, "\r\n", 2);
}

void Response::write_int(struct evbuffer* buf, int64_t value,
    char sentinel) {
  if (!buf) {
    return;
  }
  evbuffer_add_printf(buf, "%c%" PRId64 "\r\n", sentinel, value);
}



CommandParser::CommandParser() : state(State::Initial) { }

shared_ptr<DataCommand> CommandParser::resume(struct evbuffer* buf) {
  for (;;) {
    switch (this->state) {
      case State::Initial: {
        // expect "*num_args\r\n", or inline command
        string input_line;
        try {
          input_line = evbuffer_readln_alloc(buf, EVBUFFER_EOL_CRLF);
        } catch (const out_of_range& e) {
          return NULL; // complete line not yet available
        }

        if (input_line[0] != '*') {
          // this is an inline command; split it on spaces
          shared_ptr<DataCommand> cmd(new DataCommand());
          auto& args = cmd->args;

          size_t arg_start_offset = 0;
          for (size_t x = 0; input_line[x];) {
            // find the end of the current token
            for (; input_line[x] && (input_line[x] != ' '); x++);

            args.emplace_back(&input_line[arg_start_offset], x - arg_start_offset);

            // find the start of the next argument
            for (; input_line[x] && (input_line[x] == ' '); x++);
            arg_start_offset = x;
          }

          // we're done. notice that this doesn't affect the parser state at all
          return cmd;

        }

        // not an inline command. move to reading-argument state
        this->arguments_remaining = strtoll(&input_line[1], NULL, 10);
        if (this->arguments_remaining <= 0) {
          throw runtime_error("command with zero or fewer arguments");
        }
        this->command_in_progress.reset(new DataCommand(this->arguments_remaining));
        this->state = State::ReadingArgumentSize;
        break;
      }

      case State::ReadingArgumentSize: {
        // expect "$arg_size\r\n"
        string input_line;
        try {
          input_line = evbuffer_readln_alloc(buf, EVBUFFER_EOL_CRLF);
        } catch (const out_of_range& e) {
          return NULL; // complete line not yet available
        }

        if (input_line[0] != '$') {
          throw runtime_error("didn\'t get command arg size where expected");
        } else {
          this->data_bytes_remaining = strtoull(&input_line[1], NULL, 10);
          this->command_in_progress->args.emplace_back();
          this->command_in_progress->args.back().reserve(
              this->data_bytes_remaining);
          this->state = State::ReadingArgumentData;
        }
        break;
      }

      case State::ReadingArgumentData: {
        // copy data into the last argument
        string& arg = this->command_in_progress->args.back();
        size_t bytes_available = evbuffer_get_length(buf);
        if (bytes_available == 0) {
          return NULL;
        }
        if (bytes_available > this->data_bytes_remaining) {
          bytes_available = this->data_bytes_remaining;
        }

        size_t bytes_existing = arg.size();
        arg.resize(bytes_existing + bytes_available);
        ssize_t bytes_copied = evbuffer_remove(buf,
            const_cast<char*>(arg.data()) + bytes_existing,
            bytes_available);
        if (bytes_copied < 0) {
          throw runtime_error("can\'t read from evbuffer");
        }
        this->data_bytes_remaining -= bytes_copied;

        // TODO: do we need to handle the case where bytes_copied != bytes_available?

        if (this->data_bytes_remaining == 0) {
          this->arguments_remaining--;
          this->state = State::ReadingNewlineAfterArgumentData;
        }
        break;
      }

      case State::ReadingNewlineAfterArgumentData:
        if (evbuffer_get_length(buf) < 2) {
          return NULL; // not ready yet
        }
        char data[2];
        if (2 != evbuffer_remove(buf, data, 2)) {
          throw runtime_error("can\'t read newline after argument data");
        }
        if (data[0] != '\r' && data[1] != '\n') {
          throw runtime_error("\\r\\n did not follow argument data");
        }

        // if we're expecting more arguments, move back to the appropriate
        // state. if not, return the command and return to the initial state.
        if (this->arguments_remaining) {
          this->state = State::ReadingArgumentSize;
        } else {
          this->state = State::Initial;
          return move(this->command_in_progress);
        }
        break;

      default:
        throw runtime_error("command parser got into unknown state");
    }
  }

  return NULL; // complete line not yet available
}



ResponseParser::ResponseParser() : state(State::Initial) { }

shared_ptr<Response> ResponseParser::resume(struct evbuffer* buf) {
  for (;;) {
    switch (this->state) {
      case State::Initial: {
        string input_line;
        try {
          input_line = evbuffer_readln_alloc(buf, EVBUFFER_EOL_CRLF);
        } catch (const out_of_range& e) {
          return NULL; // complete line not yet available
        }

        switch (input_line[0]) {
          case Response::Type::Status:
          case Response::Type::Error: {
            shared_ptr<Response> resp(new Response((Response::Type)input_line[0], (int64_t)0));
            input_line.erase(0, 1);
            resp->data = move(input_line);
            return resp;
          }

          case Response::Type::Integer: {
            shared_ptr<Response> resp(new Response(Response::Type::Integer));
            resp->int_value = strtoll(&input_line[1], NULL, 10);
            return resp;
          }

          case Response::Type::Data: {
            this->data_bytes_remaining = strtoll(&input_line[1], NULL, 0);
            if (this->data_bytes_remaining < 0) {
              return shared_ptr<Response>(new Response(Response::Type::Data,
                  this->data_bytes_remaining));
            }

            this->response_in_progress.reset(new Response(Response::Type::Data,
                this->data_bytes_remaining));
            this->state = (this->data_bytes_remaining ? State::ReadingData :
                State::ReadingNewlineAfterData);
            break;
          }

          case Response::Type::Multi: {
            this->multi_fields_remaining = strtoll(&input_line[1], NULL, 0);
            if (this->multi_fields_remaining <= 0) {
              return shared_ptr<Response>(new Response(Response::Type::Multi,
                  this->multi_fields_remaining));
            }

            this->response_in_progress.reset(new Response(Response::Type::Multi,
                this->multi_fields_remaining));
            this->multi_in_progress.reset(new ResponseParser());
            this->state = State::MultiRecursive;
            break; }

          default:
            throw runtime_error(string_printf("incorrect sentinel: %c", input_line[0]));
        }
        break; // State::Initial
      }

      case State::MultiRecursive: {
        for (;;) {
          auto field = this->multi_in_progress->resume(buf);
          if (!field.get()) {
            return NULL;
          }

          this->response_in_progress->fields.emplace_back(field);
          this->multi_fields_remaining--;
          if (this->multi_fields_remaining == 0) {
            this->state = State::Initial;
            return move(this->response_in_progress);
          }
        }
        break; // State::MultiRecursive
      }

      case State::ReadingData: {
        // copy data into the data field
        size_t bytes_available = evbuffer_get_length(buf);
        if (bytes_available == 0) {
          return NULL;
        }
        if (bytes_available > this->data_bytes_remaining) {
          bytes_available = this->data_bytes_remaining;
        }

        size_t bytes_existing = this->response_in_progress->data.size();
        this->response_in_progress->data.resize(bytes_existing + bytes_available);
        ssize_t bytes_copied = evbuffer_remove(buf,
            const_cast<char*>(this->response_in_progress->data.data()) + bytes_existing,
            bytes_available);
        if (bytes_copied < 0) {
          throw runtime_error("can\'t read from evbuffer");
        }
        this->data_bytes_remaining -= bytes_copied;

        if (this->data_bytes_remaining == 0) {
          this->state = State::ReadingNewlineAfterData;
        }
        break;
      }

      case State::ReadingNewlineAfterData:
        if (evbuffer_get_length(buf) < 2) {
          return NULL; // not ready yet
        }
        char data[2];
        if (2 != evbuffer_remove(buf, data, 2)) {
          throw runtime_error("can\'t read newline after argument data");
        }
        if (data[0] != '\r' && data[1] != '\n') {
          throw runtime_error("\\r\\n did not follow argument data");
        }

        this->state = State::Initial;
        return move(this->response_in_progress);

      default:
        throw runtime_error("response parser got into unknown state");
    }
  }
  return NULL;
}

bool ResponseParser::forward(struct evbuffer* buf,
    struct evbuffer* output_buffer) {

  // output_buffer can be NULL if the client has already disconnected. in this
  // case, we just don't write to the output buffer (discard the response).
  for (;;) {
    switch (this->state) {
      case State::Initial: {
        string input_line;
        try {
          input_line = evbuffer_readln_alloc(buf, EVBUFFER_EOL_CRLF);
        } catch (const out_of_range& e) {
          return false; // complete line not yet available
        }

        // forward the line to the client immediately
        if (output_buffer) {
          evbuffer_add(output_buffer, input_line.data(), input_line.size());
          evbuffer_add(output_buffer, "\r\n", 2);
        }

        switch (input_line[0]) {
          case Response::Type::Status:
          case Response::Type::Error:
          case Response::Type::Integer:
            return true;

          case Response::Type::Data:
            // we add 2 here for the trailing \r\n
            this->data_bytes_remaining = strtoll(&input_line[1], NULL, 0);
            if (this->data_bytes_remaining < 0) {
              return true; // null response
            } else {
              this->state = State::ReadingData;
            }
            break;

          case Response::Type::Multi:
            this->multi_fields_remaining = strtoll(&input_line[1], NULL, 0);
            if (this->multi_fields_remaining <= 0) {
              return true; // null response
            } else {
              this->multi_in_progress.reset(new ResponseParser());
              this->state = State::MultiRecursive;
            }
            break;

          default:
            throw runtime_error(string_printf("incorrect sentinel: %c", input_line[0]));
        }
        break; // State::Initial
      }

      case State::MultiRecursive: {
        for (;;) {
          bool field_forwarded = this->multi_in_progress->forward(buf,
              output_buffer);
          if (!field_forwarded) {
            return false;
          }

          this->multi_fields_remaining--;
          if (this->multi_fields_remaining == 0) {
            this->state = State::Initial;
            return true;
          }
        }
        break;
      }

      case State::ReadingData: {
        size_t bytes_available = evbuffer_get_length(buf);
        if (bytes_available == 0) {
          return false;
        }
        if (bytes_available > this->data_bytes_remaining) {
          bytes_available = this->data_bytes_remaining;
        }

        if (output_buffer) {
          ssize_t bytes_copied = evbuffer_remove_buffer(buf, output_buffer,
              bytes_available);
          if (bytes_copied < 0) {
            throw runtime_error("can\'t read from evbuffer");
          }
          this->data_bytes_remaining -= bytes_copied;
        } else {
          evbuffer_drain(buf, bytes_available);
          this->data_bytes_remaining -= bytes_available;
        }

        if (this->data_bytes_remaining == 0) {
          this->state = State::ReadingNewlineAfterData;
        }
        break;
      }

      case State::ReadingNewlineAfterData: {
        if (evbuffer_get_length(buf) < 2) {
          return false; // not ready yet
        }
        char data[2];
        if (2 != evbuffer_remove(buf, data, 2)) {
          throw runtime_error("can\'t read newline after argument data");
        }
        if (data[0] != '\r' && data[1] != '\n') {
          throw runtime_error("\\r\\n did not follow argument data");
        }
        if (output_buffer) {
          evbuffer_add(output_buffer, "\r\n", 2);
        }

        this->state = State::Initial;
        return true;
      }

      default:
        throw runtime_error("response parser got into unknown state");
    }
  }

  return false;
}
