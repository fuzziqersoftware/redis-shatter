#define _STDC_FORMAT_MACROS

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <event2/bufferevent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>

#include <phosg/Network.hh>
#include <phosg/Process.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>

#include "Protocol.hh"
#include "Proxy.hh"

using namespace std;
using CollectionType = ResponseLink::CollectionType;



////////////////////////////////////////////////////////////////////////////////
// BackendConnection implementation

BackendConnection::BackendConnection(Backend* backend, int64_t index,
    std::unique_ptr<struct bufferevent, void(*)(struct bufferevent*)>&& new_bev)
    : backend(backend), index(index), bev(move(new_bev)), parser(),
    local_addr(), remote_addr(), num_commands_sent(0),
    num_responses_received(0), head_link(NULL), tail_link(NULL) {
  get_socket_addresses(bufferevent_getfd(this->bev.get()), &this->local_addr,
      &this->remote_addr);
}

BackendConnection::~BackendConnection() {
  // BackendConnection objects should never be destroyed if they're linked to
  // any ResponseLinks. to safely destroy a BackendConnection, call
  // disconnect_backend.
  assert(!this->head_link);
}

struct evbuffer* BackendConnection::get_output_buffer() {
  return bufferevent_get_output(this->bev.get());
}

void BackendConnection::print(FILE* stream, int indent_level) const {
  fprintf(stream, "BackendConnection[backend=<%s>, index=%" PRId64 ", io_counts=[%zu, %zu], chain=[",
      this->backend->debug_name.c_str(), this->index, this->num_commands_sent,
      this->num_responses_received);
  for (ResponseLink* l = this->head_link; l; l = l->backend_conn_to_next_link.at(const_cast<BackendConnection*>(this))) {
    fputc('\n', stream);
    print_indent(stream, indent_level + 1);
    l->print(stream, indent_level + 1);
    fputc(',', stream);
  }
  fprintf(stream, "]]");
}



////////////////////////////////////////////////////////////////////////////////
// Backend implementation

Backend::Backend(size_t index, const string& host, int port, const string& name)
    : index(index), host(host), port(port), name(name),
    debug_name(string_printf("%s:%d@%s", this->host.c_str(), this->port,
      this->name.c_str())), index_to_connection(), next_connection_index(0),
    num_responses_received(0), num_commands_sent(0) { }

void Backend::print(FILE* stream, int indent_level) const {
  fprintf(stream, "Backend[index=%zu, debug_name=%s, io_counts=[%zu, %zu], next_connection_index=%" PRId64 ", connections=[",
      this->index, this->debug_name.c_str(), this->num_responses_received,
      this->num_commands_sent, this->next_connection_index);
  for (const auto& conn_it : this->index_to_connection) {
    fputc('\n', stream);
    print_indent(stream, indent_level + 1);
    fprintf(stream, "%" PRId64 " = ", conn_it.first);
    conn_it.second.print(stream, indent_level + 1);
    fputc(',', stream);
  }
  fputc(']', stream);
}



////////////////////////////////////////////////////////////////////////////////
// Client implementation

Client::Client(unique_ptr<struct bufferevent, void(*)(struct bufferevent*)> bev)
    : name(), should_disconnect(false), bev(move(bev)), parser(),
    local_addr(), remote_addr(), num_commands_received(0),
    num_responses_sent(0), head_link(NULL), tail_link(NULL) {
  get_socket_addresses(bufferevent_getfd(this->bev.get()), &this->local_addr,
      &this->remote_addr);
  this->name = render_sockaddr_storage(this->remote_addr) +
      string_printf("@%d", bufferevent_getfd(this->bev.get()));
}

Client::~Client() {
  ResponseLink* l = this->head_link;
  while (l) {
    ResponseLink* next_l = l->next_client;

    assert(l->client == this);
    l->client = NULL;
    l->next_client = NULL;
    if (l->is_ready()) {
      // there's no backend connection that needs this object after us, so don't
      // leave it lying around
      delete l;
    }

    l = next_l;
  }
}

struct evbuffer* Client::get_output_buffer() {
  return bufferevent_get_output(this->bev.get());
}

void Client::print(FILE* stream, int indent_level) const {
  fprintf(stream, "Client[name=%s, should_disconnect=%s, io_counts=[%zu, %zu], chain=[",
      this->name.c_str(), this->should_disconnect ? "true" : "false",
      this->num_commands_received, this->num_responses_sent);

  for (ResponseLink* l = this->head_link; l; l = l->next_client) {
    fputc('\n', stream);
    print_indent(stream, indent_level + 1);
    l->print(stream, indent_level + 1);
    fputc(',', stream);
  }

  fprintf(stream, "]]");
}



////////////////////////////////////////////////////////////////////////////////
// ResposneLink implementation

const char* ResponseLink::name_for_collection_type(CollectionType type) {
  switch (type) {
    case CollectionType::ForwardResponse:
      return "ForwardResponse";
    case CollectionType::CollectStatusResponses:
      return "CollectStatusResponses";
    case CollectionType::SumIntegerResponses:
      return "SumIntegerResponses";
    case CollectionType::CombineMultiResponses:
      return "CombineMultiResponses";
    case CollectionType::CollectResponses:
      return "CollectResponses";
    case CollectionType::CollectMultiResponsesByKey:
      return "CollectMultiResponsesByKey";
    case CollectionType::CollectIdenticalResponses:
      return "CollectIdenticalResponses";
    case CollectionType::ModifyScanResponse:
      return "ModifyScanResponse";
    case CollectionType::ModifyScriptExistsResponse:
      return "ModifyScriptExistsResponse";
    default:
      return "UnknownCollectionType";
  }
}

ResponseLink::ResponseLink(CollectionType type, Client* client) : type(type),
    client(client), next_client(NULL), backend_conn_to_next_link(),
    error_response(), response_to_forward(), response_integer_sum(0),
    expected_response_type(Response::Type::Status), responses(),
    recombination_queue(), backend_index_to_response(), scan_backend_index(0) {
  // link this object from the Client
  if (this->client->tail_link) {
    this->client->tail_link->next_client = this;
  } else {
    this->client->head_link = this;
  }
  this->client->tail_link = this;
}

ResponseLink::~ResponseLink() {
  // ResponseLink objects should never be destroyed if they're part of a client
  // or backend chain
  assert(!this->next_client);
  assert(this->backend_conn_to_next_link.empty());
}

bool ResponseLink::is_ready() const {
  return this->backend_conn_to_next_link.empty();
}

void ResponseLink::print(FILE* stream, int indent_level) const {

  // note: we don't print backend_conn_to_next_link or next_client because
  // these objects are usually printed in traversal order by Client::print or
  // BackendConnection::print

  string error_response_str = this->error_response ?
      (", error_response=" + this->error_response->format()) : "";
  string client_str = this->client ?
      string_printf("<%s>", this->client->name.c_str()) : "(missing)";

  string data = string_printf(
      "ResponseLink[@=%p, type=%s, client=%s%s",
      this, this->name_for_collection_type(this->type), client_str.c_str(),
      error_response_str.c_str());
  switch (this->type) {
    case CollectionType::ModifyScanResponse:
      data += string_printf(", scan_backend_index=%" PRId64,
          this->scan_backend_index);
      // intentional fallthrough; this type uses response_to_forward also

    case CollectionType::ForwardResponse:
      if (this->response_to_forward.get()) {
        data += ", response_to_forward=";
        data += this->response_to_forward->format();
      } else {
        data += ", response_to_forward=(not present)";
      }
      break;

    case CollectionType::CollectStatusResponses:
      break;

    case CollectionType::SumIntegerResponses:
      data += string_printf(", response_integer_sum=%" PRId64,
          this->response_integer_sum);
      break;

    case CollectionType::CombineMultiResponses:
    case CollectionType::CollectResponses:
    case CollectionType::CollectIdenticalResponses:
    case CollectionType::ModifyScriptExistsResponse:
      data += ", responses=[";
      for (const auto& r : this->responses) {
        data += r->format();
        data += ',';
      }
      data += ']';
      break;

    case CollectionType::CollectMultiResponsesByKey:
      data += string_printf(", backend_index_to_response=(%zu items)"
          ", recombination_queue=(%zu items)",
          this->backend_index_to_response.size(),
          this->recombination_queue.size());
      break;
  }
  data += ']';

  fwrite(data.data(), 1, data.size(), stream);
}



////////////////////////////////////////////////////////////////////////////////
// Proxy public functions

Proxy::Proxy(int listen_fd, const vector<ConsistentHashRing::Host>& hosts,
    int hash_begin_delimiter, int hash_end_delimiter) :
    listen_fd(listen_fd), base(event_base_new(), event_base_free),
    listener(evconnlistener_new(this->base.get(),
        Proxy::dispatch_on_client_accept, this, LEV_OPT_REUSEABLE, 0,
        this->listen_fd), evconnlistener_free),
    ring(hosts), index_to_backend(), name_to_backend(), bev_to_backend_conn(),
    bev_to_client(), num_commands_received(0), num_commands_sent(0),
    num_responses_received(0), num_responses_sent(0),
    num_connections_received(0), start_time(now()),
    hash_begin_delimiter(hash_begin_delimiter),
    hash_end_delimiter(hash_end_delimiter), handlers(this->default_handlers) {

  evconnlistener_set_error_cb(this->listener.get(), Proxy::dispatch_on_listen_error);

  // set up backend structures
  for (const auto& host : hosts) {
    Backend& b = this->index_to_backend.emplace(piecewise_construct,
        forward_as_tuple(this->index_to_backend.size()),
        forward_as_tuple(this->index_to_backend.size(), host.host, host.port,
            host.name)).first->second;
    this->name_to_backend.emplace(b.name, &b);
  }
}

bool Proxy::disable_command(const string& command_name) {
  return this->handlers.erase(command_name);
}

void Proxy::serve() {
  this->start_time = now();
  event_base_dispatch(this->base.get());
}

void Proxy::print(FILE* stream, int indent_level) const {
  if (indent_level < 0) {
    indent_level = -indent_level;
  } else {
    print_indent(stream, indent_level);
  }

  fprintf(stream, "Proxy[listen_fd=%d, num_clients=%zu, io_counts=[%zu, %zu, %zu, %zu], clients=[\n",
      this->listen_fd, this->bev_to_client.size(), this->num_commands_received,
      this->num_commands_sent, this->num_responses_received,
      this->num_responses_sent);

  for (const auto& bev_client : this->bev_to_client) {
    print_indent(stream, indent_level + 1);
    bev_client.second.print(stream, indent_level + 1);
    fprintf(stream, ",\n");
  }

  fprintf(stream, "], backends=[\n");
  for (const auto& backend_it : this->index_to_backend) {
    print_indent(stream, indent_level + 1);
    fprintf(stream, "%" PRId64 " = ", backend_it.first);
    backend_it.second.print(stream, indent_level + 1);
    fprintf(stream, ",\n");
  }

  print_indent(stream, indent_level);
  fprintf(stream, "]]");
}



////////////////////////////////////////////////////////////////////////////////
// backend lookups

int64_t Proxy::backend_index_for_key(const string& s) const {
  size_t hash_begin_pos = (this->hash_begin_delimiter >= 0) ?
      s.find(this->hash_begin_delimiter) : 0;
  size_t hash_end_pos = (this->hash_end_delimiter >= 0) ?
      s.rfind(this->hash_end_delimiter) : 0;

  if (hash_end_pos == string::npos) {
    hash_end_pos = s.size();
  }

  if (hash_begin_pos == string::npos) {
    hash_begin_pos = 0;
  } else {
    hash_begin_pos++; // don't include the delimiter itself
  }

  if (hash_end_pos <= hash_begin_pos) {
    hash_begin_pos = 0;
  }

  return this->ring.host_id_for_key(s.data() + hash_begin_pos,
      hash_end_pos - hash_begin_pos);
}

int64_t Proxy::backend_index_for_argument(const string& arg) const {
  try {
    return this->name_to_backend.at(arg)->index;
  } catch (const std::out_of_range& e) {
    char* endptr;
    int64_t backend_index = strtoll(arg.data(), &endptr, 0);
    if ((endptr == arg.data()) ||
        (backend_index < 0 || backend_index >= this->index_to_backend.size())) {
      return -1;
    }
    return backend_index;
  }
}

Backend& Proxy::backend_for_index(size_t index) {
  return this->index_to_backend.at(index);
}

Backend& Proxy::backend_for_key(const string& s) {
  return this->backend_for_index(this->backend_index_for_key(s));
}

BackendConnection& Proxy::backend_conn_for_index(size_t index) {
  Backend& b = this->backend_for_index(index);
  auto conn_it = b.index_to_connection.begin();
  if (conn_it != b.index_to_connection.end()) {
    return conn_it->second;
  }

  // there's no open connection for this backend; make a new one
  int fd = connect(b.host, b.port);
  if (fd < 0) {
    string error = string_for_error(errno);
    throw runtime_error(string_printf(
        "error: can\'t connect to backend %s:%d (fd=%d, errno=%d) (%s)\n",
        b.host.c_str(), b.port, fd, errno, error.c_str()));
  }

  int fd_flags = fcntl(fd, F_GETFD, 0);
  if (fd_flags >= 0) {
    fd_flags |= FD_CLOEXEC;
    fcntl(fd, F_SETFD, fd_flags);
  }

  // set up a bufferevent for the new connection
  unique_ptr<struct bufferevent, void(*)(struct bufferevent*)> bev(
      bufferevent_socket_new(this->base.get(), fd, BEV_OPT_CLOSE_ON_FREE),
      bufferevent_free);

  BackendConnection& conn = b.index_to_connection.emplace(piecewise_construct,
      forward_as_tuple(b.next_connection_index),
      forward_as_tuple(&b, b.next_connection_index, move(bev))).first->second;
  b.next_connection_index++;
  this->bev_to_backend_conn[conn.bev.get()] = &conn;

  bufferevent_setcb(conn.bev.get(), Proxy::dispatch_on_backend_input, NULL,
      Proxy::dispatch_on_backend_error, this);
  bufferevent_enable(conn.bev.get(), EV_READ | EV_WRITE);

  return conn;
}

BackendConnection& Proxy::backend_conn_for_key(const string& s) {
  return this->backend_conn_for_index(this->backend_index_for_key(s));
}



////////////////////////////////////////////////////////////////////////////////
// connection management

void Proxy::disconnect_client(Client* c) {
  auto bev_it = this->bev_to_client.find(c->bev.get());
  assert((bev_it != this->bev_to_client.end()) && (&bev_it->second == c));
  this->bev_to_client.erase(bev_it);
  // the Client destructor will close the connection and unlink any ResponseLink
  // objects appropriately
}

void Proxy::disconnect_backend(BackendConnection* conn) {
  // issue a fake error response to all waiting clients
  static shared_ptr<Response> error_response(new Response(
      Response::Type::Error,
      "CHANNELERROR backend disconnected before sending the response"));
  while (conn->head_link) {
    this->handle_backend_response(conn, error_response);
  }

  // remove the bev -> BackendConnection reference before deleting the
  // connection itself
  this->bev_to_backend_conn.erase(conn->bev.get());

  // delete from the connections map. after doing the above, we should have
  // eliminated all references to the BackendConnection object and it should be
  // safe to delete
  conn->backend->index_to_connection.erase(conn->index);
}



////////////////////////////////////////////////////////////////////////////////
// response linking

ResponseLink* Proxy::create_link(CollectionType type, Client* c) {
  return new ResponseLink(type, c);
}

struct evbuffer* Proxy::can_send_command(BackendConnection* conn,
    ResponseLink* l) {
  assert(!l->backend_conn_to_next_link.count(conn));

  if (l->error_response.get()) {
    return NULL;
  }

  if (!conn) {
    static shared_ptr<Response> r(new Response(Response::Type::Error,
        "CHANNELERROR backend is missing"));
    l->error_response = r;
    return NULL;
  }

  struct evbuffer* out = conn->get_output_buffer();
  if (!out) {
    static shared_ptr<Response> r(new Response(Response::Type::Error,
        "CHANNELERROR backend is not connected"));
    l->error_response = r;
  }
  return out;
}

void Proxy::link_connection(BackendConnection* conn, ResponseLink* l) {
  l->backend_conn_to_next_link.emplace(conn, nullptr);

  if (!conn->tail_link) {
    conn->head_link = l;
  } else {
    conn->tail_link->backend_conn_to_next_link[conn] = l;
  }
  conn->tail_link = l;

  conn->num_commands_sent++;
  conn->backend->num_commands_sent++;
  this->num_commands_sent++;
}

void Proxy::send_command_and_link(BackendConnection* conn, ResponseLink* l,
    const DataCommand* cmd) {

  struct evbuffer* out = this->can_send_command(conn, l);
  if (out) {
    cmd->write(out);
    this->link_connection(conn, l);
  }
}

void Proxy::send_command_and_link(BackendConnection* conn, ResponseLink* l,
    const shared_ptr<const DataCommand>& cmd) {
  this->send_command_and_link(conn, l, cmd.get());
}

void Proxy::send_command_and_link(BackendConnection* conn, ResponseLink* l,
    const ReferenceCommand* cmd) {

  struct evbuffer* out = this->can_send_command(conn, l);
  if (out) {
    cmd->write(out);
    this->link_connection(conn, l);
  }
}



////////////////////////////////////////////////////////////////////////////////
// high-level output handlers

void Proxy::send_client_response(Client* c, const Response* r) {

  struct evbuffer* out = c->get_output_buffer();
  if (!out) {
    log(WARNING, "tried to send response to client %s with no output buffer",
        c->name.c_str());
    return;
  }

  r->write(out);
  c->num_responses_sent++;
  this->num_responses_sent++;
}

void Proxy::send_client_response(Client* c, const shared_ptr<const Response>& r) {
  this->send_client_response(c, r.get());
}

void Proxy::send_client_string_response(Client* c, const char* s,
    Response::Type type) {

  struct evbuffer* out = c->get_output_buffer();
  if (!out) {
    log(WARNING, "tried to send response to client %s with no output buffer",
        c->name.c_str());
    return;
  }

  Response::write_string(out, s, type);
  c->num_responses_sent++;
  this->num_responses_sent++;
}

void Proxy::send_client_string_response(Client* c, const string& s,
    Response::Type type) {

  struct evbuffer* out = c->get_output_buffer();
  if (!out) {
    log(WARNING, "tried to send response to client %s with no output buffer",
        c->name.c_str());
    return;
  }

  Response::write_string(out, s.data(), s.size(), type);
  c->num_responses_sent++;
  this->num_responses_sent++;
}

void Proxy::send_client_string_response(Client* c, const void* data,
    size_t size, Response::Type type) {

  struct evbuffer* out = c->get_output_buffer();
  if (!out) {
    log(WARNING, "tried to send response to client %s with no output buffer",
        c->name.c_str());
    return;
  }

  Response::write_string(out, data, size, type);
  c->num_responses_sent++;
  this->num_responses_sent++;
}

void Proxy::send_client_int_response(Client* c, int64_t int_value,
    Response::Type type) {

  struct evbuffer* out = c->get_output_buffer();
  if (!out) {
    log(WARNING, "tried to send response to client %s with no output buffer",
        c->name.c_str());
    return;
  }

  Response::write_int(out, int_value, type);
  c->num_responses_sent++;
  this->num_responses_sent++;
}



////////////////////////////////////////////////////////////////////////////////
// high-level input handlers

static shared_ptr<Response> bad_upstream_error_response(
    new Response(Response::Type::Error,
      "CHANNELERROR an upstream server returned a bad response"));
static shared_ptr<Response> wrong_type_error_response(
    new Response(Response::Type::Error,
      "CHANNELERROR an upstream server returned a result of the wrong type"));
static shared_ptr<Response> no_command_error_response(
    new Response(Response::Type::Error,
      "CHANNELERROR received a response from a server that was not sent a command"));
static shared_ptr<Response> incorrect_count_error_response(
    new Response(Response::Type::Error,
      "CHANNELERROR a backend returned an incorrect result count"));
static shared_ptr<Response> unknown_collection_type_error_response(
    new Response(Response::Type::Error,
      "PROXYERROR unknown response wait type"));
static shared_ptr<Response> non_identical_results_error_response(
    new Response(Response::Type::Error,
      "CHANNELERROR backends did not return identical results"));
static shared_ptr<Response> no_data_error_response(
    new Response(Response::Type::Error, "PROXYERROR no data was returned"));

void Proxy::send_ready_response(ResponseLink* l) {

  if (l->error_response) {
    this->send_client_response(l->client, l->error_response);
    return;
  }

  switch (l->type) {

    case CollectionType::ForwardResponse:
      this->send_client_response(l->client, l->response_to_forward);
      break;

    case CollectionType::CollectStatusResponses:
      this->send_client_string_response(l->client, "OK", Response::Type::Status);
      break;

    case CollectionType::SumIntegerResponses:
      this->send_client_int_response(l->client, l->response_integer_sum, Response::Type::Integer);
      break;

    case CollectionType::CombineMultiResponses: {
      size_t num_fields = 0;
      for (const auto& backend_r : l->responses) {
        if (!backend_r) {
          this->send_client_response(l->client, bad_upstream_error_response);
          return;
        }
        if (backend_r->type != Response::Type::Multi) {
          this->send_client_response(l->client, wrong_type_error_response);
          return;
        }
        num_fields += backend_r->fields.size();
      }

      Response r(Response::Type::Multi, num_fields);
      for (const auto& backend_r : l->responses) {
        // note: we skip null responses here because it doesn't make sense to
        // aggregate them into one - these should have fields.size() == 0
        // anyway, so it's safe to not handle them explicitly
        for (const auto& backend_r_field : backend_r->fields) {
          r.fields.emplace_back(backend_r_field);
        }
      }
      this->send_client_response(l->client, &r);
      break;
    }

    case CollectionType::CollectResponses: {
      Response r(Response::Type::Multi, l->responses.size());
      for (const auto& backend_r : l->responses) {
        r.fields.emplace_back(backend_r.get() ? backend_r : bad_upstream_error_response);
      }
      this->send_client_response(l->client, &r);
      break;
    }

    case CollectionType::CollectMultiResponsesByKey: {
      Response r(Response::Type::Multi, l->recombination_queue.size());

      unordered_map<int64_t, size_t> backend_index_to_offset;
      for (int64_t backend_index : l->recombination_queue) {
        auto offset_it = backend_index_to_offset.find(backend_index);
        if (offset_it == backend_index_to_offset.end()) {
          offset_it = backend_index_to_offset.emplace(backend_index, 0).first;
        }

        try {
          auto& backend_r = l->backend_index_to_response.at(backend_index);
          // we don't check the response type - we assume .fields will be blank
          // if the response isn't a Multi
          r.fields.emplace_back(backend_r->fields.at(offset_it->second));
        } catch (const out_of_range& e) {
          this->send_client_string_response(l->client,
              "PROXYERROR a backend sent an incorrect key count or did not reply",
              Response::Type::Error);
          return;
        }
        offset_it->second++;
      }

      // check that we used all the input data
      bool response_ok = backend_index_to_offset.size() == l->backend_index_to_response.size();
      if (response_ok) {
        for (const auto& it : l->backend_index_to_response) {
          try {
            if (it.second->type != Response::Type::Multi) {
              this->send_client_string_response(l->client,
                  "PROXYERROR a backend returned a non-multi response",
                  Response::Type::Error);
              return;
            }

            size_t offset = backend_index_to_offset.at(it.first);
            if (offset != it.second->fields.size()) {
              this->send_client_string_response(l->client,
                  "PROXYERROR did not use all of at least one backend response",
                  Response::Type::Error);
              return;
            }
          } catch (const out_of_range& e) {
            this->send_client_string_response(l->client,
                "PROXYERROR at least one backend response was not handled",
                Response::Type::Error);
            return;
          }
        }
      }

      // if we get here, then all is well; send it off
      this->send_client_response(l->client, &r);
      break;
    }

    case CollectionType::CollectIdenticalResponses: {
      for (size_t x = 1; x < l->responses.size(); x++) {
        if (*l->responses[x] != *l->responses[0]) {
          this->send_client_response(l->client,
              non_identical_results_error_response);
          return;
        }
      }

      this->send_client_response(l->client, l->responses[0]);
      break;
    }

    case CollectionType::ModifyScanResponse: {
      if ((l->response_to_forward->type != Response::Type::Multi) ||
          (l->response_to_forward->fields.size() != 2) ||
          (l->response_to_forward->fields[0]->type != Response::Type::Data)) {
        this->send_client_response(l->client, wrong_type_error_response);
        break;
      }

      // if this backend is done, go to the next one (if any)
      auto cursor = l->response_to_forward->fields[0];
      if (cursor->data == "0") {
        int64_t next_backend_id = l->scan_backend_index + 1;
        if (next_backend_id < this->index_to_backend.size()) {
          l->response_to_forward->fields[0]->data = string_printf(
              "%" PRId64 ":0", next_backend_id);
        }

      // if this backend isn't done, add the backend prefix on the cursor
      } else {
        // there might be a use-after-free if we just reassign cursor->data,
        // since cursor.data is heap-allocated and may be freed before
        // string_printf is called
        string cursor_contents = cursor->data;
        cursor->data = string_printf("%" PRId64 ":%.*s",
            l->scan_backend_index, cursor_contents.size(),
            cursor_contents.data());
      }

      this->send_client_response(l->client, l->response_to_forward);
      break;
    }

    case CollectionType::ModifyScriptExistsResponse: {
      // expect a multi response with integer fields
      shared_ptr<Response> r;
      for (const auto& backend_r : l->responses) {
        if (backend_r->type != Response::Type::Multi) {
          this->send_client_response(l->client, wrong_type_error_response);
          return;
        }

        if (!r.get()) {
          r.reset(new Response(Response::Type::Multi, backend_r->fields.size()));
          r->fields.resize(backend_r->fields.size());
        } else {
          if (r->fields.size() != backend_r->fields.size()) {
            this->send_client_response(l->client,
                incorrect_count_error_response);
            return;
          }
        }

        for (size_t x = 0; x < backend_r->fields.size(); x++) {
          const auto& backend_r_field = backend_r->fields[x];

          if (backend_r_field->type != Response::Type::Integer) {
            this->send_client_response(l->client, wrong_type_error_response);
            return;
          }

          if (!r->fields[x].get()) {
            r->fields[x].reset(new Response(Response::Type::Integer,
                backend_r_field->int_value));
            r->fields[x]->int_value = backend_r_field->int_value;
          } else {
            r->fields[x]->int_value &= backend_r_field->int_value;
          }
        }
      }
      if (r.get()) {
        this->send_client_response(l->client, r);
      } else {
        this->send_client_response(l->client, no_data_error_response);
      }
      break;
    }

    default:
      this->send_client_response(l->client, unknown_collection_type_error_response);
  }
}

void Proxy::process_client_response_link_chain(Client* c) {
  while (c->head_link && c->head_link->is_ready()) {
    this->send_ready_response(c->head_link);

    // delete the link object. we don't need to mess with BackendConnection
    // chains because the link object is ready - this means it's not linked to
    // any BackendConnections.
    ResponseLink* next_l = c->head_link->next_client;
    c->head_link->next_client = NULL;
    delete c->head_link;
    c->head_link = next_l;
    if (!c->head_link) {
      c->tail_link = NULL;
    }
  }
}

void Proxy::handle_backend_response(BackendConnection* conn,
    shared_ptr<Response> r) {

  // get the current response link
  auto l = conn->head_link;
  if (!l) {
    log(WARNING, "received response from backend with no response link");
    return;
  }

  // advance the head ptr for the backend link queue (thereby unlinking this
  // response link from it)
  // TODO: factor this out
  auto next_link_it = l->backend_conn_to_next_link.find(conn);
  if (next_link_it == l->backend_conn_to_next_link.end()) {
    log(ERROR, "inconsistent backend conn link");
    return;
  }
  conn->head_link = next_link_it->second;
  if (!conn->head_link) {
    conn->tail_link = NULL;
  }
  l->backend_conn_to_next_link.erase(next_link_it);

  // if an error response isn't present, update the link object based on the new
  // response
  if (!l->error_response) {
    switch (l->type) {
      case CollectionType::ForwardResponse:
      case CollectionType::ModifyScanResponse:
        l->response_to_forward = r;
        break;

      case CollectionType::CollectStatusResponses:
        if (r->type != Response::Type::Status) {
          l->error_response = wrong_type_error_response;
        }
        break;

      case CollectionType::SumIntegerResponses:
        if (r->type != Response::Type::Integer) {
          l->error_response = wrong_type_error_response;
        } else {
          l->response_integer_sum += r->int_value;
        }
        break;

      case CollectionType::CombineMultiResponses:
      case CollectionType::CollectResponses:
      case CollectionType::CollectIdenticalResponses:
      case CollectionType::ModifyScriptExistsResponse:
        l->responses.emplace_back(r);
        break;

      case CollectionType::CollectMultiResponsesByKey: {
        if (r->type != Response::Type::Multi) {
          l->error_response = wrong_type_error_response;
          break;
        }

        l->backend_index_to_response.emplace(conn->backend->index, r);
        break;
      }

      default:
        l->error_response = unknown_collection_type_error_response;
    }
  }

  // if this link doesn't have a client (it disconnected before the response was
  // ready), then delete the response if it's ready - it's not linked from any
  // client object and therefore will be leaked if we don't deal with it now
  if (!l->client) {
    if (l->is_ready()) {
      delete l;
    }

  } else {
    // receiving a single response from a backend can cause at most one response
    // to be ready, but multiple responses can be sent to the client if they
    // were ready before this one. (for example, if the client sends reqs A and
    // B which go to different servers, and B responds first, then the B
    // response will be ready but not sent, because it has to wait for A. in
    // this case, we'll send both A and B when A becomes ready.) this call sends
    // all ready responses at the beginning of the chain to the client.
    this->process_client_response_link_chain(l->client);
  }
}

void Proxy::handle_client_command(Client* c, shared_ptr<DataCommand> cmd) {

  if (cmd->args.size() <= 0) {
    this->send_client_string_response(c, "ERR invalid command",
        Response::Type::Error);
    return;
  }

  int x;
  char* arg0_str = const_cast<char*>(cmd->args[0].c_str());
  for (x = 0; x < cmd->args[0].size(); x++) {
    arg0_str[x] = toupper(arg0_str[x]);
  }

  command_handler handler;
  try {
    handler = this->handlers.at(arg0_str);
  } catch (const out_of_range& e) {
    handler = &Proxy::command_default;
  }

  try {
    (this->*handler)(c, cmd);
  } catch (const exception& e) {
    string error_str = string_printf("PROXYERROR handler failed: %s", e.what());
    this->send_client_string_response(c, error_str.data(), error_str.size(),
        Response::Type::Error);
  }

  // need to check for complete responses here because the command handler
  // probably produces a ResponseLink object, and this object can contain an
  // error already (and if so, it's likely to be ready)
  this->process_client_response_link_chain(c);
}



////////////////////////////////////////////////////////////////////////////////
// low-level input handlers

void Proxy::dispatch_on_client_input(struct bufferevent *bev, void* ctx) {
  ((Proxy*)ctx)->on_client_input(bev);
}

void Proxy::on_client_input(struct bufferevent *bev) {
  auto& c = this->bev_to_client.at(bev);
  struct evbuffer* in_buffer = bufferevent_get_input(bev);

  shared_ptr<DataCommand> cmd;
  try {
    while (!c.should_disconnect && (cmd = c.parser.resume(in_buffer))) {
      c.num_commands_received++;
      this->num_commands_received++;
      this->handle_client_command(&c, cmd);
    }

  } catch (const exception& e) {
    log(WARNING, "error in client %s input stream: %s", c.name.c_str(),
        e.what());
    c.should_disconnect = true;
  }

  if (c.should_disconnect) {
    this->disconnect_client(&c);
  }
}


void Proxy::dispatch_on_client_error(struct bufferevent *bev, short events,
    void* ctx) {
  ((Proxy*)ctx)->on_client_error(bev, events);
}

void Proxy::on_client_error(struct bufferevent *bev, short events) {
  auto& c = this->bev_to_client.at(bev);

  if (events & BEV_EVENT_ERROR) {
    int err = EVUTIL_SOCKET_ERROR();
    log(WARNING, "client %s caused error %d (%s) in input stream",
        c.name.c_str(), err, evutil_socket_error_to_string(err));
  }
  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    this->disconnect_client(&c);
  }
}


void Proxy::dispatch_on_backend_input(struct bufferevent *bev, void* ctx) {
  ((Proxy*)ctx)->on_backend_input(bev);
}

void Proxy::on_backend_input(struct bufferevent *bev) {
  BackendConnection* conn = this->bev_to_backend_conn.at(bev);
  struct evbuffer* in_buffer = bufferevent_get_input(bev);

  for (;;) {
    // if the head of the queue is a forwarding client, then use the forwarding
    // parser (don't allocate a response object)
    auto* l = conn->head_link;
    if (!l || (l->type == CollectionType::ForwardResponse)) {
      struct evbuffer* out_buffer = NULL;
      if (l && l->client) {
        out_buffer = l->client->get_output_buffer();
      }

      try {
        if (!conn->parser.forward(in_buffer, out_buffer)) {
          break;
        }
      } catch (const exception& e) {
        log(WARNING, "parse error in backend stream %s (%s)",
            conn->backend->debug_name.c_str(), e.what());
        this->disconnect_backend(conn); // same as getting a BEV_EVENT_EOF
        break;
      }

      conn->num_responses_received++;
      conn->backend->num_responses_received++;
      this->num_responses_received++;
      l->client->num_responses_sent++;
      this->num_responses_sent++;

      // a full response was forwarded; delete the wait object
      // TODO: factor this out
      auto next_link_it = l->backend_conn_to_next_link.find(conn);
      if (next_link_it == l->backend_conn_to_next_link.end()) {
        log(ERROR, "inconsistent backend conn link");
        return;
      }
      conn->head_link = next_link_it->second;
      if (!conn->head_link) {
        conn->tail_link = NULL;
      }
      l->backend_conn_to_next_link.erase(next_link_it);

      l->client->head_link = l->next_client;
      if (!l->client->head_link) {
        l->client->tail_link = NULL;
      }

      assert(l->is_ready());
      delete l;

    } else {
      shared_ptr<Response> rsp;
      try {
        rsp = conn->parser.resume(in_buffer);
      } catch (const exception& e) {
        log(WARNING, "parse error in backend stream %s (%d)",
            conn->backend->debug_name.c_str(), e.what());
        this->disconnect_backend(conn); // same as getting a BEV_EVENT_EOF
        break;
      }
      if (!rsp.get()) {
        break;
      }

      conn->num_responses_received++;
      conn->backend->num_responses_received++;
      this->num_responses_received++;
      this->handle_backend_response(conn, rsp);
    }
  }
}


void Proxy::dispatch_on_backend_error(struct bufferevent *bev, short events,
    void* ctx) {
  ((Proxy*)ctx)->on_backend_error(bev, events);
}

void Proxy::on_backend_error(struct bufferevent *bev, short events) {
  BackendConnection* conn = this->bev_to_backend_conn.at(bev);

  if (events & BEV_EVENT_ERROR) {
    int err = EVUTIL_SOCKET_ERROR();
    log(WARNING, "backend %s gave %d (%s)", conn->backend->debug_name.c_str(), err,
        evutil_socket_error_to_string(err));
  }
  if (events & BEV_EVENT_EOF) {
    log(WARNING, "backend %s has disconnected",
        conn->backend->debug_name.c_str());
  }
  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    this->disconnect_backend(conn);
  }
}


void Proxy::dispatch_on_listen_error(struct evconnlistener *listener,
    void* ctx) {
  ((Proxy*)ctx)->on_listen_error(listener);
}

void Proxy::on_listen_error(struct evconnlistener *listener) {
  int err = EVUTIL_SOCKET_ERROR();
  log(WARNING, "error %d (%s) on listening socket", err,
      evutil_socket_error_to_string(err));

  event_base_loopexit(this->base.get(), NULL);
}


void Proxy::dispatch_on_client_accept(struct evconnlistener *listener,
    evutil_socket_t fd, struct sockaddr *address, int socklen, void* ctx) {
  ((Proxy*)ctx)->on_client_accept(listener, fd, address, socklen);
}

void Proxy::on_client_accept(struct evconnlistener *listener,
    evutil_socket_t fd, struct sockaddr *address, int socklen) {

  int fd_flags = fcntl(fd, F_GETFD, 0);
  if (fd_flags >= 0) {
    fd_flags |= FD_CLOEXEC;
    fcntl(fd, F_SETFD, fd_flags);
  }

  // set up a bufferevent for the new connection
  struct bufferevent* raw_bev = bufferevent_socket_new(this->base.get(), fd,
      BEV_OPT_CLOSE_ON_FREE);
  unique_ptr<struct bufferevent, void(*)(struct bufferevent*)> bev(
      raw_bev, bufferevent_free);

  // create a Client for this connection
  this->bev_to_client.emplace(piecewise_construct,
      forward_as_tuple(raw_bev), forward_as_tuple(move(bev)));
  this->num_connections_received++;

  // set read/error callbacks and enable i/o
  bufferevent_setcb(raw_bev, Proxy::dispatch_on_client_input, NULL,
      Proxy::dispatch_on_client_error, this);
  bufferevent_enable(raw_bev, EV_READ | EV_WRITE);
}



////////////////////////////////////////////////////////////////////////////////
// generic command implementations

void Proxy::command_all_collect_responses(Client* c,
    shared_ptr<DataCommand> cmd) {
  this->command_forward_all(c, cmd, CollectionType::CollectResponses);
}

void Proxy::command_all_collect_status_responses(Client* c,
    shared_ptr<DataCommand> cmd) {
  this->command_forward_all(c, cmd, CollectionType::CollectStatusResponses);
}

void Proxy::command_forward_all(Client* c, shared_ptr<DataCommand> cmd,
    CollectionType type) {
  auto l = this->create_link(type, c);
  for (auto& backend_it : this->index_to_backend) {
    BackendConnection& conn = this->backend_conn_for_index(backend_it.first);
    this->send_command_and_link(&conn, l, cmd);
  }
}

void Proxy::command_forward_by_key_1(Client* c, shared_ptr<DataCommand> cmd) {
  this->command_forward_by_key_index(c, cmd, 1);
}

void Proxy::command_forward_by_key_index(Client* c, shared_ptr<DataCommand> cmd,
    size_t key_index) {

  if (key_index >= cmd->args.size()) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  BackendConnection& conn = this->backend_conn_for_key(cmd->args[key_index]);
  ResponseLink* l = this->create_link(CollectionType::ForwardResponse, c);
  this->send_command_and_link(&conn, l, cmd);
}

void Proxy::command_forward_by_keys(Client* c, shared_ptr<DataCommand> cmd,
    ssize_t start_key_index, ssize_t end_key_index) {

  if (cmd->args.size() <= start_key_index) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if ((end_key_index < 0) || (end_key_index > cmd->args.size())) {
    end_key_index = cmd->args.size();
  }

  // check that the keys all hash to the same server
  int64_t backend_index = this->backend_index_for_key(cmd->args[start_key_index]);
  int x;
  for (x = start_key_index + 1; x < end_key_index; x++) {
    if (this->backend_index_for_key(cmd->args[x]) != backend_index) {
      this->send_client_string_response(c,
          "PROXYERROR keys are on different backends", Response::Type::Error);
      return;
    }
  }

  BackendConnection& conn = this->backend_conn_for_index(backend_index);
  ResponseLink* l = this->create_link(CollectionType::ForwardResponse, c);
  this->send_command_and_link(&conn, l, cmd);
}

void Proxy::command_forward_by_keys_1_all(Client* c,
    shared_ptr<DataCommand> cmd) {
  this->command_forward_by_keys(c, cmd, 1, -1);
}

void Proxy::command_forward_by_keys_1_2(Client* c,
    shared_ptr<DataCommand> cmd) {
  this->command_forward_by_keys(c, cmd, 1, 3);
}

void Proxy::command_forward_by_keys_2_all(Client* c,
    shared_ptr<DataCommand> cmd) {
  this->command_forward_by_keys(c, cmd, 2, -1);
}

void Proxy::command_forward_random(Client* c, shared_ptr<DataCommand> cmd) {
  BackendConnection& conn = this->backend_conn_for_index(
      rand() % this->index_to_backend.size());
  ResponseLink* l = this->create_link(CollectionType::ForwardResponse, c);
  this->send_command_and_link(&conn, l, cmd);
}

void Proxy::command_partition_by_keys(Client* c, shared_ptr<DataCommand> cmd,
    size_t args_per_key, CollectionType type) {

  if (((cmd->args.size() - 1) % args_per_key) != 0) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }
  size_t num_keys = (cmd->args.size() - 1) / args_per_key;

  // set up the ResponseLink
  auto l = this->create_link(type, c);

  // compute the backend for each key, creating command objects as needed
  unordered_map<int64_t, ReferenceCommand> backend_index_to_command;
  for (size_t y = 0; y < num_keys; y++) {
    size_t base_arg_index = (y * args_per_key) + 1;
    int64_t backend_index = this->backend_index_for_key(
        cmd->args[base_arg_index]);

    // the recombination queue tells us in which order we should pull keys from
    // the returned responses when sending the ready response to the client
    if (l->type == CollectionType::CollectMultiResponsesByKey) {
      l->recombination_queue.emplace_back(backend_index);
    }

    // copy the keys into the commands, creating them if needed
    auto& backend_cmd = backend_index_to_command[backend_index];
    if (backend_cmd.args.empty()) {
      const string& arg = cmd->args[0];
      backend_cmd.args.emplace_back(arg.data(), arg.size());
    }
    for (size_t z = 0; z < args_per_key; z++) {
      const string& arg = cmd->args[base_arg_index + z];
      backend_cmd.args.emplace_back(arg.data(), arg.size());
    }
  }

  // send the commands off
  for (const auto& it : backend_index_to_command) {
    BackendConnection& conn = this->backend_conn_for_index(it.first);
    this->send_command_and_link(&conn, l, &it.second);
  }
}

void Proxy::command_partition_by_keys_1_integer(Client* c,
    shared_ptr<DataCommand> cmd) {
  this->command_partition_by_keys(c, cmd, 1,
      CollectionType::SumIntegerResponses);
}

void Proxy::command_partition_by_keys_1_multi(Client* c,
    shared_ptr<DataCommand> cmd) {
  this->command_partition_by_keys(c, cmd, 1,
      CollectionType::CollectMultiResponsesByKey);
}

void Proxy::command_partition_by_keys_2_status(Client* c,
    shared_ptr<DataCommand> cmd) {
  this->command_partition_by_keys(c, cmd, 2,
      CollectionType::CollectStatusResponses);
}

void Proxy::command_unimplemented(Client* c, shared_ptr<DataCommand> cmd) {
  this->send_client_string_response(c, "PROXYERROR command not supported",
      Response::Type::Error);
}

void Proxy::command_default(Client* c, shared_ptr<DataCommand> cmd) {
  string formatted_cmd = cmd->format();
  log(INFO, "unknown command from %s: %s", c->name.c_str(), formatted_cmd.c_str());

  this->send_client_string_response(c, "PROXYERROR unknown command",
      Response::Type::Error);
}



////////////////////////////////////////////////////////////////////////////////
// specific command implementations

void Proxy::command_BACKEND(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() != 2) {
    this->send_client_string_response(c, "ERR wrong number of arguments",
        Response::Type::Error);
    return;
  }

  const Backend& b = this->backend_for_key(cmd->args[1]);
  this->send_client_string_response(c, b.name, Response::Type::Data);
}

void Proxy::command_BACKENDNUM(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() != 2) {
    this->send_client_string_response(c, "ERR wrong number of arguments",
        Response::Type::Error);
    return;
  }

  int64_t backend_index = this->backend_index_for_key(cmd->args[1]);
  this->send_client_int_response(c, backend_index, Response::Type::Integer);
}

void Proxy::command_BACKENDS(Client* c, shared_ptr<DataCommand> cmd) {
  Response r(Response::Type::Multi, this->index_to_backend.size());
  for (const auto& backend_it : this->index_to_backend) {
    r.fields.emplace_back(new Response(Response::Type::Data,
        backend_it.second.debug_name));
  }
  this->send_client_response(c, &r);
}

void Proxy::command_CLIENT(Client* c, shared_ptr<DataCommand> cmd) {

  if (cmd->args.size() < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if (cmd->args[1] == "LIST") {
    string response_data;
    for (const auto& bev_client : this->bev_to_client) {
      auto& c = bev_client.second;

      size_t response_chain_length = 0;
      for (auto* l = c.head_link; l; l = l->next_client) {
        response_chain_length++;
      }

      response_data += string_printf(
          "name=%s cmdrecv=%d rspsent=%d rspchain=%d\n",
          c.name.c_str(), c.num_commands_received, c.num_responses_sent,
          response_chain_length);
    }

    this->send_client_string_response(c, response_data, Response::Type::Data);

  } else if (cmd->args[1] == "GETNAME") {
    this->send_client_string_response(c, c->name, Response::Type::Data);

  } else {
    this->send_client_string_response(c, "ERR unsupported subcommand",
        Response::Type::Error);
  }
}

void Proxy::command_DBSIZE(Client* c, shared_ptr<DataCommand> cmd) {
  this->command_forward_all(c, cmd, CollectionType::SumIntegerResponses);
}

void Proxy::command_DEBUG(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);

  } else if (cmd->args[1] == "OBJECT") {
    this->command_forward_by_key_index(c, cmd, 2);

  } else {
    this->send_client_string_response(c, "PROXYERROR unsupported subcommand",
        Response::Type::Error);
  }
}

void Proxy::command_ECHO(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() != 2) {
    this->send_client_string_response(c, "ERR wrong number of arguments",
        Response::Type::Error);
    return;
  }

  this->send_client_string_response(c, cmd->args[1], Response::Type::Data);
}

void Proxy::command_EVAL(Client* c, shared_ptr<DataCommand> cmd) {

  if (cmd->args.size() < 3) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  char* endptr;
  int64_t num_keys = strtoll(cmd->args[2].data(), &endptr, 0);
  if ((endptr == cmd->args[2].data()) ||
      (num_keys < 0 || num_keys > cmd->args.size() - 3)) {
    this->send_client_string_response(c, "ERR key count is invalid",
        Response::Type::Error);
    return;
  }

  // check that the keys all hash to the same server
  int64_t backend_index = -1;
  for (size_t x = 3; x < num_keys + 3; x++) {
    int64_t this_key_backend_index = this->backend_index_for_key(cmd->args[x]);

    if (backend_index == -1) {
      backend_index = this_key_backend_index;

    } else if (backend_index != this_key_backend_index) {
      this->send_client_string_response(c,
          "PROXYERROR keys are on different backends", Response::Type::Error);
      return;
    }
  }

  if (backend_index == -1) {
    backend_index = rand() % this->index_to_backend.size();
  }

  BackendConnection& conn = this->backend_conn_for_index(backend_index);
  ResponseLink* l = this->create_link(CollectionType::ForwardResponse, c);
  this->send_command_and_link(&conn, l, cmd);
}

void Proxy::command_FORWARD(Client* c, shared_ptr<DataCommand> cmd) {

  if (cmd->args.size() < 3) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  int64_t backend_index = this->backend_index_for_argument(cmd->args[1]);
  if (backend_index < 0) {
    this->send_client_string_response(c, "ERR backend does not exist",
        Response::Type::Error);
    return;
  }

  BackendConnection& conn = this->backend_conn_for_index(backend_index);

  // send everything after the backend name/index to the backend
  ReferenceCommand backend_cmd(cmd->args.size() - 2);
  for (size_t x = 2; x < cmd->args.size(); x++) {
    const string& arg = cmd->args[x];
    backend_cmd.args.emplace_back(arg.data(), arg.size());
  }

  ResponseLink* l = this->create_link(CollectionType::ForwardResponse, c);
  this->send_command_and_link(&conn, l, &backend_cmd);
}

void Proxy::command_GEORADIUS(Client* c, shared_ptr<DataCommand> cmd) {
  // GEORADIUS[BYMEMBER] key long lat rad unit ...

  if (cmd->args.size() < 6) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  // check that all keys hash to the same server
  int64_t backend_index = this->backend_index_for_key(cmd->args[1]);

  size_t arg_index = 6;
  while (arg_index < cmd->args.size()) {
    const string& arg = cmd->args[arg_index];
    if (starts_with(arg, "WITH") || (arg == "ASC") || (arg == "DESC")) {
      arg_index++;
    } else if (arg == "COUNT") {
      arg_index += 2;
    } else if ((arg == "STORE") || (arg == "STOREDIST")) {
      if (arg_index == cmd->args.size() - 1) {
        this->send_client_string_response(c, "ERR store clause missing argument",
            Response::Type::Error);
        return;
      }
      if (this->backend_index_for_key(cmd->args[arg_index + 1]) != backend_index) {
        this->send_client_string_response(c,
            "PROXYERROR keys are on different backends", Response::Type::Error);
        return;
      }
    }
  }

  // send it off
  BackendConnection& conn = this->backend_conn_for_index(backend_index);
  ResponseLink* l = this->create_link(CollectionType::ForwardResponse, c);
  this->send_command_and_link(&conn, l, cmd);
}

void Proxy::command_INFO(Client* c, shared_ptr<DataCommand> cmd) {

  // INFO - return proxy info
  if (cmd->args.size() == 1) {
    char hash_begin_delimiter_str[5], hash_end_delimiter_str[5];
    if (this->hash_begin_delimiter) {
      sprintf(hash_begin_delimiter_str, "%c", this->hash_begin_delimiter);
    } else {
      sprintf(hash_begin_delimiter_str, "NULL");
    }
    if (this->hash_end_delimiter) {
      sprintf(hash_end_delimiter_str, "%c", this->hash_end_delimiter);
    } else {
      sprintf(hash_end_delimiter_str, "NULL");
    }

    uint64_t uptime = now() - this->start_time;

    Response r(Response::Type::Data, "\
# Counters\n\
num_commands_received:%zu\n\
num_commands_sent:%zu\n\
num_responses_received:%zu\n\
num_responses_sent:%zu\n\
num_connections_received:%zu\n\
num_clients:%zu\n\
num_backends:%zu\n\
\n\
# Timing\n\
start_time_usecs:%" PRIu64 "\n\
uptime_usecs:%" PRIu64 "\n\
\n\
# Configuration\n\
process_id:%d\n\
hash_begin_delimiter:%s\n\
hash_end_delimiter:%s\n\
", this->num_commands_received, this->num_commands_sent,
        this->num_responses_received, this->num_responses_sent,
        this->num_connections_received, this->bev_to_client.size(),
        this->index_to_backend.size(), this->start_time, uptime,
        getpid_cached(), hash_begin_delimiter_str, hash_end_delimiter_str);
    this->send_client_response(c, &r);
    return;
  }

  // INFO BACKEND num - return proxy's info for backend num
  if ((cmd->args.size() == 3) && (cmd->args[1] == "BACKEND")) {
    int64_t backend_index = this->backend_index_for_argument(cmd->args[1]);
    if (backend_index < 0) {
      this->send_client_string_response(c, "ERR backend does not exist",
          Response::Type::Error);
      return;
    }

    Backend& b = this->backend_for_index(backend_index);
    Response r(Response::Type::Data, "\
name:%s\n\
debug_name:%s\n\
host:%s\n\
port:%d\n\
num_commands_sent:%d\n\
num_responses_received:%d\n\
", b.name.c_str(), b.debug_name.c_str(), b.host.c_str(), b.port,
        b.num_commands_sent, b.num_responses_received);
    for (auto& conn_it : b.index_to_connection) {
      auto& conn = conn_it.second;

      size_t response_chain_length = 0;
      for (ResponseLink* l = conn.head_link; l; l = l->backend_conn_to_next_link.at(&conn)) {
        response_chain_length++;
      }

      r.data += string_printf("connection_%" PRId64 ":commands_sent=%zu,responses_received=%zu,chain_length=%zu\n",
          conn.index, conn.num_commands_sent, conn.num_responses_received,
          response_chain_length);
    }

    this->send_client_response(c, &r);
    return;
  }

  // INFO num - return info from backend server
  // INFO num section - return info from backend server

  int64_t backend_index = this->backend_index_for_argument(cmd->args[1]);
  if (backend_index < 0) {
    this->send_client_string_response(c, "ERR backend does not exist",
        Response::Type::Error);
    return;
  }

  // remove the backend name/number from the command
  ReferenceCommand backend_cmd(cmd->args.size() - 1);
  backend_cmd.args.emplace_back(cmd->args[0].data(), cmd->args[0].size());
  for (size_t x = 2; x < cmd->args.size(); x++) {
    backend_cmd.args.emplace_back(cmd->args[x].data(), cmd->args[x].size());
  }

  BackendConnection& conn = this->backend_conn_for_index(backend_index);
  ResponseLink* l = this->create_link(CollectionType::ForwardResponse, c);
  this->send_command_and_link(&conn, l, &backend_cmd);
}

void Proxy::command_KEYS(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() != 2) {
    this->send_client_string_response(c, "ERR incorrect argument count",
        Response::Type::Error);
  } else {
    this->command_forward_all(c, cmd, CollectionType::CombineMultiResponses);
  }
}

void Proxy::command_MIGRATE(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() < 6) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
  } else {
    this->command_forward_by_key_index(c, cmd, 3);
  }
}

void Proxy::command_MSETNX(Client* c, shared_ptr<DataCommand> cmd) {

  if (cmd->args.size() < 3) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if ((cmd->args.size() & 1) != 1) {
    this->send_client_string_response(c, "ERR incorrect argument count",
        Response::Type::Error);
    return;
  }

  // check that the keys all hash to the same server
  int64_t backend_index = this->backend_index_for_key(cmd->args[1]);
  int x;
  for (x = 3; x < cmd->args.size(); x += 2) {
    if (this->backend_index_for_key(cmd->args[x]) != backend_index) {
      this->send_client_string_response(c,
          "PROXYERROR keys are on different backends", Response::Type::Error);
      return;
    }
  }

  BackendConnection& conn = this->backend_conn_for_index(backend_index);
  ResponseLink* l = this->create_link(CollectionType::ForwardResponse, c);
  this->send_command_and_link(&conn, l, cmd);
}

void Proxy::command_OBJECT(Client* c, shared_ptr<DataCommand> cmd) {

  if (cmd->args.size() < 3) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
  } else if ((cmd->args[1] == "REFCOUNT") &&
             (cmd->args[1] == "ENCODING") &&
             (cmd->args[1] == "IDLETIME")) {
    this->send_client_string_response(c, "PROXYERROR unsupported subcommand",
        Response::Type::Error);
  } else {
    this->command_forward_by_key_index(c, cmd, 2);
  }
}

void Proxy::command_PING(Client* c, shared_ptr<DataCommand> cmd) {
  this->send_client_string_response(c, "PONG", Response::Type::Status);
}

void Proxy::command_PRINTSTATE(Client* c, shared_ptr<DataCommand> cmd) {
  log(INFO, "state readout requested by client %s", c->name.c_str());
  this->print(stderr);
  fputc('\n', stderr);
  this->send_client_string_response(c, "OK", Response::Type::Status);
}

void Proxy::command_QUIT(Client* c, shared_ptr<DataCommand> cmd) {
  c->should_disconnect = 1;
}

void Proxy::command_ROLE(Client* c, shared_ptr<DataCommand> cmd) {
  static shared_ptr<Response> role_response(new Response(
      Response::Type::Data, "proxy"));

  Response r(Response::Type::Multi, 2);
  r.fields.emplace_back(role_response);
  r.fields.emplace_back(new Response(Response::Type::Multi,
      this->index_to_backend.size()));

  auto& backends_r = *r.fields[1];
  for (const auto& backend_it : this->index_to_backend) {
    backends_r.fields.emplace_back(new Response(Response::Type::Data,
        backend_it.second.debug_name));
  }

  this->send_client_response(c, &r);
}

void Proxy::command_SCAN(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  // if cursor is "0" then we're starting a new scan on the first backend
  if (cmd->args[1] == "0") {
    BackendConnection& conn = this->backend_conn_for_index(0);
    auto l = this->create_link(CollectionType::ModifyScanResponse, c);
    l->scan_backend_index = 0;
    this->send_command_and_link(&conn, l, cmd);
    return;
  }

  // cursor is formatted as "backend_bev:cursor"
  size_t colon_offset = cmd->args[1].find(':');
  if (colon_offset == string::npos) {
    this->send_client_string_response(c,
        "PROXYERROR cursor format is incorrect", Response::Type::Error);
    return;
  }

  // parse backend_num and cursor_contents
  char* endptr;
  int64_t backend_index = strtoll(cmd->args[1].data(), &endptr, 0);
  if ((endptr == cmd->args[1].data()) ||
      (backend_index < 0 || backend_index >= this->index_to_backend.size())) {
    this->send_client_string_response(c,
        "ERR cursor refers to a nonexistent backend", Response::Type::Error);
    return;
  }

  // create backend command. note that we can't easily use ReferenceCommand
  // because we're reconstructing one of the arguments; it's probably not worth
  // it anyway because SCAN usually only takes a few short arguments
  shared_ptr<DataCommand> backend_cmd(new DataCommand());
  backend_cmd->args = cmd->args;
  backend_cmd->args[1] = cmd->args[1].substr(colon_offset + 1);

  // send command
  BackendConnection& conn = this->backend_conn_for_index(backend_index);
  auto l = this->create_link(CollectionType::ModifyScanResponse, c);
  l->scan_backend_index = backend_index;
  this->send_command_and_link(&conn, l, backend_cmd);
}

void Proxy::command_SCRIPT(Client* c, shared_ptr<DataCommand> cmd) {
  // subcommands:
  // EXISTS - forward to all backends, aggregate responses
  // FLUSH - forward to all backends, aggregate responses
  // KILL - not supported
  // LOAD <script> - forward to all backends, aggregate responses

  if (cmd->args.size() < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if (cmd->args[1] == "FLUSH") {
    this->command_forward_all(c, cmd, CollectionType::CollectStatusResponses);
  } else if (cmd->args[1] == "LOAD") {
    this->command_forward_all(c, cmd, CollectionType::CollectIdenticalResponses);
  } else if (cmd->args[1] == "EXISTS") {
    this->command_forward_all(c, cmd, CollectionType::ModifyScriptExistsResponse);
  } else {
    this->send_client_string_response(c, "PROXYERROR unsupported subcommand",
        Response::Type::Error);
    return;
  }
}

void Proxy::command_ZACTIONSTORE(Client* c, shared_ptr<DataCommand> cmd) {
  // this is basically the same as command_forward_by_keys except the number of
  // checked keys is given in arg 2

  if (cmd->args.size() <= 3) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  char* endptr;
  int64_t num_keys = strtoll(cmd->args[2].data(), &endptr, 0);
  if ((endptr == cmd->args[2].data()) ||
      (num_keys < 1 || num_keys > cmd->args.size() - 3)) {
    this->send_client_string_response(c, "ERR key count is invalid",
        Response::Type::Error);
    return;
  }

  // check that the keys all hash to the same server
  int backend_index = this->backend_index_for_key(cmd->args[1]);
  for (size_t x = 0; x < num_keys; x++) {
    if (this->backend_index_for_key(cmd->args[3 + x]) != backend_index) {
      this->send_client_string_response(c,
          "PROXYERROR keys are on different backends", Response::Type::Error);
      return;
    }
  }

  BackendConnection& conn = this->backend_conn_for_index(backend_index);
  auto l = this->create_link(CollectionType::ForwardResponse, c);
  this->send_command_and_link(&conn, l, cmd);
}



const unordered_map<string, Proxy::command_handler> Proxy::default_handlers({
  {"AUTH",              &Proxy::command_unimplemented}, // password - Authenticate to the server
  {"BLPOP",             &Proxy::command_unimplemented}, // key [key ...] timeout - Remove and get the first element in a list, or block until one is available
  {"BRPOP",             &Proxy::command_unimplemented}, // key [key ...] timeout - Remove and get the last element in a list, or block until one is available
  {"BRPOPLPUSH",        &Proxy::command_unimplemented}, // source destination timeout - Pop a value from a list, push it to another list and return it; or block until one is available
  {"CLUSTER",           &Proxy::command_unimplemented}, // ADDSLOTS \ COUNT-FAILURE-REPORTS \ COUNTKEYSINSLOT \ DELSLOTS \ FAILOVER \ FORGET \ GETKEYSINSLOT \ INFO \ KEYSLOT \ MEET \ NODES \ REPLICATE \ RESET \ SAVECONFIG \ SET-CONFIG-EPOCH \ SETSLOT \ SLAVES \ SLOTS
  {"DISCARD",           &Proxy::command_unimplemented}, // - Discard all commands issued after MULTI
  {"EXEC",              &Proxy::command_unimplemented}, // - Execute all commands issued after MULTI
  {"MONITOR",           &Proxy::command_unimplemented}, // - Listen for all requests received by the server in real time
  {"MOVE",              &Proxy::command_unimplemented}, // key db - Move a key to another database
  {"MULTI",             &Proxy::command_unimplemented}, // - Mark the start of a transaction block
  {"PSUBSCRIBE",        &Proxy::command_unimplemented}, // pattern [pattern ...] - Listen for messages published to channels matching the given patterns
  {"PUBLISH",           &Proxy::command_unimplemented}, // channel message - Post a message to a channel
  {"PUBSUB",            &Proxy::command_unimplemented}, // subcommand [argument [argument ...]] - Inspect the state of the Pub/Sub subsystem
  {"PUNSUBSCRIBE",      &Proxy::command_unimplemented}, // [pattern [pattern ...]] - Stop listening for messages posted to channels matching the given patterns
  {"READONLY",          &Proxy::command_unimplemented}, // - allows reads from cluster slave nodes
  {"READWRITE",         &Proxy::command_unimplemented}, // - inverse of READONLY
  {"SELECT",            &Proxy::command_unimplemented}, // index - Change the selected database for the current connection
  {"SLAVEOF",           &Proxy::command_unimplemented}, // host port - Make the server a slave of another instance, or promote it as master
  {"SUBSCRIBE",         &Proxy::command_unimplemented}, // channel [channel ...] - Listen for messages published to the given channels
  {"SWAPDB",            &Proxy::command_unimplemented}, // index index - exchange the contents of two Redis databases
  {"SYNC",              &Proxy::command_unimplemented}, // - Internal command used for replication
  {"UNSUBSCRIBE",       &Proxy::command_unimplemented}, // [channel [channel ...]] - Stop listening for messages posted to the given channels
  {"UNWATCH",           &Proxy::command_unimplemented}, // - Forget about all watched keys
  {"WAIT",              &Proxy::command_unimplemented}, // numslaves timeout - Wait for synchronous replication
  {"WATCH",             &Proxy::command_unimplemented}, // key [key ...] - Watch the given keys to determine execution of the MULTI/EXEC block

  {"APPEND",            &Proxy::command_forward_by_key_1},             // key value - Append a value to a key
  {"BGREWRITEAOF",      &Proxy::command_all_collect_status_responses}, // - Asynchronously rewrite the append-only file
  {"BGSAVE",            &Proxy::command_all_collect_status_responses}, // - Asynchronously save db to disk
  {"BITCOUNT",          &Proxy::command_forward_by_key_1},             // key [start] [end] - Count set bits in a string
  {"BITOP",             &Proxy::command_forward_by_keys_2_all},        // operation destkey key [key ...] - Perform bitwise operations between strings
  {"BITPOS",            &Proxy::command_forward_by_key_1},             // key bit [start] [end] - Return the position of the first bit set to 1 or 0 in a string
  {"CLIENT",            &Proxy::command_CLIENT},                       // KILL ip:port / LIST / GETNAME / SETNAME name / PAUSE / REPLY
  {"COMMAND",           &Proxy::command_forward_random},               // [COUNT | GETKEYS | INFO] - Get information about Redis commands
  {"CONFIG",            &Proxy::command_all_collect_responses},        // GET parameter / REWRITE / SET param value / RESETSTAT
  {"DBSIZE",            &Proxy::command_DBSIZE},                       // - Return the number of keys in the selected database
  {"DEBUG",             &Proxy::command_DEBUG},                        // OBJECT key / SEGFAULT
  {"DECR",              &Proxy::command_forward_by_key_1},             // key - Decrement the integer value of a key by one
  {"DECRBY",            &Proxy::command_forward_by_key_1},             // key decrement - Decrement the integer value of a key by the given number
  {"DEL",               &Proxy::command_partition_by_keys_1_integer},  // key [key ...] - Delete a key
  {"DUMP",              &Proxy::command_forward_by_key_1},             // key - Return a serialized version of the value stored at the specified key.
  {"ECHO",              &Proxy::command_ECHO},                         // message - Echo the given string
  {"EVAL",              &Proxy::command_EVAL},                         // script numkeys key [key ...] arg [arg ...] - Execute a Lua script server side
  {"EVALSHA",           &Proxy::command_EVAL},                         // sha1 numkeys key [key ...] arg [arg ...] - Execute a Lua script server side
  {"EXISTS",            &Proxy::command_forward_by_key_1},             // key - Determine if a key exists
  {"EXPIRE",            &Proxy::command_forward_by_key_1},             // key seconds - Set a keys time to live in seconds
  {"EXPIREAT",          &Proxy::command_forward_by_key_1},             // key timestamp - Set the expiration for a key as a UNIX timestamp
  {"FLUSHALL",          &Proxy::command_all_collect_status_responses}, // - Remove all keys from all databases
  {"FLUSHDB",           &Proxy::command_all_collect_status_responses}, // - Remove all keys from the current database
  {"GEOADD",            &Proxy::command_forward_by_key_1},             // key longitude latitude member ...
  {"GEOHASH",           &Proxy::command_forward_by_key_1},             // key member ...
  {"GEOPOS",            &Proxy::command_forward_by_key_1},             // key member ...
  {"GEODIST",           &Proxy::command_forward_by_key_1},             // key member1 member2 [unit]
  {"GEORADIUS",         &Proxy::command_GEORADIUS},                    // key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]
  {"GEORADIUSBYMEMBER", &Proxy::command_GEORADIUS},                    // key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]
  {"GET",               &Proxy::command_forward_by_key_1},             // key - Get the value of a key
  {"GETBIT",            &Proxy::command_forward_by_key_1},             // key offset - Returns the bit value at offset in the string value stored at key
  {"GETRANGE",          &Proxy::command_forward_by_key_1},             // key start end - Get a substring of the string stored at a key
  {"GETSET",            &Proxy::command_forward_by_key_1},             // key value - Set the string value of a key and return its old value
  {"HDEL",              &Proxy::command_forward_by_key_1},             // key field [field ...] - Delete one or more hash fields
  {"HEXISTS",           &Proxy::command_forward_by_key_1},             // key field - Determine if a hash field exists
  {"HGET",              &Proxy::command_forward_by_key_1},             // key field - Get the value of a hash field
  {"HGETALL",           &Proxy::command_forward_by_key_1},             // key - Get all the fields and values in a hash
  {"HINCRBY",           &Proxy::command_forward_by_key_1},             // key field increment - Increment the integer value of a hash field by the given number
  {"HINCRBYFLOAT",      &Proxy::command_forward_by_key_1},             // key field increment - Increment the float value of a hash field by the given amount
  {"HKEYS",             &Proxy::command_forward_by_key_1},             // key - Get all the fields in a hash
  {"HLEN",              &Proxy::command_forward_by_key_1},             // key - Get the number of fields in a hash
  {"HMGET",             &Proxy::command_forward_by_key_1},             // key field [field ...] - Get the values of all the given hash fields
  {"HMSET",             &Proxy::command_forward_by_key_1},             // key field value [field value ...] - Set multiple hash fields to multiple values
  {"HSCAN",             &Proxy::command_forward_by_key_1},             // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate hash fields and associated values
  {"HSET",              &Proxy::command_forward_by_key_1},             // key field value - Set the string value of a hash field
  {"HSETNX",            &Proxy::command_forward_by_key_1},             // key field value - Set the value of a hash field, only if the field does not exist
  {"HSTRLEN",           &Proxy::command_forward_by_key_1},             // key field - Get the length of a hash field's value
  {"HVALS",             &Proxy::command_forward_by_key_1},             // key - Get all the values in a hash
  {"INCR",              &Proxy::command_forward_by_key_1},             // key - Increment the integer value of a key by one
  {"INCRBY",            &Proxy::command_forward_by_key_1},             // key increment - Increment the integer value of a key by the given amount
  {"INCRBYFLOAT",       &Proxy::command_forward_by_key_1},             // key increment - Increment the float value of a key by the given amount
  {"INFO",              &Proxy::command_INFO},                         // [backendnum] [section] - Get information and statistics about the server
  {"KEYS",              &Proxy::command_KEYS},                         // pattern - Find all keys matching the given pattern
  {"LASTSAVE",          &Proxy::command_all_collect_responses},        // - Get the UNIX time stamp of the last successful save to disk  
  {"LINDEX",            &Proxy::command_forward_by_key_1},             // key index - Get an element from a list by its index
  {"LINSERT",           &Proxy::command_forward_by_key_1},             // key BEFORE|AFTER pivot value - Insert an element before or after another element in a list
  {"LLEN",              &Proxy::command_forward_by_key_1},             // key - Get the length of a list
  {"LPOP",              &Proxy::command_forward_by_key_1},             // key - Remove and get the first element in a list
  {"LPUSH",             &Proxy::command_forward_by_key_1},             // key value [value ...] - Prepend one or multiple values to a list
  {"LPUSHX",            &Proxy::command_forward_by_key_1},             // key value - Prepend a value to a list, only if the list exists
  {"LRANGE",            &Proxy::command_forward_by_key_1},             // key start stop - Get a range of elements from a list
  {"LREM",              &Proxy::command_forward_by_key_1},             // key count value - Remove elements from a list
  {"LSET",              &Proxy::command_forward_by_key_1},             // key index value - Set the value of an element in a list by its index
  {"LTRIM",             &Proxy::command_forward_by_key_1},             // key start stop - Trim a list to the specified range
  {"MGET",              &Proxy::command_partition_by_keys_1_multi},    // key [key ...] - Get the values of all the given keys
  {"MIGRATE",           &Proxy::command_MIGRATE},                      // host port key destination-db timeout [COPY] [REPLACE] - Atomically transfer a key from a Redis instance to another one.
  {"MSET",              &Proxy::command_partition_by_keys_2_status},   // key value [key value ...] - Set multiple keys to multiple values
  {"MSETNX",            &Proxy::command_MSETNX},                       // key value [key value ...] - Set multiple keys to multiple values, only if none of the keys exist
  {"OBJECT",            &Proxy::command_OBJECT},                       // subcommand [arguments [arguments ...]] - Inspect the internals of Redis objects
  {"PERSIST",           &Proxy::command_forward_by_key_1},             // key - Remove the expiration from a key
  {"PEXPIRE",           &Proxy::command_forward_by_key_1},             // key milliseconds - Set a keys time to live in milliseconds
  {"PEXPIREAT",         &Proxy::command_forward_by_key_1},             // key milliseconds-timestamp - Set the expiration for a key as a UNIX timestamp specified in milliseconds
  {"PFADD",             &Proxy::command_forward_by_key_1},             // key element [element ...] - Add all elements to HyperLogLog
  {"PFCOUNT",           &Proxy::command_forward_by_keys_1_all},        // key [key ...] - Count unique elements in HyperLogLogs
  {"PFMERGE",           &Proxy::command_forward_by_keys_1_all},        // key [key ...] - Merge HyperLogLogs into one
  {"PING",              &Proxy::command_PING},                         // - Ping the server
  {"PSETEX",            &Proxy::command_forward_by_key_1},             // key milliseconds value - Set the value and expiration in milliseconds of a key
  {"PTTL",              &Proxy::command_forward_by_key_1},             // key - Get the time to live for a key in milliseconds
  {"QUIT",              &Proxy::command_QUIT},                         // - Close the connection
  {"RANDOMKEY",         &Proxy::command_forward_random},               // - Return a random key from the keyspace
  {"RENAME",            &Proxy::command_forward_by_keys_1_all},        // key newkey - Rename a key
  {"RENAMENX",          &Proxy::command_forward_by_keys_1_all},        // key newkey - Rename a key, only if the new key does not exist
  {"RESTORE",           &Proxy::command_forward_by_key_1},             // key ttl serialized-value - Create a key using the provided serialized value, previously obtained using DUMP.
  {"ROLE",              &Proxy::command_ROLE},                         // - Return the role of the instance in the context of replication
  {"RPOP",              &Proxy::command_forward_by_key_1},             // key - Remove and get the last element in a list
  {"RPOPLPUSH",         &Proxy::command_forward_by_keys_1_all},        // source destination - Remove the last element in a list, append it to another list and return it
  {"RPUSH",             &Proxy::command_forward_by_key_1},             // key value [value ...] - Append one or multiple values to a list
  {"RPUSHX",            &Proxy::command_forward_by_key_1},             // key value - Append a value to a list, only if the list exists
  {"SADD",              &Proxy::command_forward_by_key_1},             // key member [member ...] - Add one or more members to a set
  {"SAVE",              &Proxy::command_all_collect_status_responses}, // - Synchronously save the dataset to disk
  {"SCAN",              &Proxy::command_SCAN},                         // cursor [MATCH pattern] [COUNT count] - Incrementally iterate the keys space
  {"SCARD",             &Proxy::command_forward_by_key_1},             // key - Get the number of members in a set
  {"SCRIPT",            &Proxy::command_SCRIPT},                       // KILL / EXISTS name / FLUSH / LOAD data - Kill the script currently in execution.
  {"SDIFF",             &Proxy::command_forward_by_keys_1_all},        // key [key ...] - Subtract multiple sets
  {"SDIFFSTORE",        &Proxy::command_forward_by_keys_1_all},        // destination key [key ...] - Subtract multiple sets and store the resulting set in a key
  {"SET",               &Proxy::command_forward_by_key_1},             // key value [EX seconds] [PX milliseconds] [NX|XX] - Set the string value of a key
  {"SETBIT",            &Proxy::command_forward_by_key_1},             // key offset value - Sets or clears the bit at offset in the string value stored at key
  {"SETEX",             &Proxy::command_forward_by_key_1},             // key seconds value - Set the value and expiration of a key
  {"SETNX",             &Proxy::command_forward_by_key_1},             // key value - Set the value of a key, only if the key does not exist
  {"SETRANGE",          &Proxy::command_forward_by_key_1},             // key offset value - Overwrite part of a string at key starting at the specified offset
  {"SHUTDOWN",          &Proxy::command_all_collect_status_responses}, // [NOSAVE] [SAVE] - Synchronously save the dataset to disk and then shut down the server
  {"SINTER",            &Proxy::command_forward_by_keys_1_all},        // key [key ...] - Intersect multiple sets
  {"SINTERSTORE",       &Proxy::command_forward_by_keys_1_all},        // destination key [key ...] - Intersect multiple sets and store the resulting set in a key
  {"SISMEMBER",         &Proxy::command_forward_by_key_1},             // key member - Determine if a given value is a member of a set
  {"SLOWLOG",           &Proxy::command_all_collect_responses},        // GET [n] / LEN / RESET
  {"SMEMBERS",          &Proxy::command_forward_by_key_1},             // key - Get all the members in a set
  {"SMOVE",             &Proxy::command_forward_by_keys_1_2},          // source destination member - Move a member from one set to another
  {"SORT",              &Proxy::command_forward_by_key_1},             // key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination] - Sort the elements in a list, set or sorted set
  {"SPOP",              &Proxy::command_forward_by_key_1},             // key - Remove and return a random member from a set
  {"SRANDMEMBER",       &Proxy::command_forward_by_key_1},             // key [count] - Get one or multiple random members from a set
  {"SREM",              &Proxy::command_forward_by_key_1},             // key member [member ...] - Remove one or more members from a set
  {"SSCAN",             &Proxy::command_forward_by_key_1},             // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate Set elements
  {"STRLEN",            &Proxy::command_forward_by_key_1},             // key - Get the length of the value stored in a key
  {"SUNION",            &Proxy::command_forward_by_keys_1_all},        // key [key ...] - Add multiple sets
  {"SUNIONSTORE",       &Proxy::command_forward_by_keys_1_all},        // destination key [key ...] - Add multiple sets and store the resulting set in a key
  {"TIME",              &Proxy::command_all_collect_responses},        // - Return the current server time
  {"TOUCH",             &Proxy::command_partition_by_keys_1_integer},  // key [key ...] - Update last access time for keys without getting them
  {"TTL",               &Proxy::command_forward_by_key_1},             // key - Get the time to live for a key
  {"TYPE",              &Proxy::command_forward_by_key_1},             // key - Determine the type stored at key
  {"UNLINK",            &Proxy::command_partition_by_keys_1_integer},  // key [key ...] - Delete keys asynchonously
  {"ZADD",              &Proxy::command_forward_by_key_1},             // key score member [score member ...] - Add one or more members to a sorted set, or update its score if it already exists
  {"ZCARD",             &Proxy::command_forward_by_key_1},             // key - Get the number of members in a sorted set
  {"ZCOUNT",            &Proxy::command_forward_by_key_1},             // key min max - Count the members in a sorted set with scores within the given values
  {"ZINCRBY",           &Proxy::command_forward_by_key_1},             // key increment member - Increment the score of a member in a sorted set
  {"ZINTERSTORE",       &Proxy::command_ZACTIONSTORE},                 // destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] - Intersect multiple sorted sets and store the resulting sorted set in a new key
  {"ZLEXCOUNT",         &Proxy::command_forward_by_key_1},             // key min max - Count the members in a sorted set with scores within the given values
  {"ZRANGE",            &Proxy::command_forward_by_key_1},             // key start stop [WITHSCORES] - Return a range of members in a sorted set, by index
  {"ZRANGEBYLEX",       &Proxy::command_forward_by_key_1},             // key min max [LIMIT offset count] - Return a range of members in a sorted set, by lex
  {"ZRANGEBYSCORE",     &Proxy::command_forward_by_key_1},             // key min max [WITHSCORES] [LIMIT offset count] - Return a range of members in a sorted set, by score
  {"ZRANK",             &Proxy::command_forward_by_key_1},             // key member - Determine the index of a member in a sorted set
  {"ZREM",              &Proxy::command_forward_by_key_1},             // key member [member ...] - Remove one or more members from a sorted set
  {"ZREMRANGEBYLEX",    &Proxy::command_forward_by_key_1},             // key min max - Remove all members in a sorted set within the given lex range
  {"ZREMRANGEBYRANK",   &Proxy::command_forward_by_key_1},             // key start stop - Remove all members in a sorted set within the given indexes
  {"ZREMRANGEBYSCORE",  &Proxy::command_forward_by_key_1},             // key min max - Remove all members in a sorted set within the given scores
  {"ZREVRANGE",         &Proxy::command_forward_by_key_1},             // key start stop [WITHSCORES] - Return a range of members in a sorted set, by index, with scores ordered from high to low
  {"ZREVRANGEBYLEX",    &Proxy::command_forward_by_key_1},             // key min max [LIMIT offset count] - Return a range of members in a sorted set, by lex from high to low
  {"ZREVRANGEBYSCORE",  &Proxy::command_forward_by_key_1},             // key max min [WITHSCORES] [LIMIT offset count] - Return a range of members in a sorted set, by score, with scores ordered from high to low
  {"ZREVRANK",          &Proxy::command_forward_by_key_1},             // key member - Determine the index of a member in a sorted set, with scores ordered from high to low
  {"ZSCAN",             &Proxy::command_forward_by_key_1},             // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate sorted sets elements and associated scores
  {"ZSCORE",            &Proxy::command_forward_by_key_1},             // key member - Get the score associated with the given member in a sorted set
  {"ZUNIONSTORE",       &Proxy::command_ZACTIONSTORE},                 // destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] - Add multiple sorted sets and store the resulting sorted set in a new key

  // commands that aren't part of the official protocol
  {"BACKEND",           &Proxy::command_BACKEND},    // key - Get the backend number that the given key hashes to
  {"BACKENDNUM",        &Proxy::command_BACKENDNUM}, // key - Get the backend number that the given key hashes to
  {"BACKENDS",          &Proxy::command_BACKENDS},   // - Get the list of all backend netlocs
  {"FORWARD",           &Proxy::command_FORWARD},    // backendnum command [args] - forward the given command directly to the given backend
  {"PRINTSTATE",        &Proxy::command_PRINTSTATE}, // - print the proxy's state to stderr
});
