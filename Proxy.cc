#define _STDC_FORMAT_MACROS

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <event2/bufferevent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

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
    : name(), debug_name(), should_disconnect(false), bev(move(bev)), parser(),
    local_addr(), remote_addr(), num_commands_received(0),
    num_responses_sent(0), head_link(NULL), tail_link(NULL) {
  get_socket_addresses(bufferevent_getfd(this->bev.get()), &this->local_addr,
      &this->remote_addr);
  this->debug_name = render_sockaddr_storage(this->remote_addr) +
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
  fprintf(stream, "Client[name=%s, debug_name=%s, should_disconnect=%s, io_counts=[%zu, %zu], chain=[",
      this->name.c_str(), this->debug_name.c_str(), this->should_disconnect ? "true" : "false",
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
    case CollectionType::ModifyMigrateResponse:
      return "ModifyMigrateResponse";
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
      string_printf("<%s>", this->client->debug_name.c_str()) : "(missing)";

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
    case CollectionType::ModifyMigrateResponse:
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

Proxy::Stats::Stats() : num_commands_received(0), num_commands_sent(0),
    num_responses_received(0), num_responses_sent(0),
    num_connections_received(0), num_clients(0), start_time(now()) { }

Proxy::Proxy(int listen_fd, shared_ptr<const ConsistentHashRing> ring,
    int hash_begin_delimiter, int hash_end_delimiter, shared_ptr<Stats> stats,
    size_t proxy_index) : listen_fd(listen_fd),
    base(event_base_new(), event_base_free),
    listener(evconnlistener_new(this->base.get(),
        Proxy::dispatch_on_client_accept, this, LEV_OPT_REUSEABLE, 0,
        this->listen_fd), evconnlistener_free),
    should_exit(false), ring(ring), backends(), name_to_backend(),
    bev_to_backend_conn(), bev_to_client(), proxy_index(proxy_index),
    stats(stats), hash_begin_delimiter(hash_begin_delimiter),
    hash_end_delimiter(hash_end_delimiter), handlers(this->default_handlers) {

  if (!this->stats.get()) {
    this->stats.reset(new Stats());
  }

  evconnlistener_set_error_cb(this->listener.get(), Proxy::dispatch_on_listen_error);

  // set up backend structures
  for (const auto& host : this->ring->all_hosts()) {
    Backend* b = new Backend(this->backends.size(), host.host,
        host.port, host.name);
    this->backends.emplace_back(b);
    this->name_to_backend.emplace(b->name, b);
  }
}

bool Proxy::disable_command(const string& command_name) {
  return this->handlers.erase(command_name);
}

void Proxy::serve() {
  struct timeval tv = {1, 0}; // 1 second

  struct event* ev = event_new(this->base.get(), -1, EV_PERSIST,
      &Proxy::dispatch_check_for_thread_exit, this);
  event_add(ev, &tv);

  event_base_dispatch(this->base.get());

  event_del(ev);
}

void Proxy::stop() {
  this->should_exit = true;
}

void Proxy::print(FILE* stream, int indent_level) const {
  if (indent_level < 0) {
    indent_level = -indent_level;
  } else {
    print_indent(stream, indent_level);
  }

  fprintf(stream, "Proxy[listen_fd=%d, num_clients=%zu, io_counts=[%zu, %zu, %zu, %zu], clients=[\n",
      this->listen_fd, this->bev_to_client.size(),
      this->stats->num_commands_received.load(),
      this->stats->num_commands_sent.load(),
      this->stats->num_responses_received.load(),
      this->stats->num_responses_sent.load());

  for (const auto& bev_client : this->bev_to_client) {
    print_indent(stream, indent_level + 1);
    bev_client.second.print(stream, indent_level + 1);
    fprintf(stream, ",\n");
  }

  fprintf(stream, "], backends=[\n");
  for (const auto& backend : this->backends) {
    print_indent(stream, indent_level + 1);
    backend->print(stream, indent_level + 1);
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

  return this->ring->host_id_for_key(s.data() + hash_begin_pos,
      hash_end_pos - hash_begin_pos);
}

int64_t Proxy::backend_index_for_argument(const string& arg) const {
  try {
    return this->name_to_backend.at(arg)->index;
  } catch (const std::out_of_range& e) {
    char* endptr;
    int64_t backend_index = strtoll(arg.data(), &endptr, 0);
    int64_t backend_count = this->backends.size();
    if ((endptr == arg.data()) ||
        (backend_index < 0 || backend_index >= backend_count)) {
      return -1;
    }
    return backend_index;
  }
}

Backend& Proxy::backend_for_index(size_t index) {
  return *this->backends[index];
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
  unique_ptr<struct bufferevent, void(*)(struct bufferevent*)> bev(
      bufferevent_socket_new(this->base.get(), -1, BEV_OPT_CLOSE_ON_FREE),
      bufferevent_free);
  bufferevent_setcb(bev.get(), Proxy::dispatch_on_backend_input, NULL,
      Proxy::dispatch_on_backend_error, this);
  evbuffer_defer_callbacks(bufferevent_get_output(bev.get()), this->base.get());

  // connect to the backend (nonblocking)
  auto s = make_sockaddr_storage(b.host, b.port);
  if (bufferevent_socket_connect(bev.get(), (struct sockaddr*)&s.first,
      s.second) < 0) {
    string error = string_for_error(errno);
    throw runtime_error(string_printf(
        "error: can\'t connect to backend %s:%d (errno=%d) (%s)\n",
        b.host.c_str(), b.port, errno, error.c_str()));
  }

  // allow reads & writes (though data won't be sent until it's connected)
  bufferevent_enable(bev.get(), EV_READ | EV_WRITE);

  // track this connection
  BackendConnection& conn = b.index_to_connection.emplace(piecewise_construct,
      forward_as_tuple(b.next_connection_index),
      forward_as_tuple(&b, b.next_connection_index, move(bev))).first->second;
  b.next_connection_index++;
  this->bev_to_backend_conn[conn.bev.get()] = &conn;

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
  this->stats->num_clients--;
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

ResponseLink* Proxy::create_error_link(Client* c, shared_ptr<Response> r) {
  ResponseLink* l = new ResponseLink(CollectionType::ForwardResponse, c);
  l->error_response = r;
  return l;
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
  this->stats->num_commands_sent++;
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
        c->debug_name.c_str());
    return;
  }

  r->write(out);
  c->num_responses_sent++;
  this->stats->num_responses_sent++;
}

void Proxy::send_client_response(Client* c, const shared_ptr<const Response>& r) {
  this->send_client_response(c, r.get());
}

void Proxy::send_client_string_response(Client* c, const char* s,
    Response::Type type) {

  struct evbuffer* out = c->get_output_buffer();
  if (!out) {
    log(WARNING, "tried to send response to client %s with no output buffer",
        c->debug_name.c_str());
    return;
  }

  Response::write_string(out, s, type);
  c->num_responses_sent++;
  this->stats->num_responses_sent++;
}

void Proxy::send_client_string_response(Client* c, const string& s,
    Response::Type type) {

  struct evbuffer* out = c->get_output_buffer();
  if (!out) {
    log(WARNING, "tried to send response to client %s with no output buffer",
        c->debug_name.c_str());
    return;
  }

  Response::write_string(out, s.data(), s.size(), type);
  c->num_responses_sent++;
  this->stats->num_responses_sent++;
}

void Proxy::send_client_string_response(Client* c, const void* data,
    size_t size, Response::Type type) {

  struct evbuffer* out = c->get_output_buffer();
  if (!out) {
    log(WARNING, "tried to send response to client %s with no output buffer",
        c->debug_name.c_str());
    return;
  }

  Response::write_string(out, data, size, type);
  c->num_responses_sent++;
  this->stats->num_responses_sent++;
}

void Proxy::send_client_int_response(Client* c, int64_t int_value,
    Response::Type type) {

  struct evbuffer* out = c->get_output_buffer();
  if (!out) {
    log(WARNING, "tried to send response to client %s with no output buffer",
        c->debug_name.c_str());
    return;
  }

  Response::write_int(out, int_value, type);
  c->num_responses_sent++;
  this->stats->num_responses_sent++;
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

  // this should only be called when the client is known to be valid
  assert(l->client);

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
        if (next_backend_id < static_cast<int64_t>(this->backends.size())) {
          // the next cursor should be 0, but on the next backend
          uint8_t index_bits = this->scan_cursor_backend_index_bits();
          l->response_to_forward->fields[0]->data = string_printf(
              "%" PRIu64, next_backend_id << (64 - index_bits));
        }

      // if this backend isn't done, add the current backend index to the cursor
      } else {

        // parse the cursor value
        char* endptr;
        uint64_t cursor_value = strtoull(cursor->data.data(), &endptr, 0);
        if (endptr == cursor->data.data()) {
          this->send_client_string_response(l->client,
              "PROXYERROR the backend returned a non-integer cursor",
              Response::Type::Error);
          break;
        }

        // if the backend index overwrites part of the cursor, return an error
        // what the heck are you storing in redis if the cursor is that large?
        uint8_t index_bits = this->scan_cursor_backend_index_bits();
        if (cursor_value & ~((1 << (64 - index_bits)) - 1)) {
          this->send_client_string_response(l->client,
              "PROXYERROR the backend\'s keyspace is too large",
              Response::Type::Error);
          break;
        }

        cursor_value |= (l->scan_backend_index << (64 - index_bits));
        cursor->data = string_printf("%" PRIu64, cursor_value);
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

    case CollectionType::ModifyMigrateResponse: {
      // each response should be either a status (OK) or error (NOKEY)
      size_t num_ok_responses = 0;
      bool error_response = false;
      for (const auto& backend_r : l->responses) {
        if (backend_r->type == Response::Type::Status) {
          if (backend_r->data != "NOKEY") {
            num_ok_responses++;
          }
        } else if (backend_r->type == Response::Type::Error) {
          error_response = true;
        }
      }

      if (error_response) {
        Response r(Response::Type::Multi, l->responses.size());
        for (auto& backend_r : l->responses) {
          r.fields.emplace_back(backend_r);
        }
        this->send_client_response(l->client, &r);
        return;
      }

      if (num_ok_responses) {
        this->send_client_string_response(l->client, "OK",
            Response::Type::Status);
      } else {
        this->send_client_string_response(l->client, "NOKEY",
            Response::Type::Status);
      }

      break;
    }

    default:
      this->send_client_response(l->client, unknown_collection_type_error_response);
  }
}

void Proxy::send_all_ready_responses(Client* c) {
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
        if (r->type == Response::Type::Error) {
          if (!l->error_response.get()) {
            l->error_response.reset(new Response(Response::Type::Error,
                "CHANNELERROR one of more backends returned error responses:"));
          }
          l->error_response->data += string_printf(" (%s) %s",
              conn->backend->name.c_str(), r->data.c_str());
        } else if (r->type != Response::Type::Status) {
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
      case CollectionType::ModifyMigrateResponse:
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
    this->send_all_ready_responses(l->client);
  }
}

void Proxy::handle_client_command(Client* c, shared_ptr<DataCommand> cmd) {

  if (cmd->args.size() <= 0) {
    static shared_ptr<Response> invalid_command_response(new Response(
        Response::Type::Error, "ERR invalid command"));
    if (c->tail_link) {
      this->create_error_link(c, invalid_command_response);
      // TODO: is this call necessary? presumably if there were waiting links,
      // they can't be ready at this point... right?
      this->send_all_ready_responses(c);

    } else {
      this->send_client_response(c, invalid_command_response);
    }
    return;
  }

  // command names are case-insensitive; convert it to uppercase
  char* arg0_str = const_cast<char*>(cmd->args[0].c_str());
  for (size_t x = 0; x < cmd->args[0].size(); x++) {
    arg0_str[x] = toupper(arg0_str[x]);
  }

  // find the appropriate handler
  command_handler handler;
  try {
    handler = this->handlers.at(arg0_str);
  } catch (const out_of_range& e) {
    handler = &Proxy::command_default;
  }

  // call the handler
  ResponseLink* orig_tail_link = c->tail_link;
  try {
    (this->*handler)(c, cmd);

  } catch (const exception& e) {
    shared_ptr<Response> r(new Response(Response::Type::Error,
        string_printf("PROXYERROR handler failed: %s", e.what())));

    // if there's no tail_link, then there are no pending responses - just send
    // the response directly
    if (!c->tail_link) {
      this->send_client_response(c, r);

    // if tail_link is present and didn't change, then the handler didn't create
    // a ResponseLink, but there are other responses waiting - add the error
    // after the waiting responses to maintain correct ordering
    } else if (c->tail_link == orig_tail_link) {
      this->create_error_link(c, r);

    // if tail_link is present and did change, then the handler created a
    // ResponseLink representing this command - apply the error to it to
    // maintain correct ordering. but if there's already an error, don't
    // overwrite it
    } else if (!c->tail_link->error_response.get()) {
      c->tail_link->error_response = r;
    }
  }

  // check for complete responses. the command handler probably produces a
  // ResponseLink object, and this object can contain an error already (and if
  // so, it's likely to be ready)
  this->send_all_ready_responses(c);
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
      this->stats->num_commands_received++;
      this->handle_client_command(&c, cmd);
    }
    if (c.parser.error()) {
      log(WARNING, "parse error in client %s input stream",
          c.debug_name.c_str(), c.parser.error());
      c.should_disconnect = true;
    }

  } catch (const exception& e) {
    log(WARNING, "error in client %s input stream: %s", c.debug_name.c_str(),
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
        c.debug_name.c_str(), err, evutil_socket_error_to_string(err));
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
    // if there's no client on the queue or if the head of the queue is a
    // forwarding client, then use the forwarding parser (don't allocate a
    // response object). in the first case, the response will be discarded
    // (probably the client disconnected early); in the second case, the
    // response will be forwarded verbatim to the client.
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
        this->disconnect_backend(conn);
        break;
      }

      conn->num_responses_received++;
      conn->backend->num_responses_received++;
      this->stats->num_responses_received++;
      if (l->client) {
        l->client->num_responses_sent++;
      }
      this->stats->num_responses_sent++;

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

      if (l->client) {
        l->client->head_link = l->next_client;
        if (!l->client->head_link) {
          l->client->tail_link = NULL;
        }
      }

      assert(l->is_ready());
      delete l;

    } else {
      shared_ptr<Response> rsp;
      try {
        rsp = conn->parser.resume(in_buffer);
      } catch (const exception& e) {
        log(WARNING, "parse error in backend stream %s (%s)",
            conn->backend->debug_name.c_str(), e.what());
        this->disconnect_backend(conn);
        break;
      }
      if (!rsp.get()) {
        if (conn->parser.error()) {
          log(WARNING, "parse error in backend stream %s",
              conn->backend->debug_name.c_str());
          this->disconnect_backend(conn);
        }
        break;
      }

      conn->num_responses_received++;
      conn->backend->num_responses_received++;
      this->stats->num_responses_received++;
      this->handle_backend_response(conn, rsp);
    }
  }

  if (conn->parser.error()) {
    log(WARNING, "parse error in backend stream %s (%s)",
        conn->backend->debug_name.c_str(), conn->parser.error());
    this->disconnect_backend(conn);
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

  // enable TCP keepalive on the socket
  int optval = 1;
  socklen_t optlen = sizeof(optval);
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &optval, optlen) < 0) {
    string error = string_for_error(errno);
    fprintf(stderr, "warning: failed to enable tcp keepalive on fd %d (%s)\n",
        fd, error.c_str());
  }

  // set up a bufferevent for the new connection
  struct bufferevent* raw_bev = bufferevent_socket_new(this->base.get(), fd,
      BEV_OPT_CLOSE_ON_FREE);
  unique_ptr<struct bufferevent, void(*)(struct bufferevent*)> bev(
      raw_bev, bufferevent_free);
  evbuffer_defer_callbacks(bufferevent_get_output(bev.get()), this->base.get());

  // create a Client for this connection
  this->bev_to_client.emplace(piecewise_construct,
      forward_as_tuple(raw_bev), forward_as_tuple(move(bev)));
  this->stats->num_connections_received++;
  this->stats->num_clients++;

  // set read/error callbacks and enable i/o
  bufferevent_setcb(raw_bev, Proxy::dispatch_on_client_input, NULL,
      Proxy::dispatch_on_client_error, this);
  bufferevent_enable(raw_bev, EV_READ | EV_WRITE);
}



////////////////////////////////////////////////////////////////////////////////
// timer event handlers

void Proxy::dispatch_check_for_thread_exit(evutil_socket_t fd, short what,
    void* ctx) {
  ((Proxy*)ctx)->check_for_thread_exit(fd, what);
}


void Proxy::check_for_thread_exit(evutil_socket_t fd, short what) {
  if (this->should_exit) {
    event_base_loopexit(this->base.get(), NULL);
  }
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

void Proxy::command_all_sum_int_responses(Client* c,
    shared_ptr<DataCommand> cmd) {
  this->command_forward_all(c, cmd, CollectionType::SumIntegerResponses);
}

void Proxy::command_forward_all(Client* c, shared_ptr<DataCommand> cmd,
    CollectionType type) {
  auto l = this->create_link(type, c);
  for (size_t backend_index = 0; backend_index < this->backends.size();
       backend_index++) {
    BackendConnection& conn = this->backend_conn_for_index(backend_index);
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

  int64_t num_args = cmd->args.size();
  if (num_args <= start_key_index) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if ((end_key_index < 0) || (end_key_index > num_args)) {
    end_key_index = num_args;
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
      rand() % this->backends.size());
  ResponseLink* l = this->create_link(CollectionType::ForwardResponse, c);
  this->send_command_and_link(&conn, l, cmd);
}

void Proxy::command_partition_by_keys(Client* c, shared_ptr<DataCommand> cmd,
    size_t start_arg_index, size_t args_per_key, bool interleaved,
    CollectionType type) {

  if (cmd->args.size() <= start_arg_index) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }
  if (((cmd->args.size() - start_arg_index) % args_per_key) != 0) {
    this->send_client_string_response(c, "ERR incorrect number of arguments",
        Response::Type::Error);
    return;
  }
  size_t num_keys = (cmd->args.size() - start_arg_index) / args_per_key;

  // set up the ResponseLink
  auto l = this->create_link(type, c);
  if (l->type == CollectionType::CollectMultiResponsesByKey) {
    l->recombination_queue.reserve(num_keys);
  }

  // compute the backend for each key, creating command objects as needed
  vector<ReferenceCommand> backend_commands(this->backends.size());
  if (interleaved) {
    for (size_t y = 0; y < num_keys; y++) {
      size_t base_arg_index = start_arg_index + (y * args_per_key);
      int64_t backend_index = this->backend_index_for_key(
          cmd->args[base_arg_index]);

      // the recombination queue tells us in which order we should pull keys
      // from the responses when sending the ready response to the client
      if (l->type == CollectionType::CollectMultiResponsesByKey) {
        l->recombination_queue.emplace_back(backend_index);
      }

      // create the backend command if needed and copy the args before the keys
      auto& backend_cmd = backend_commands[backend_index];
      if (backend_cmd.args.empty()) {
        for (size_t z = 0; z < start_arg_index; z++) {
          const string& arg = cmd->args[z];
          backend_cmd.args.emplace_back(arg.data(), arg.size());
        }
      }

      // add the key to the backend command
      for (size_t z = 0; z < args_per_key; z++) {
        const string& arg = cmd->args[base_arg_index + z];
        backend_cmd.args.emplace_back(arg.data(), arg.size());
      }
    }

  } else { // not interleaved (the annoying case)
    vector<vector<int64_t>> backend_key_indexes(this->backends.size());
    for (size_t y = 0; y < num_keys; y++) {
      size_t arg_index = start_arg_index + y;
      int64_t backend_index = this->backend_index_for_key(cmd->args[arg_index]);

      // the recombination queue tells us in which order we should pull keys
      // from the responses when sending the ready response to the client
      if (l->type == CollectionType::CollectMultiResponsesByKey) {
        l->recombination_queue.emplace_back(backend_index);
      }

      backend_key_indexes[backend_index].emplace_back(y);
    }

    for (int64_t backend_index = 0;
         backend_index < static_cast<ssize_t>(this->backends.size());
         backend_index++) {
      const auto& key_indexes = backend_key_indexes.at(backend_index);
      if (key_indexes.empty()) {
        continue;
      }
      size_t dest_num_keys = key_indexes.size();

      // create the backend command
      auto& backend_cmd = backend_commands[backend_index];
      backend_cmd.args.resize(start_arg_index + dest_num_keys * args_per_key);

      // copy the args before the keys
      for (size_t z = 0; z < start_arg_index; z++) {
        const string& arg = cmd->args[z];
        backend_cmd.args.emplace_back(arg.data(), arg.size());
      }

      // add the keys to the backend command
      for (size_t dest_key_index = 0; dest_key_index < dest_num_keys;
           dest_key_index++) {
        for (size_t z = 0; z < args_per_key; z++) {
          size_t src_arg_index = start_arg_index + (key_indexes.at(z) * num_keys);
          const string& arg = cmd->args[src_arg_index];
          size_t dest_arg_index = start_arg_index + dest_key_index +
              (z * dest_num_keys);
          auto& dest_arg = backend_cmd.args[dest_arg_index];
          dest_arg.data = arg.data();
          dest_arg.size = arg.size();
        }
      }
    }
  }

  // send the commands off
  for (size_t backend_index = 0; backend_index < this->backends.size();
       backend_index++) {
    const auto& backend_cmd = backend_commands[backend_index];
    if (backend_cmd.args.empty()) {
      continue;
    }
    BackendConnection& conn = this->backend_conn_for_index(backend_index);
    this->send_command_and_link(&conn, l, &backend_cmd);
  }
}

void Proxy::command_partition_by_keys_1_integer(Client* c,
    shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() == 2) {
    this->command_forward_by_key_1(c, cmd);
  } else {
    this->command_partition_by_keys(c, cmd, 1, 1, true,
        CollectionType::SumIntegerResponses);
  }
}

void Proxy::command_partition_by_keys_1_multi(Client* c,
    shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() == 2) {
    this->command_forward_by_key_1(c, cmd);
  } else {
    this->command_partition_by_keys(c, cmd, 1, 1, true,
        CollectionType::CollectMultiResponsesByKey);
  }
}

void Proxy::command_partition_by_keys_2_status(Client* c,
    shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() == 3) {
    this->command_forward_by_key_1(c, cmd);
  } else {
    this->command_partition_by_keys(c, cmd, 1, 2, true,
        CollectionType::CollectStatusResponses);
  }
}

void Proxy::command_unimplemented(Client* c, shared_ptr<DataCommand> cmd) {
  this->send_client_string_response(c, "PROXYERROR command not supported",
      Response::Type::Error);
}

void Proxy::command_default(Client* c, shared_ptr<DataCommand> cmd) {
  string formatted_cmd = cmd->format();
  log(INFO, "unknown command from %s: %s", c->debug_name.c_str(),
      formatted_cmd.c_str());

  this->send_client_string_response(c, "PROXYERROR unknown command",
      Response::Type::Error);
}



////////////////////////////////////////////////////////////////////////////////
// specific command implementations

void Proxy::command_ACL(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if ((cmd->args[1] == "LOAD") || (cmd->args[1] == "SAVE") ||
      (cmd->args[1] == "SETUSER")) {
    this->command_all_collect_status_responses(c, cmd);
    return;
  }

  if ((cmd->args[1] == "GETUSER") || (cmd->args[1] == "LIST") ||
      (cmd->args[1] == "LOG") || (cmd->args[1] == "USERS")) {
    this->command_all_collect_responses(c, cmd);
    return;
  }

  if (cmd->args[1] == "DELUSER") {
    this->command_all_sum_int_responses(c, cmd);
    return;
  }

  if ((cmd->args[1] == "CAT") || (cmd->args[1] == "GENPASS") ||
      (cmd->args[1] == "HELP")) {
    this->command_forward_random(c, cmd);
    return;
  }

  this->send_client_string_response(c, "ERR unrecognized subcommand",
      Response::Type::Error);
}

void Proxy::command_BACKEND(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if (cmd->args.size() == 2) {
    const Backend& b = this->backend_for_key(cmd->args[1]);
    this->send_client_string_response(c, b.name, Response::Type::Data);

  } else {
    Response r(Response::Type::Multi, cmd->args.size() - 1);
    for (size_t arg_index = 1; arg_index < cmd->args.size(); arg_index++) {
      const auto& arg = cmd->args[arg_index];
      const Backend& b = this->backend_for_key(arg);
      r.fields.emplace_back(new Response(Response::Type::Data, b.name));
    }
    this->send_client_response(c, &r);
  }
}

void Proxy::command_BACKENDNUM(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if (cmd->args.size() == 2) {
    int64_t backend_index = this->backend_index_for_key(cmd->args[1]);
    this->send_client_int_response(c, backend_index, Response::Type::Integer);

  } else {
    Response r(Response::Type::Multi, cmd->args.size() - 1);
    for (size_t arg_index = 1; arg_index < cmd->args.size(); arg_index++) {
      const auto& arg = cmd->args[arg_index];
      int64_t backend_index = this->backend_index_for_key(arg);
      r.fields.emplace_back(new Response(Response::Type::Integer,
          backend_index));
      r.fields.back()->int_value = backend_index;
    }
    this->send_client_response(c, &r);
  }
}

void Proxy::command_BACKENDS(Client* c, shared_ptr<DataCommand> cmd) {
  Response r(Response::Type::Multi, this->backends.size());
  for (const auto& b : this->backends) {
    r.fields.emplace_back(new Response(Response::Type::Data, b->debug_name));
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

      int fd = bufferevent_getfd(c.bev.get());
      string addr_str = render_sockaddr_storage(c.remote_addr);

      response_data += string_printf(
          "addr=%s fd=%d name=%s debug_name=%s cmdrecv=%d rspsent=%d rspchain=%d\n",
          addr_str.c_str(), fd, c.name.c_str(), c.debug_name.c_str(),
          c.num_commands_received, c.num_responses_sent, response_chain_length);
    }

    this->send_client_string_response(c, response_data, Response::Type::Data);

  } else if (cmd->args[1] == "GETNAME") {
    if (c->name.empty()) {
      // Redis returns a null response if the client name is missing
      this->send_client_int_response(c, -1, Response::Type::Data);
    } else {
      this->send_client_string_response(c, c->name, Response::Type::Data);
    }

  } else if (cmd->args[1] == "SETNAME") {
    if (cmd->args.size() != 3) {
      this->send_client_string_response(c, "ERR incorrect argument count",
          Response::Type::Error);
      return;
    }
    if (cmd->args[2].size() > 0x100) {
      this->send_client_string_response(c,
          "ERR client names can be at most 256 bytes", Response::Type::Error);
      return;
    }
    if (cmd->args[2].find(' ') != string::npos) {
      this->send_client_string_response(c,
          "ERR client names can\'t contain spaces", Response::Type::Error);
      return;
    }

    c->name = cmd->args[2];
    this->send_client_string_response(c, "OK", Response::Type::Status);

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

  int64_t num_args = cmd->args.size();
  if (num_args < 3) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  char* endptr;
  int64_t num_keys = strtoll(cmd->args[2].data(), &endptr, 0);
  if ((endptr == cmd->args[2].data()) ||
      (num_keys < 0 || num_keys > num_args - 3)) {
    this->send_client_string_response(c, "ERR key count is invalid",
        Response::Type::Error);
    return;
  }

  // check that the keys all hash to the same server
  int64_t backend_index = -1;
  for (int64_t x = 3; x < num_keys + 3; x++) {
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
    backend_index = rand() % this->backends.size();
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

  // send everything after the backend name/index to the backend
  ReferenceCommand backend_cmd(cmd->args.size() - 2);
  for (size_t x = 2; x < cmd->args.size(); x++) {
    const string& arg = cmd->args[x];
    backend_cmd.args.emplace_back(arg.data(), arg.size());
  }

  // if the backend name/index is blank, forward to all backends and return
  // their responses verbatim
  if (cmd->args[1].empty()) {
    auto l = this->create_link(CollectionType::CollectResponses, c);
    for (size_t backend_index = 0; backend_index < this->backends.size();
         backend_index++) {
      BackendConnection& conn = this->backend_conn_for_index(backend_index);
      this->send_command_and_link(&conn, l, &backend_cmd);
    }

  // else, forward to a specific backend
  } else {
    int64_t backend_index = this->backend_index_for_argument(cmd->args[1]);
    if (backend_index < 0) {
      this->send_client_string_response(c, "ERR backend does not exist",
          Response::Type::Error);
      return;
    }

    BackendConnection& conn = this->backend_conn_for_index(backend_index);
    ResponseLink* l = this->create_link(CollectionType::ForwardResponse, c);
    this->send_command_and_link(&conn, l, &backend_cmd);
  }
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

    uint64_t uptime = now() - this->stats->start_time;

    Response r(Response::Type::Data, "\
# Server\n\
redis_version:redis-shatter\n\
process_id:%d\n\
start_time_usecs:%" PRIu64 "\n\
uptime_usecs:%" PRIu64 "\n\
hash_begin_delimiter:%s\n\
hash_end_delimiter:%s\n\
\n\
# Counters\n\
num_commands_received:%zu\n\
num_commands_sent:%zu\n\
num_responses_received:%zu\n\
num_responses_sent:%zu\n\
num_connections_received:%zu\n\
num_clients:%zu\n\
num_clients_this_instance:%zu\n\
num_backends:%zu\n\
", getpid_cached(), this->stats->start_time, uptime, hash_begin_delimiter_str,
        hash_end_delimiter_str, this->stats->num_commands_received.load(),
        this->stats->num_commands_sent.load(),
        this->stats->num_responses_received.load(),
        this->stats->num_responses_sent.load(),
        this->stats->num_connections_received.load(),
        this->stats->num_clients.load(), this->bev_to_client.size(),
        this->backends.size(), this->proxy_index);
    this->send_client_response(c, &r);
    return;
  }

  // INFO BACKEND num - return proxy's info for backend num
  if ((cmd->args.size() == 3) && (cmd->args[1] == "BACKEND")) {
    int64_t backend_index = this->backend_index_for_argument(cmd->args[2]);
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

void Proxy::command_LATENCY(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if ((cmd->args[1] == "DOCTOR") || (cmd->args[1] == "GRAPH") ||
      (cmd->args[1] == "RESET") || (cmd->args[1] == "LATEST") ||
      (cmd->args[1] == "HISTORY")) {
    this->command_all_collect_responses(c, cmd);
    return;
  }

  if ((cmd->args.size() == 2) && (cmd->args[1] == "HELP")) {
    this->command_forward_random(c, cmd);
    return;
  }

  this->send_client_string_response(c, "ERR unrecognized subcommand",
      Response::Type::Error);
}

void Proxy::command_MEMORY(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if ((cmd->args.size() == 2) && (
      (cmd->args[1] == "DOCTOR") || (cmd->args[1] == "MALLOC-STATS") ||
      (cmd->args[1] == "PURGE") || (cmd->args[1] == "STATS"))) {
    this->command_all_collect_responses(c, cmd);
    return;
  }

  if ((cmd->args.size() == 2) && (cmd->args[1] == "HELP")) {
    this->command_forward_random(c, cmd);
    return;
  }

  if ((cmd->args.size() >= 3) && (cmd->args[1] == "USAGE")) {
    this->command_forward_by_key_index(c, cmd, 2);
    return;
  }

  this->send_client_string_response(c, "ERR unrecognized subcommand",
      Response::Type::Error);
}

void Proxy::command_MIGRATE(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() < 6) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
  } else {
    if (!cmd->args[3].empty()) {
      this->command_forward_by_key_index(c, cmd, 3);
    } else {
      // new form of MIGRATE - can contain multiple keys. find the KEYS token
      // and partition the command after it
      size_t arg_index;
      for (arg_index = 6; arg_index < cmd->args.size(); arg_index++) {
        if (cmd->args[arg_index] == "KEYS") {
          break;
        }
      }
      if (arg_index >= cmd->args.size()) {
        this->send_client_string_response(c,
            "ERR the KEYS option is required if argument 3 is blank",
            Response::Type::Error);
        return;
      }

      this->command_partition_by_keys(c, cmd, arg_index, 1, true,
          CollectionType::ModifyMigrateResponse);
    }
  }
}

void Proxy::command_MODULE(Client* c, shared_ptr<DataCommand> cmd) {
  if (cmd->args.size() < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if ((cmd->args[1] == "LIST") || (cmd->args[1] == "LOAD") ||
      (cmd->args[1] == "UNLOAD")) {
    this->command_all_collect_responses(c, cmd);
    return;
  }

  this->send_client_string_response(c, "ERR unrecognized subcommand",
      Response::Type::Error);
}

void Proxy::command_MSETNX(Client* c, shared_ptr<DataCommand> cmd) {

  int64_t num_args = cmd->args.size();
  if (num_args < 3) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if ((num_args & 1) != 1) {
    this->send_client_string_response(c, "ERR incorrect argument count",
        Response::Type::Error);
    return;
  }

  // check that the keys all hash to the same server
  int64_t backend_index = this->backend_index_for_key(cmd->args[1]);
  int x;
  for (x = 3; x < num_args; x += 2) {
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
  if ((cmd->args.size() == 2) && (cmd->args[1] == "HELP")) {
    this->command_forward_random(c, cmd);
    return;
  }

  if (cmd->args.size() != 3) {
    this->send_client_string_response(c, "ERR incorrect argument count",
        Response::Type::Error);
  } else if ((cmd->args[1] == "REFCOUNT") &&
             (cmd->args[1] == "ENCODING") &&
             (cmd->args[1] == "IDLETIME") &&
             (cmd->args[1] == "FREQ")) {
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
      this->backends.size()));

  auto& backends_r = *r.fields[1];
  for (const auto& b : this->backends) {
    backends_r.fields.emplace_back(new Response(Response::Type::Data,
        b->debug_name));
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

  // parse the cursor value
  char* endptr;
  uint64_t cursor = strtoull(cmd->args[1].data(), &endptr, 0);
  if (endptr == cmd->args[1].data()) {
    this->send_client_string_response(c,
        "ERR cursor format is incorrect", Response::Type::Error);
    return;
  }

  // the highest-order bits of the cursor are the backend index; the rest are
  // the cursor on that backend
  uint8_t index_bits = this->scan_cursor_backend_index_bits();
  int64_t backend_index = (cursor >> (64 - index_bits)) &
      ((1 << index_bits) - 1);
  int64_t backend_count = this->backends.size();
  if ((backend_index < 0) || (backend_index >= backend_count)) {
    this->send_client_string_response(c,
        "PROXYERROR cursor refers to a nonexistent backend",
        Response::Type::Error);
    return;
  }

  // remove the backend index from the cursor
  cursor &= ((1 << (64 - index_bits)) - 1);

  // create backend command. it's ok for the modified argument to be on the
  // stack of this function because send_command_and_link calls evbuffer_add
  // which resolves the data reference
  string cursor_str = string_printf("%" PRIu64, cursor);
  ReferenceCommand backend_cmd(cmd->args.size());
  backend_cmd.args.emplace_back(cmd->args[0].data(), cmd->args[0].size());
  backend_cmd.args.emplace_back(cursor_str.data(), cursor_str.size());
  for (size_t x = 2; x < cmd->args.size(); x++) {
    auto& arg = cmd->args[x];
    backend_cmd.args.emplace_back(arg.data(), arg.size());
  }

  // send command
  BackendConnection& conn = this->backend_conn_for_index(backend_index);
  auto l = this->create_link(CollectionType::ModifyScanResponse, c);
  l->scan_backend_index = backend_index;
  this->send_command_and_link(&conn, l, &backend_cmd);
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

void Proxy::command_XGROUP(Client* c, shared_ptr<DataCommand> cmd) {
  int64_t num_args = cmd->args.size();
  if (num_args < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if (cmd->args[1] == "HELP") {
    this->command_forward_random(c, cmd);

  } else if ((cmd->args[1] == "CREATE") ||
             (cmd->args[1] == "SETID") ||
             (cmd->args[1] == "DESTROY") ||
             (cmd->args[1] == "DELCONSUMER")) {
    this->command_forward_by_key_index(c, cmd, 2);

  } else {
    this->send_client_string_response(c, "ERR unknown subcommand",
        Response::Type::Error);
  }
}

void Proxy::command_XINFO(Client* c, shared_ptr<DataCommand> cmd) {
  int64_t num_args = cmd->args.size();
  if (num_args < 2) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if (cmd->args[1] == "HELP") {
    this->command_forward_random(c, cmd);

  } else if ((cmd->args[1] == "CONSUMERS") ||
             (cmd->args[1] == "GROUPS") ||
             (cmd->args[1] == "STREAM")) {
    this->command_forward_by_key_index(c, cmd, 2);

  } else {
    this->send_client_string_response(c, "ERR unknown subcommand",
        Response::Type::Error);
  }
}

void Proxy::command_XREAD(Client* c, shared_ptr<DataCommand> cmd) {
  int64_t num_args = cmd->args.size();
  if (num_args < 3) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  int64_t arg_index = 1;
  if (cmd->args[0] == "XREADGROUP") {
    if (cmd->args[1] != "GROUP") {
      this->send_client_string_response(c, "ERR GROUP is required",
          Response::Type::Error);
      return;
    }
    arg_index = 4;
  }
  if (arg_index >= num_args) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if (cmd->args[arg_index] == "COUNT") {
    arg_index += 2;
  }
  if (arg_index >= num_args) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  if (cmd->args[arg_index] == "BLOCK") {
    this->send_client_string_response(c,
        "PROXYERROR blocking reads are not supported", Response::Type::Error);
    return;
  }

  if (cmd->args[arg_index] != "STREAMS") {
    this->send_client_string_response(c, "ERR STREAMS argument expected",
        Response::Type::Error);
    return;
  }
  arg_index++;

  if ((num_args - arg_index) & 1) {
    this->send_client_string_response(c,
        "ERR there must be an equal number of streams and IDs",
        Response::Type::Error);
    return;
  }

  this->command_partition_by_keys(c, cmd, arg_index, 2, false,
      CollectionType::CollectMultiResponsesByKey);
}

void Proxy::command_ZACTIONSTORE(Client* c, shared_ptr<DataCommand> cmd) {
  // this is basically the same as command_forward_by_keys except the number of
  // checked keys is given in arg 2

  int64_t num_args = cmd->args.size();
  if (num_args <= 3) {
    this->send_client_string_response(c, "ERR not enough arguments",
        Response::Type::Error);
    return;
  }

  char* endptr;
  int64_t num_keys = strtoll(cmd->args[2].data(), &endptr, 0);
  if ((endptr == cmd->args[2].data()) ||
      (num_keys < 1 || num_keys > num_args - 3)) {
    this->send_client_string_response(c, "ERR key count is invalid",
        Response::Type::Error);
    return;
  }

  // check that the keys all hash to the same server
  int64_t backend_index = this->backend_index_for_key(cmd->args[1]);
  for (int64_t x = 0; x < num_keys; x++) {
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



uint8_t Proxy::scan_cursor_backend_index_bits() const {
  size_t backend_count = this->backends.size();

  // if the backend count is a power of two, we don't need an extra bit
  if ((backend_count & (backend_count - 1)) == 0) {
    return 63 - __builtin_clzll(backend_count);
  } else {
    return 64 - __builtin_clzll(backend_count);
  }

}



const unordered_map<string, Proxy::command_handler> Proxy::default_handlers({
  {"AUTH",              &Proxy::command_unimplemented},
  {"BLPOP",             &Proxy::command_unimplemented},
  {"BRPOP",             &Proxy::command_unimplemented},
  {"BRPOPLPUSH",        &Proxy::command_unimplemented},
  {"BZPOPMAX",          &Proxy::command_unimplemented},
  {"BZPOPMIN",          &Proxy::command_unimplemented},
  {"CLUSTER",           &Proxy::command_unimplemented},
  {"DISCARD",           &Proxy::command_unimplemented},
  {"EXEC",              &Proxy::command_unimplemented},
  {"MONITOR",           &Proxy::command_unimplemented},
  {"MOVE",              &Proxy::command_unimplemented},
  {"MULTI",             &Proxy::command_unimplemented},
  {"PSUBSCRIBE",        &Proxy::command_unimplemented},
  {"PUBLISH",           &Proxy::command_unimplemented},
  {"PUBSUB",            &Proxy::command_unimplemented},
  {"PUNSUBSCRIBE",      &Proxy::command_unimplemented},
  {"READONLY",          &Proxy::command_unimplemented},
  {"READWRITE",         &Proxy::command_unimplemented},
  {"SELECT",            &Proxy::command_unimplemented},
  {"SLAVEOF",           &Proxy::command_unimplemented},
  {"SUBSCRIBE",         &Proxy::command_unimplemented},
  {"SWAPDB",            &Proxy::command_unimplemented},
  {"SYNC",              &Proxy::command_unimplemented},
  {"UNSUBSCRIBE",       &Proxy::command_unimplemented},
  {"UNWATCH",           &Proxy::command_unimplemented},
  {"WAIT",              &Proxy::command_unimplemented},
  {"WATCH",             &Proxy::command_unimplemented},

  {"ACL",               &Proxy::command_ACL},
  {"APPEND",            &Proxy::command_forward_by_key_1},
  {"BGREWRITEAOF",      &Proxy::command_all_collect_status_responses},
  {"BGSAVE",            &Proxy::command_all_collect_status_responses},
  {"BITCOUNT",          &Proxy::command_forward_by_key_1},
  {"BITFIELD",          &Proxy::command_forward_by_key_1},
  {"BITOP",             &Proxy::command_forward_by_keys_2_all},
  {"BITPOS",            &Proxy::command_forward_by_key_1},
  {"CLIENT",            &Proxy::command_CLIENT},
  {"COMMAND",           &Proxy::command_forward_random},
  {"CONFIG",            &Proxy::command_all_collect_responses},
  {"DBSIZE",            &Proxy::command_DBSIZE},
  {"DEBUG",             &Proxy::command_DEBUG},
  {"DECR",              &Proxy::command_forward_by_key_1},
  {"DECRBY",            &Proxy::command_forward_by_key_1},
  {"DEL",               &Proxy::command_partition_by_keys_1_integer},
  {"DUMP",              &Proxy::command_forward_by_key_1},
  {"ECHO",              &Proxy::command_ECHO},
  {"EVAL",              &Proxy::command_EVAL},
  {"EVALSHA",           &Proxy::command_EVAL},
  {"EXISTS",            &Proxy::command_partition_by_keys_1_integer},
  {"EXPIRE",            &Proxy::command_forward_by_key_1},
  {"EXPIREAT",          &Proxy::command_forward_by_key_1},
  {"FLUSHALL",          &Proxy::command_all_collect_status_responses},
  {"FLUSHDB",           &Proxy::command_all_collect_status_responses},
  {"GEOADD",            &Proxy::command_forward_by_key_1},
  {"GEOHASH",           &Proxy::command_forward_by_key_1},
  {"GEOPOS",            &Proxy::command_forward_by_key_1},
  {"GEODIST",           &Proxy::command_forward_by_key_1},
  {"GEORADIUS",         &Proxy::command_GEORADIUS},
  {"GEORADIUSBYMEMBER", &Proxy::command_GEORADIUS},
  {"GET",               &Proxy::command_forward_by_key_1},
  {"GETBIT",            &Proxy::command_forward_by_key_1},
  {"GETRANGE",          &Proxy::command_forward_by_key_1},
  {"GETSET",            &Proxy::command_forward_by_key_1},
  {"HDEL",              &Proxy::command_forward_by_key_1},
  {"HEXISTS",           &Proxy::command_forward_by_key_1},
  {"HGET",              &Proxy::command_forward_by_key_1},
  {"HGETALL",           &Proxy::command_forward_by_key_1},
  {"HINCRBY",           &Proxy::command_forward_by_key_1},
  {"HINCRBYFLOAT",      &Proxy::command_forward_by_key_1},
  {"HKEYS",             &Proxy::command_forward_by_key_1},
  {"HLEN",              &Proxy::command_forward_by_key_1},
  {"HMGET",             &Proxy::command_forward_by_key_1},
  {"HMSET",             &Proxy::command_forward_by_key_1},
  {"HSCAN",             &Proxy::command_forward_by_key_1},
  {"HSET",              &Proxy::command_forward_by_key_1},
  {"HSETNX",            &Proxy::command_forward_by_key_1},
  {"HSTRLEN",           &Proxy::command_forward_by_key_1},
  {"HVALS",             &Proxy::command_forward_by_key_1},
  {"INCR",              &Proxy::command_forward_by_key_1},
  {"INCRBY",            &Proxy::command_forward_by_key_1},
  {"INCRBYFLOAT",       &Proxy::command_forward_by_key_1},
  {"INFO",              &Proxy::command_INFO},
  {"KEYS",              &Proxy::command_KEYS},
  {"LASTSAVE",          &Proxy::command_all_collect_responses},
  {"LATENCY",           &Proxy::command_LATENCY},
  {"LINDEX",            &Proxy::command_forward_by_key_1},
  {"LINSERT",           &Proxy::command_forward_by_key_1},
  {"LLEN",              &Proxy::command_forward_by_key_1},
  {"LOLWUT",            &Proxy::command_forward_random},
  {"LPOP",              &Proxy::command_forward_by_key_1},
  {"LPUSH",             &Proxy::command_forward_by_key_1},
  {"LPUSHX",            &Proxy::command_forward_by_key_1},
  {"LRANGE",            &Proxy::command_forward_by_key_1},
  {"LREM",              &Proxy::command_forward_by_key_1},
  {"LSET",              &Proxy::command_forward_by_key_1},
  {"LTRIM",             &Proxy::command_forward_by_key_1},
  {"MEMORY",            &Proxy::command_MEMORY},
  {"MGET",              &Proxy::command_partition_by_keys_1_multi},
  {"MIGRATE",           &Proxy::command_MIGRATE},
  {"MODULE",            &Proxy::command_MODULE},
  {"MSET",              &Proxy::command_partition_by_keys_2_status},
  {"MSETNX",            &Proxy::command_MSETNX},
  {"OBJECT",            &Proxy::command_OBJECT},
  {"PERSIST",           &Proxy::command_forward_by_key_1},
  {"PEXPIRE",           &Proxy::command_forward_by_key_1},
  {"PEXPIREAT",         &Proxy::command_forward_by_key_1},
  {"PFADD",             &Proxy::command_forward_by_key_1},
  {"PFCOUNT",           &Proxy::command_forward_by_keys_1_all},
  {"PFMERGE",           &Proxy::command_forward_by_keys_1_all},
  {"PING",              &Proxy::command_PING},
  {"PSETEX",            &Proxy::command_forward_by_key_1},
  {"PTTL",              &Proxy::command_forward_by_key_1},
  {"QUIT",              &Proxy::command_QUIT},
  {"RANDOMKEY",         &Proxy::command_forward_random},
  {"RENAME",            &Proxy::command_forward_by_keys_1_all},
  {"RENAMENX",          &Proxy::command_forward_by_keys_1_all},
  {"RESTORE",           &Proxy::command_forward_by_key_1},
  {"ROLE",              &Proxy::command_ROLE},
  {"RPOP",              &Proxy::command_forward_by_key_1},
  {"RPOPLPUSH",         &Proxy::command_forward_by_keys_1_all},
  {"RPUSH",             &Proxy::command_forward_by_key_1},
  {"RPUSHX",            &Proxy::command_forward_by_key_1},
  {"SADD",              &Proxy::command_forward_by_key_1},
  {"SAVE",              &Proxy::command_all_collect_status_responses},
  {"SCAN",              &Proxy::command_SCAN},
  {"SCARD",             &Proxy::command_forward_by_key_1},
  {"SCRIPT",            &Proxy::command_SCRIPT},
  {"SDIFF",             &Proxy::command_forward_by_keys_1_all},
  {"SDIFFSTORE",        &Proxy::command_forward_by_keys_1_all},
  {"SET",               &Proxy::command_forward_by_key_1},
  {"SETBIT",            &Proxy::command_forward_by_key_1},
  {"SETEX",             &Proxy::command_forward_by_key_1},
  {"SETNX",             &Proxy::command_forward_by_key_1},
  {"SETRANGE",          &Proxy::command_forward_by_key_1},
  {"SHUTDOWN",          &Proxy::command_all_collect_status_responses},
  {"SINTER",            &Proxy::command_forward_by_keys_1_all},
  {"SINTERSTORE",       &Proxy::command_forward_by_keys_1_all},
  {"SISMEMBER",         &Proxy::command_forward_by_key_1},
  {"SLOWLOG",           &Proxy::command_all_collect_responses},
  {"SMEMBERS",          &Proxy::command_forward_by_key_1},
  {"SMOVE",             &Proxy::command_forward_by_keys_1_2},
  {"SORT",              &Proxy::command_forward_by_key_1},
  {"SPOP",              &Proxy::command_forward_by_key_1},
  {"SRANDMEMBER",       &Proxy::command_forward_by_key_1},
  {"SREM",              &Proxy::command_forward_by_key_1},
  {"SSCAN",             &Proxy::command_forward_by_key_1},
  {"STRLEN",            &Proxy::command_forward_by_key_1},
  {"SUNION",            &Proxy::command_forward_by_keys_1_all},
  {"SUNIONSTORE",       &Proxy::command_forward_by_keys_1_all},
  {"TIME",              &Proxy::command_all_collect_responses},
  {"TOUCH",             &Proxy::command_partition_by_keys_1_integer},
  {"TTL",               &Proxy::command_forward_by_key_1},
  {"TYPE",              &Proxy::command_forward_by_key_1},
  {"UNLINK",            &Proxy::command_partition_by_keys_1_integer},
  {"XACK",              &Proxy::command_forward_by_key_1},
  {"XADD",              &Proxy::command_forward_by_key_1},
  {"XCLAIM",            &Proxy::command_forward_by_key_1},
  {"XDEL",              &Proxy::command_forward_by_key_1},
  {"XGROUP",            &Proxy::command_XGROUP},
  {"XINFO",             &Proxy::command_XINFO},
  {"XLEN",              &Proxy::command_forward_by_key_1},
  {"XPENDING",          &Proxy::command_forward_by_key_1},
  {"XRANGE",            &Proxy::command_forward_by_key_1},
  {"XREAD",             &Proxy::command_XREAD},
  {"XREADGROUP",        &Proxy::command_XREAD},
  {"XREVRANGE",         &Proxy::command_forward_by_key_1},
  {"XTRIM",             &Proxy::command_forward_by_key_1},
  {"ZADD",              &Proxy::command_forward_by_key_1},
  {"ZCARD",             &Proxy::command_forward_by_key_1},
  {"ZCOUNT",            &Proxy::command_forward_by_key_1},
  {"ZINCRBY",           &Proxy::command_forward_by_key_1},
  {"ZINTERSTORE",       &Proxy::command_ZACTIONSTORE},
  {"ZLEXCOUNT",         &Proxy::command_forward_by_key_1},
  {"ZPOPMAX",           &Proxy::command_forward_by_key_1},
  {"ZPOPMIN",           &Proxy::command_forward_by_key_1},
  {"ZRANGE",            &Proxy::command_forward_by_key_1},
  {"ZRANGEBYLEX",       &Proxy::command_forward_by_key_1},
  {"ZRANGEBYSCORE",     &Proxy::command_forward_by_key_1},
  {"ZRANK",             &Proxy::command_forward_by_key_1},
  {"ZREM",              &Proxy::command_forward_by_key_1},
  {"ZREMRANGEBYLEX",    &Proxy::command_forward_by_key_1},
  {"ZREMRANGEBYRANK",   &Proxy::command_forward_by_key_1},
  {"ZREMRANGEBYSCORE",  &Proxy::command_forward_by_key_1},
  {"ZREVRANGE",         &Proxy::command_forward_by_key_1},
  {"ZREVRANGEBYLEX",    &Proxy::command_forward_by_key_1},
  {"ZREVRANGEBYSCORE",  &Proxy::command_forward_by_key_1},
  {"ZREVRANK",          &Proxy::command_forward_by_key_1},
  {"ZSCAN",             &Proxy::command_forward_by_key_1},
  {"ZSCORE",            &Proxy::command_forward_by_key_1},
  {"ZUNIONSTORE",       &Proxy::command_ZACTIONSTORE},

  // commands that aren't part of the official protocol
  {"BACKEND",           &Proxy::command_BACKEND},
  {"BACKENDNUM",        &Proxy::command_BACKENDNUM},
  {"BACKENDS",          &Proxy::command_BACKENDS},
  {"FORWARD",           &Proxy::command_FORWARD},
  {"PRINTSTATE",        &Proxy::command_PRINTSTATE},
});
