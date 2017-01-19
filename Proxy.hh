#pragma once

#include <event2/event.h>
#include <event2/listener.h>

#include <deque>
#include <memory>
#include <phosg/ConsistentHashRing.hh>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "Protocol.hh"


struct ResponseLink;
struct Backend;


struct BackendConnection {
  Backend* backend;
  int64_t index;

  std::unique_ptr<struct bufferevent, void(*)(struct bufferevent*)> bev;
  ResponseParser parser;

  struct sockaddr_storage local_addr;
  struct sockaddr_storage remote_addr;

  size_t num_commands_sent;
  size_t num_responses_received;

  ResponseLink* head_link;
  ResponseLink* tail_link;

  BackendConnection(Backend* backend, int64_t index,
      std::unique_ptr<struct bufferevent, void(*)(struct bufferevent*)>&& bev);
  BackendConnection(const BackendConnection&) = delete;
  BackendConnection(BackendConnection&&) = delete;
  BackendConnection& operator=(const BackendConnection&) = delete;
  ~BackendConnection();

  struct evbuffer* get_output_buffer();

  void print(FILE* stream, int indent_level = 0) const;
};

struct Backend {
  size_t index;

  std::string host;
  int port;

  std::string name;
  std::string debug_name;

  std::unordered_map<int64_t, BackendConnection> index_to_connection;
  int64_t next_connection_index;

  size_t num_responses_received;
  size_t num_commands_sent;

  Backend(size_t index, const std::string& host, int port, const std::string& name);
  Backend(const Backend&) = delete;
  Backend(Backend&&) = delete;
  Backend& operator=(const Backend&) = delete;
  ~Backend() = default;

  BackendConnection& get_default_connection();

  void print(FILE* stream, int indent_level = 0) const;
};


struct Client {
  std::string name;
  bool should_disconnect;

  std::unique_ptr<struct bufferevent, void(*)(struct bufferevent*)> bev;
  CommandParser parser;

  struct sockaddr_storage local_addr;
  struct sockaddr_storage remote_addr;

  size_t num_commands_received;
  size_t num_responses_sent;

  ResponseLink* head_link;
  ResponseLink* tail_link;

  Client(std::unique_ptr<struct bufferevent, void(*)(struct bufferevent*)> bev);
  Client(const Client&) = delete;
  Client(Client&&) = delete;
  Client& operator=(const Client&) = delete;
  ~Client();

  struct evbuffer* get_output_buffer();

  void print(FILE* stream, int indent_level = 0) const;
};


// a ResponseLink represents a response that a client is expecting to receive,
// and also a promise that one or more backends will send a response that can be
// used to generate the response for the waiting client. each ResponseLink is
// linked in one or more lists representing its dependencies.
//
// each Client has a linked list of ResponseLinks representing the responses
// that the client expects to receive (in order). this list is traversed by
// following the next_client links in the ResponseLink.
//
// similarly, each BackendConnection has a linked list of ResponseLinks in the
// order that responses should be routed. this isn't necessarily the same as the
// next_client order. because a ResponseLink may represent an aggregation of
// multiple backend responses, the ResponseLink may exist in multiple
// BackendConnection lists. to traverse one of these lists, look up the given
// BackendConnection in the ResponseLink's backend_conn_to_next_link map.
//
// a ResponseLink is "ready" when all the needed backend responses have been
// received. this doesn't mean it can be sent to the client though - since the
// order of responses must be preserved, it can only be sent if it's first in
// the client's list. for example, if a client sends "GET x" and "GET y", and
// the backend for y responds first, we can't send the result yet. in this case,
// the ResponseLink stays ready until the backend for x responds. at that time,
// both ResponseLinks are ready, and are sent in the correct order.
//
// ResponseLinks are owned by the Client they're linked to, and are destroyed
// after the response is sent. if a client disconnects before receiving all of
// its pending responses, the client is unlinked from its ResponseLinks, but the
// ResponseLinks remain. this is necessary because there may be pipelined
// commands on the linked backend connections, and we need to discard the
// responses meant for this client. in this case, the ResponseLinks are owned by
// the BackendConnections, and are destroyed when they're unlinked from the last
// BackendConnection.
//
// if a BackendConnection disconnects early, then all of the ResponseLinks it's
// linked to receive an error response, and they're unlinked from the
// BackendConnection immediately. any ready ResponseLinks are processed (sent
// to the client, if possible) at this time also.

struct ResponseLink {
  enum CollectionType {
    ForwardResponse = 0,
    CollectStatusResponses,
    SumIntegerResponses,
    CombineMultiResponses,
    CollectResponses,
    CollectMultiResponsesByKey,
    CollectIdenticalResponses,
    ModifyScanResponse,
    ModifyScriptExistsResponse,
    ModifyMigrateResponse,
  };
  CollectionType type;

  static const char* name_for_collection_type(CollectionType type);

  Client* client;
  ResponseLink* next_client;
  std::unordered_map<BackendConnection*, ResponseLink*> backend_conn_to_next_link;

  std::shared_ptr<Response> error_response;

  // type-specific fields

  std::shared_ptr<Response> response_to_forward;

  int64_t response_integer_sum;

  Response::Type expected_response_type;
  std::vector<std::shared_ptr<Response>> responses;

  std::deque<int64_t> recombination_queue;
  std::unordered_map<int64_t, std::shared_ptr<Response>> backend_index_to_response;

  int64_t scan_backend_index;

  ResponseLink(CollectionType type, Client* c);
  ResponseLink(const ResponseLink&) = delete;
  ResponseLink(ResponseLink&&) = delete;
  ResponseLink& operator=(const ResponseLink&) = delete;
  ~ResponseLink();

  bool is_ready() const;

  void print(FILE* stream, int indent_level = 0) const;
};


class Proxy {
public:
  struct Netloc {
    std::string name;
    std::string host;
    int port;
  };

  Proxy(int listen_fd, const std::vector<ConsistentHashRing::Host>& hosts,
      int hash_begin_delimiter = -1, int hash_end_delimiter = -1);
  Proxy(const Proxy&) = delete;
  Proxy(Proxy&&) = delete;
  Proxy& operator=(const Proxy&) = delete;
  ~Proxy() = default;

  bool disable_command(const std::string& command_name);

  void serve();

  void print(FILE* stream, int indent_level = 0) const;

private:
  // network state
  int listen_fd;
  std::unique_ptr<struct event_base, void(*)(struct event_base*)> base;
  std::unique_ptr<struct evconnlistener, void(*)(struct evconnlistener*)> listener;

  // connection indexing and lookup
  ConsistentHashRing ring;
  std::unordered_map<int64_t, Backend> index_to_backend;
  std::unordered_map<std::string, Backend*> name_to_backend;
  std::unordered_map<struct bufferevent*, BackendConnection*> bev_to_backend_conn;
  std::unordered_map<struct bufferevent*, Client> bev_to_client;

  // stats
  size_t num_commands_received;
  size_t num_commands_sent;
  size_t num_responses_received;
  size_t num_responses_sent;
  size_t num_connections_received;
  uint64_t start_time;

  // hash configuration
  int hash_begin_delimiter;
  int hash_end_delimiter;

  // backend lookups
  int64_t backend_index_for_key(const std::string& s) const;
  int64_t backend_index_for_argument(const std::string& arg) const;
  Backend& backend_for_index(size_t index);
  Backend& backend_for_key(const std::string& s);
  BackendConnection& backend_conn_for_index(size_t index);
  BackendConnection& backend_conn_for_key(const std::string& s);

  // connection management
  void disconnect_client(Client* c);
  void disconnect_backend(BackendConnection* b);

  // response linking
  ResponseLink* create_link(ResponseLink::CollectionType type, Client* c);
  ResponseLink* create_error_link(Client* c, std::shared_ptr<Response> r);
  struct evbuffer* can_send_command(BackendConnection* conn, ResponseLink* l);
  void link_connection(BackendConnection* conn, ResponseLink* l);
  void send_command_and_link(BackendConnection* conn, ResponseLink* l,
      const DataCommand* cmd);
  void send_command_and_link(BackendConnection* conn, ResponseLink* l,
      const std::shared_ptr<const DataCommand>& cmd);
  void send_command_and_link(BackendConnection* conn, ResponseLink* l,
      const ReferenceCommand* cmd);

  // high-level output handlers
  void send_client_response(Client* c, const Response* r);
  void send_client_response(Client* c,
      const std::shared_ptr<const Response>& r);
  void send_client_string_response(Client* c, const char* s,
      Response::Type type);
  void send_client_string_response(Client* c, const std::string& s,
      Response::Type type);
  void send_client_string_response(Client* c, const void* data, size_t size,
      Response::Type type);
  void send_client_int_response(Client* c, int64_t int_value,
      Response::Type type);

  // high-level input handlers
  void send_ready_response(ResponseLink* l);
  void send_all_ready_responses(Client* c);
  void handle_backend_response(BackendConnection* conn,
      std::shared_ptr<Response> r);
  void handle_client_command(Client* c, std::shared_ptr<DataCommand> cmd);

  // low-level input handlers
  static void dispatch_on_client_input(struct bufferevent *bev, void* ctx);
  void on_client_input(struct bufferevent *bev);
  static void dispatch_on_client_error(struct bufferevent *bev, short events,
      void* ctx);
  void on_client_error(struct bufferevent *bev, short events);
  static void dispatch_on_backend_input(struct bufferevent *bev, void* ctx);
  void on_backend_input(struct bufferevent *bev);
  static void dispatch_on_backend_error(struct bufferevent *bev, short events,
      void* ctx);
  void on_backend_error(struct bufferevent *bev, short events);
  static void dispatch_on_listen_error(struct evconnlistener *listener,
      void* ctx);
  void on_listen_error(struct evconnlistener *listener);
  static void dispatch_on_client_accept(struct evconnlistener *listener,
      evutil_socket_t fd, struct sockaddr *address, int socklen, void* ctx);
  void on_client_accept(struct evconnlistener *listener, evutil_socket_t fd,
      struct sockaddr *address, int socklen);

  // generic command implementations
  void command_all_collect_responses(Client* c,
      std::shared_ptr<DataCommand> cmd);
  void command_all_collect_status_responses(Client* c,
      std::shared_ptr<DataCommand> cmd);
  void command_forward_all(Client* c, std::shared_ptr<DataCommand> cmd,
      ResponseLink::CollectionType type);
  void command_forward_by_key_1(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_forward_by_key_index(Client* c, std::shared_ptr<DataCommand> cmd,
      size_t key_index);
  void command_forward_by_keys(Client* c, std::shared_ptr<DataCommand> cmd,
      ssize_t start_key_index, ssize_t end_key_index);
  void command_forward_by_keys_1_all(Client* c,
      std::shared_ptr<DataCommand> cmd);
  void command_forward_by_keys_1_2(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_forward_by_keys_2_all(Client* c,
      std::shared_ptr<DataCommand> cmd);
  void command_forward_random(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_partition_by_keys(Client* c, std::shared_ptr<DataCommand> cmd,
      size_t start_arg_index, size_t args_per_key,
      ResponseLink::CollectionType type);
  void command_partition_by_keys_1_integer(Client* c,
      std::shared_ptr<DataCommand> cmd);
  void command_partition_by_keys_1_multi(Client* c,
      std::shared_ptr<DataCommand> cmd);
  void command_partition_by_keys_2_status(Client* c,
      std::shared_ptr<DataCommand> cmd);
  void command_unimplemented(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_default(Client* c, std::shared_ptr<DataCommand> cmd);

  // specific command implementations
  void command_BACKEND(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_BACKENDNUM(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_BACKENDS(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_CLIENT(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_DBSIZE(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_DEBUG(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_ECHO(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_EVAL(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_FORWARD(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_GEORADIUS(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_INFO(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_KEYS(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_MIGRATE(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_MSETNX(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_OBJECT(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_PING(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_PRINTSTATE(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_QUIT(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_ROLE(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_SCAN(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_SCRIPT(Client* c, std::shared_ptr<DataCommand> cmd);
  void command_ZACTIONSTORE(Client* c, std::shared_ptr<DataCommand> cmd);

  // handler index
  typedef void (Proxy::*command_handler)(Client* c,
      std::shared_ptr<DataCommand> cmd);
  std::unordered_map<std::string, command_handler> handlers;
  static const std::unordered_map<std::string, command_handler>
      default_handlers;
};
