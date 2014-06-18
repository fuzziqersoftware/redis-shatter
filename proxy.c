#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "debug.h"
#include "network.h"
#include "resource.h"
#include "protocol.h"
#include "backend.h"
#include "client.h"
#include "proxy.h"



////////////////////////////////////////////////////////////////////////////////
// forward declarations for event callbacks

static void proxy_on_client_input(struct bufferevent *bev, void *ctx);
static void proxy_on_client_error(struct bufferevent *bev, short events,
    void *ctx);
static void proxy_on_backend_input(struct bufferevent *bev, void *ctx);
static void proxy_on_backend_error(struct bufferevent *bev, short events,
    void *ctx);
static void proxy_on_listen_error(struct evconnlistener *listener, void *ctx);
static void proxy_on_client_accept(struct evconnlistener *listener,
    evutil_socket_t fd, struct sockaddr *address, int socklen, void *ctx);



////////////////////////////////////////////////////////////////////////////////
// backend helpers

static int parse_integer_field(const char* arg_data, int size, int64_t* value) {
  *value = 0;
  int x;
  for (x = 0; x < size; x++) {
    if (arg_data[x] < '0' || arg_data[x] > '9')
      return 0;
    *value = (*value * 10) + (arg_data[x] - '0');
  }
  return 1;
}

static int proxy_index_for_key_simple(const struct proxy* proxy,
    const void* key, int64_t size) {
  return ketama_server_for_key(proxy->ketama, key, size);
}

static int proxy_index_for_key_with_begin_delimiter(const struct proxy* proxy,
    const void* _key, int64_t size) {

  const char* key = (const char*)_key;
  const char* key_end = (const char*)key + size;
  const char* begin_delim = key;
  for (; (begin_delim < key_end) && (*begin_delim != proxy->hash_begin_delimiter); begin_delim++);

  if (begin_delim < key_end)
    return ketama_server_for_key(proxy->ketama, begin_delim + 1, key_end - begin_delim - 1);
  else
    return ketama_server_for_key(proxy->ketama, key, size);
}

static int proxy_index_for_key_with_end_delimiter(const struct proxy* proxy,
    const void* _key, int64_t size) {

  const char* key = (const char*)_key;
  const char* key_end = (const char*)key + size;
  const char* end_delim = key_end;
  for (end_delim--; (end_delim >= key) && (*end_delim != proxy->hash_end_delimiter); end_delim--);

  if (end_delim >= key)
    return ketama_server_for_key(proxy->ketama, key, end_delim - key);
  else
    return ketama_server_for_key(proxy->ketama, key, size);
}

static int proxy_index_for_key_with_delimiters(const struct proxy* proxy,
    const void* _key, int64_t size) {

  const char* key = (const char*)_key;
  const char* key_end = (const char*)key + size;
  const char* begin_delim = key;
  const char* end_delim = key_end;
  for (; (begin_delim < key_end) && (*begin_delim != proxy->hash_begin_delimiter); begin_delim++);
  for (end_delim--; (end_delim >= key) && (*end_delim != proxy->hash_end_delimiter); end_delim--);

  if (begin_delim < key_end && end_delim >= key && end_delim > begin_delim)
    return ketama_server_for_key(proxy->ketama, begin_delim + 1, end_delim - begin_delim - 1);
  else if (end_delim >= key)
    return ketama_server_for_key(proxy->ketama, key, end_delim - key);
  else if (begin_delim < key_end)
    return ketama_server_for_key(proxy->ketama, begin_delim + 1, key_end - begin_delim - 1);
  else
    return ketama_server_for_key(proxy->ketama, key, size);
}

void proxy_connect_backend(struct proxy* proxy, int index) {
  int fd = network_connect(proxy->backends[index]->host,
      proxy->backends[index]->port);
  if (fd < 0) {
    printf("error: can\'t connect to backend %s:%d (%d, %d)\n",
        proxy->backends[index]->host, proxy->backends[index]->port, fd, errno);
    return;
  }

  int fd_flags = fcntl(fd, F_GETFD, 0);
  if (fd_flags >= 0) {
    fd_flags |= FD_CLOEXEC;
    fcntl(fd, F_SETFD, fd_flags);
  }

  // set up a bufferevent for the new connection
  proxy->backends[index]->bev = bufferevent_socket_new(proxy->base, fd,
      BEV_OPT_CLOSE_ON_FREE);

  network_get_socket_addresses(fd, &proxy->backends[index]->local,
      &proxy->backends[index]->remote);

  bufferevent_setcb(proxy->backends[index]->bev, proxy_on_backend_input,
      NULL, proxy_on_backend_error, proxy->backends[index]);
  bufferevent_enable(proxy->backends[index]->bev, EV_READ | EV_WRITE);
}

static struct backend* proxy_backend_for_index(struct proxy* proxy, int index) {

  if (!proxy->backends[index]->bev)
    proxy_connect_backend(proxy, index);
  return proxy->backends[index];
}

static struct backend* proxy_backend_for_key(struct proxy* proxy, void* key,
    int64_t size) {
  return proxy_backend_for_index(proxy, proxy->index_for_key(proxy, key, size));
}

static int proxy_try_send_backend_command(struct backend* b,
    struct client_expected_response* e, struct command* cmd) {

  if (e->error_response)
    return 1;

  if (!b) {
    e->error_response = response_printf(e, RESPONSE_ERROR,
        "CHANNELERROR backend is missing");
    return 2;
  }

  struct evbuffer* out = backend_get_output_buffer(b);
  if (!out) {
    e->error_response = response_printf(e, RESPONSE_ERROR,
        "CHANNELERROR backend is not connected");
    return 3;
  }
  command_write(out, cmd);
  backend_add_waiting_client(b, e);

  struct proxy* proxy = (struct proxy*)e->client->ctx;
  b->num_commands_sent++;
  proxy->num_commands_sent++;
  e->num_responses_expected++;
  return 0;
}

static int proxy_send_client_response(struct client* c,
    const struct response* resp) {

  struct evbuffer* out = client_get_output_buffer(c);
  if (!out) {
    printf("warning: tried to send response to client with no output buffer\n");
    return 2;
  }
  response_write(out, resp);

  struct proxy* proxy = (struct proxy*)c->ctx;
  c->num_responses_sent++;
  proxy->num_responses_sent++;
  return 0;
}

static int proxy_send_client_string_response(struct client* c,
    const char* string, int response_type) {

  struct evbuffer* out = client_get_output_buffer(c);
  if (!out) {
    printf("warning: tried to send response to client with no output buffer\n");
    return 2;
  }
  response_write_string(out, string, response_type);

  struct proxy* proxy = (struct proxy*)c->ctx;
  c->num_responses_sent++;
  proxy->num_responses_sent++;
  return 0;
}

static int proxy_send_client_int_response(struct client* c, int64_t int_value,
    int response_type) {

  struct evbuffer* out = client_get_output_buffer(c);
  if (!out) {
    printf("warning: tried to send response to client with no output buffer\n");
    return 2;
  }
  response_write_int(out, int_value, response_type);

  struct proxy* proxy = (struct proxy*)c->ctx;
  c->num_responses_sent++;
  proxy->num_responses_sent++;
  return 0;
}



////////////////////////////////////////////////////////////////////////////////
// creation/deletion

static void proxy_delete(struct proxy* proxy) {
  if (proxy->listener)
    evconnlistener_free(proxy->listener);
  if (proxy->base)
    event_base_free(proxy->base);
  free(proxy);
}

struct proxy* proxy_create(void* resource_parent, int listen_fd,
    const char** netlocs, int num_backends, char hash_begin_delimiter,
    char hash_end_delimiter) {

  struct proxy* proxy = (struct proxy*)resource_calloc(
      resource_parent, sizeof(struct proxy), proxy_delete);
  if (!proxy) {
    resource_delete_ref(resource_parent, proxy);
    return NULL;
  }

  resource_annotate(proxy, "proxy[%d]", listen_fd);
  proxy->listen_fd = listen_fd;
  proxy->ketama = ketama_continuum_create(proxy, num_backends, netlocs);
  proxy->num_backends = num_backends;
  proxy->hash_begin_delimiter = hash_begin_delimiter;
  proxy->hash_end_delimiter = hash_end_delimiter;

  proxy->backends = (struct backend**)resource_calloc(proxy,
      sizeof(struct backend*) * num_backends, free);
  if (!proxy->backends) {
    resource_delete_ref(resource_parent, proxy);
    return NULL;
  }

  proxy->base = event_base_new();
  if (!proxy->base) {
    resource_delete_ref(resource_parent, proxy);
    return NULL;
  }

  evutil_make_socket_nonblocking(listen_fd);
  proxy->listener = evconnlistener_new(proxy->base,
      proxy_on_client_accept, proxy, LEV_OPT_REUSEABLE, 0, listen_fd);
  if (!proxy->listener) {
    resource_delete_ref(resource_parent, proxy);
    return NULL;
  }
  evconnlistener_set_error_cb(proxy->listener, proxy_on_listen_error);

  if (proxy->hash_begin_delimiter && proxy->hash_end_delimiter)
    proxy->index_for_key = proxy_index_for_key_with_delimiters;
  else if (proxy->hash_begin_delimiter)
    proxy->index_for_key = proxy_index_for_key_with_begin_delimiter;
  else if (proxy->hash_end_delimiter)
    proxy->index_for_key = proxy_index_for_key_with_end_delimiter;
  else
    proxy->index_for_key = proxy_index_for_key_simple;

  int x;
  for (x = 0; x < proxy->num_backends; x++) {
    proxy->backends[x] = backend_create(proxy, netlocs[x], 0);
    if (!proxy->backends[x])
      printf("proxy: can\'t add backend %s\n", netlocs[x]);
    else {
      proxy_connect_backend(proxy, x);
      proxy->backends[x]->ctx = proxy;
    }
  }

  return proxy;
}

void proxy_serve(struct proxy* proxy) {
  proxy->start_time = time(NULL);
  event_base_dispatch(proxy->base);

  // hack to make gcc not think proxy_print is dead code and delete it -
  // it's really useful to call when using gdb
  volatile int debug = 0;
  if (debug)
    proxy_print(proxy, 0);
}

void proxy_print(const struct proxy* proxy, int indent) {
  int x;
  if (indent < 0)
    indent = -indent;
  else
    print_indent(indent);

  if (!proxy) {
    printf("proxy@NULL");
    return;
  }

  printf("proxy@%p[base=%p, listener=%p, listen_fd=%d, ketama=%p, num_clients=%d, io_counts=[%d, %d, %d, %d], clients=[\n",
      proxy, proxy->base, proxy->listener, proxy->listen_fd, proxy->ketama, proxy->num_clients,
      proxy->num_commands_received, proxy->num_commands_sent, proxy->num_responses_received, proxy->num_responses_sent);

  struct client* c;
  for (c = proxy->client_chain_head; c; c = c->next) {
    printf("\n");
    client_print(c, indent + 1);
    printf(",");
  }

  printf("], backends=[");
  for (x = 0; x < proxy->num_backends; x++) {
    printf("\n");
    backend_print(proxy->backends[x], indent + 1);
    printf(",");
  }

  print_indent(indent);
  printf("]]");
}



////////////////////////////////////////////////////////////////////////////////
// response function implementations

void proxy_complete_response(struct client_expected_response* e) {

  if (e->error_response) {
    proxy_send_client_response(e->client, e->error_response);
    return;
  }

  switch (e->wait_type) {

    case CWAIT_FORWARD_RESPONSE:
      proxy_send_client_response(e->client, e->response_to_forward);
      break;

    case CWAIT_COLLECT_STATUS_RESPONSES:
      proxy_send_client_string_response(e->client, "OK", RESPONSE_STATUS);
      break;

    case CWAIT_SUM_INT_RESPONSES:
      proxy_send_client_int_response(e->client, e->int_sum, RESPONSE_INTEGER);
      break;

    case CWAIT_COMBINE_MULTI_RESPONSES: {
      int x, y, num_fields = 0;
      for (x = 0; x < e->collect_multi.num_responses; x++) {
        if (e->collect_multi.responses[x]->response_type != RESPONSE_MULTI) {
          proxy_send_client_string_response(e->client,
              "CHANNELERROR an upstream server returned a result of the wrong type", RESPONSE_ERROR);
          break;
        } else
          num_fields += e->collect_multi.responses[x]->multi_value.num_fields;
      }
      if (x < e->collect_multi.num_responses)
        break;

      struct response* resp = response_create(e, RESPONSE_MULTI, num_fields);
      resp->multi_value.num_fields = 0;
      for (x = 0; x < e->collect_multi.num_responses; x++) {
        struct response* current = e->collect_multi.responses[x];
        if (current->multi_value.num_fields <= 0)
          continue; // probably a null reply... redis doesn't give these but w/e
        for (y = 0; y < current->multi_value.num_fields; y++) {
          resp->multi_value.fields[resp->multi_value.num_fields] = current->multi_value.fields[y];
          resp->multi_value.num_fields++;
        }
      }
      proxy_send_client_response(e->client, resp);
      resource_delete_ref(e, resp);
      break; }

    case CWAIT_COLLECT_RESPONSES: {
      struct response* resp = response_create(e, RESPONSE_MULTI,
          e->collect_multi.num_responses);
      int x;
      for (x = 0; x < e->collect_multi.num_responses; x++) {
        struct response* current = e->collect_multi.responses[x];
        if (!current) {
          resp->multi_value.fields[x] = response_printf(resp, RESPONSE_ERROR,
              "CHANNELERROR an upstream server returned a bad response");
        } else {
          resp->multi_value.fields[x] = current;
          resource_add_ref(resp, current);
        }
      }
      proxy_send_client_response(e->client, resp);
      resource_delete_ref(e, resp);
      break; }

    case CWAIT_COLLECT_MULTI_RESPONSES_BY_KEY:
      proxy_send_client_response(e->client, e->collect_key.response_in_progress);
      break;

    case CWAIT_COLLECT_IDENTICAL_RESPONSES: {
      int x;
      for (x = 0; x < e->collect_multi.num_responses; x++) {
        if (!responses_equal(e->collect_multi.responses[x],
            e->collect_multi.responses[0])) {
          proxy_send_client_string_response(e->client,
              "CHANNELERROR backends did not return identical results", RESPONSE_ERROR);
          break;
        }
      }
      if (x < e->collect_multi.num_responses)
        break;

      proxy_send_client_response(e->client, e->collect_multi.responses[0]);
      break; }

    case CWAIT_MODIFY_SCAN_RESPONSE: {
      if (e->response_to_forward->response_type != RESPONSE_MULTI ||
          e->response_to_forward->multi_value.num_fields != 2 ||
          e->response_to_forward->multi_value.fields[0]->response_type != RESPONSE_DATA) {
        proxy_send_client_string_response(e->client,
            "CHANNELERROR backend returned wrong result type", RESPONSE_ERROR);
        break;
      }

      struct response* cursor = e->response_to_forward->multi_value.fields[0];
      if (cursor->data_value.size == 1 && *(char*)cursor->data_value.data == '0') {

        // this backend is done; go to the next one if any
        int64_t next_backend_id = e->scan_backend_num + 1;
        struct proxy* proxy = e->client->ctx;

        // if there are more backends, start the next one
        if (next_backend_id < proxy->num_backends) {
          e->response_to_forward->multi_value.fields[0] = response_printf(
              e->response_to_forward, RESPONSE_DATA, "%d:0", next_backend_id);
        }

      } else {
        // this backend isn't done yet
        e->response_to_forward->multi_value.fields[0] = response_printf(
            e->response_to_forward, RESPONSE_DATA, "%d:%.*s",
            e->scan_backend_num, cursor->data_value.size,
            cursor->data_value.data);
      }

      proxy_send_client_response(e->client, e->response_to_forward);
      break; }

    default:
      proxy_send_client_string_response(e->client,
          "PROXYERROR invalid wait type on response completion", RESPONSE_ERROR);
  }
}

void proxy_process_client_response_chain(struct client* c) {
  while (c->response_chain_head && (c->response_chain_head->num_responses_expected == 0)) {
    proxy_complete_response(c->response_chain_head);
    client_remove_expected_response(c);
  }
}

void proxy_handle_backend_response(struct proxy* proxy, struct backend* b,
    struct response* r) {

  struct client_expected_response* e = backend_get_waiting_client(b);
  if (!e) {
    printf("error: received response from backend with no waiting client\n");
    return;
  }
  e->num_responses_expected--;
  if (!e->error_response) {

    switch (e->wait_type) {

      case CWAIT_FORWARD_RESPONSE:
      case CWAIT_MODIFY_SCAN_RESPONSE:
        e->response_to_forward = r;
        resource_add_ref(e, r);
        break;

      case CWAIT_COLLECT_STATUS_RESPONSES:
        if (r->response_type != RESPONSE_STATUS)
          e->error_response = response_printf(e, RESPONSE_ERROR,
              "CHANNELERROR an upstream server returned a result of the wrong type");
        break;

      case CWAIT_SUM_INT_RESPONSES:
        if (r->response_type != RESPONSE_INTEGER)
          e->error_response = response_printf(e, RESPONSE_ERROR,
              "CHANNELERROR an upstream server returned a result of the wrong type");
        else
          e->int_sum += r->int_value;
        break;

      case CWAIT_COMBINE_MULTI_RESPONSES:
      case CWAIT_COLLECT_RESPONSES:
      case CWAIT_COLLECT_IDENTICAL_RESPONSES:
        e->collect_multi.responses[e->collect_multi.num_responses] = r;
        e->collect_multi.num_responses++;
        resource_add_ref(e, r);
        break;

      case CWAIT_COLLECT_MULTI_RESPONSES_BY_KEY: {

        if (!e->collect_key.response_in_progress)
          e->collect_key.response_in_progress = response_create(e,
              RESPONSE_MULTI, e->collect_key.num_keys);
        if (!e->collect_key.response_in_progress) {
          e->error_response = response_printf(e, RESPONSE_ERROR,
              "PROXYERROR can\'t allocate memory");
          break;
        }

        // TODO we should really do something faster than this
        int backend_id = -1;
        for (backend_id = 0; backend_id < proxy->num_backends; backend_id++)
          if (proxy->backends[backend_id] == b)
            break;

        if (!e->collect_key.index_to_command[backend_id]) {
          e->error_response = response_printf(e, RESPONSE_ERROR,
              "CHANNELERROR received a response from a server that was not sent a command");
          break;
        }

        if (r->response_type != RESPONSE_MULTI ||
            r->multi_value.num_fields != e->collect_key.index_to_command[backend_id]->num_args - 1) {
          e->error_response = response_printf(e, RESPONSE_ERROR,
              "CHANNELERROR the command gave a bad result on server %d", backend_id);
          break;
        }

        int y;
        for (y = 0; y < r->multi_value.num_fields; y++) {
          int response_field = e->collect_key.index_to_command[backend_id]->args[(y * e->collect_key.args_per_key) + 1].annotation;
          e->collect_key.response_in_progress->multi_value.fields[response_field] = r->multi_value.fields[y];
          resource_add_ref(e->collect_key.response_in_progress, r->multi_value.fields[y]);
        }
        break; }

      default:
        e->error_response = response_printf(e, RESPONSE_ERROR,
            "PROXYERROR unknown response wait type");
    }
  }

  proxy_process_client_response_chain(e->client);
}



////////////////////////////////////////////////////////////////////////////////
// proxy commands

void redis_command_BACKEND(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  if (cmd->num_args != 2) {
    proxy_send_client_string_response(c, "ERR wrong number of arguments",
        RESPONSE_ERROR);
    return;
  }

  struct backend* b = proxy_backend_for_key(proxy, cmd->args[1].data,
      cmd->args[1].size);

  struct response* resp = NULL;
  if (b)
    resp = response_printf(cmd, RESPONSE_DATA, "%s:%d", b->host, b->port);
  else
    resp = response_printf(cmd, RESPONSE_DATA, "NULL");
  proxy_send_client_response(c, resp);
  resource_delete_ref(cmd, resp);
}

void redis_command_BACKENDNUM(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  if (cmd->num_args != 2) {
    proxy_send_client_string_response(c, "ERR wrong number of arguments",
        RESPONSE_ERROR);
    return;
  }

  int backend_id = proxy->index_for_key(proxy, cmd->args[1].data,
      cmd->args[1].size);
  proxy_send_client_int_response(c, backend_id, RESPONSE_INTEGER);
}

void redis_command_BACKENDS(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  struct response* resp = response_create(cmd, RESPONSE_MULTI,
      proxy->num_backends);
  int x;
  for (x = 0; x < proxy->num_backends; x++) {
    if (proxy->backends[x])
      resp->multi_value.fields[x] = response_printf(resp, RESPONSE_DATA,
          "%s:%d", proxy->backends[x]->host, proxy->backends[x]->port);
    else
      resp->multi_value.fields[x] = response_printf(resp, RESPONSE_DATA,
          "NULL");
  }
  proxy_send_client_response(c, resp);
  resource_delete_ref(cmd, resp);
}

void redis_command_FORWARD(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  if (cmd->num_args < 3) {
    proxy_send_client_string_response(c, "ERR not enough arguments",
        RESPONSE_ERROR);
    return;
  }

  int64_t x, backend_id;
  if (!parse_integer_field(cmd->args[1].data, cmd->args[1].size, &backend_id) ||
      (backend_id < 0 || backend_id >= proxy->num_backends)) {
    proxy_send_client_string_response(c, "ERR backend id is invalid",
        RESPONSE_ERROR);
    return;
  }

  struct backend* b = proxy_backend_for_index(proxy, backend_id);
  if (!b) {
    proxy_send_client_string_response(c, "ERR backend does not exist",
        RESPONSE_ERROR);
    return;
  }

  struct command* backend_cmd = command_create(cmd, cmd->num_args - 2);
  backend_cmd->external_arg_data = 1;
  for (x = 2; x < cmd->num_args; x++) {
    backend_cmd->args[x - 2].data = cmd->args[x].data;
    backend_cmd->args[x - 2].size = cmd->args[x].size;
  }

  struct client_expected_response* e = client_expect_response(c,
      CWAIT_FORWARD_RESPONSE, backend_cmd, proxy->num_backends);
  proxy_try_send_backend_command(b, e, backend_cmd);
  resource_delete_ref(cmd, backend_cmd);
}

void redis_command_INFO(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  // INFO - return proxy info
  if (cmd->num_args == 1) {
    char hash_begin_delimiter_str[5], hash_end_delimiter_str[5];
    if (proxy->hash_begin_delimiter)
      sprintf(hash_begin_delimiter_str, "%c", proxy->hash_begin_delimiter);
    else
      sprintf(hash_begin_delimiter_str, "NULL");
    if (proxy->hash_end_delimiter)
      sprintf(hash_end_delimiter_str, "%c", proxy->hash_end_delimiter);
    else
      sprintf(hash_end_delimiter_str, "NULL");

    int64_t uptime = (time(NULL) - proxy->start_time);

    struct response* resp = response_printf(cmd, RESPONSE_DATA, "\
# Counters\n\
num_commands_received:%d\n\
num_commands_sent:%d\n\
num_responses_received:%d\n\
num_responses_sent:%d\n\
num_connections_received:%d\n\
num_clients:%d\n\
num_backends:%d\n\
\n\
# Timing\n\
start_time:%d\n\
uptime:%d\n\
\n\
# Memory\n\
resource_count:%d\n\
resource_refcount:%d\n\
\n\
# Configuration\n\
process_id:%d\n\
process_num:%d\n\
num_processes:%d\n\
hash_begin_delimiter:%s\n\
hash_end_delimiter:%s\n\
", proxy->num_commands_received, proxy->num_commands_sent,
        proxy->num_responses_received, proxy->num_responses_sent,
        proxy->num_connections_received, proxy->num_clients, proxy->num_backends,
        proxy->start_time, uptime, resource_count(), resource_refcount(),
        getpid(), proxy->process_num, proxy->num_processes,
        hash_begin_delimiter_str, hash_end_delimiter_str);
    proxy_send_client_response(c, resp);
    resource_delete_ref(cmd, resp);
    return;
  }

  // INFO BACKEND num - return proxy's info for backend num
  if ((cmd->num_args == 3) && (cmd->args[1].size == 7) &&
      !memcmp(cmd->args[1].data, "BACKEND", 7)) {
    int64_t backend_id;
    if (!parse_integer_field(cmd->args[2].data, cmd->args[2].size, &backend_id) ||
        (backend_id < 0 || backend_id >= proxy->num_backends)) {
      proxy_send_client_string_response(c, "ERR backend id is invalid",
          RESPONSE_ERROR);
      return;
    }

    struct backend* b = proxy_backend_for_index(proxy, backend_id);
    if (!b) {
      proxy_send_client_string_response(c, "ERR backend does not exist",
          RESPONSE_ERROR);
      return;
    }

    struct response* resp = response_printf(cmd, RESPONSE_DATA, "\
name:%s\n\
host:%s\n\
port:%d\n\
connected:%d\n\
num_commands_sent:%d\n\
num_responses_received:%d\n\
", b->name, b->host, b->port, (b->bev ? 1 : 0), b->num_commands_sent,
        b->num_responses_received);
    proxy_send_client_response(c, resp);
    resource_delete_ref(cmd, resp);
    return;
  }

  // INFO num - return info from backend server
  // INFO num section - return info from backend server

  int64_t x, backend_id;
  if (!parse_integer_field(cmd->args[1].data, cmd->args[1].size, &backend_id) ||
      (backend_id < 0 || backend_id >= proxy->num_backends)) {
    proxy_send_client_string_response(c, "ERR backend id is invalid",
        RESPONSE_ERROR);
    return;
  }

  struct command* backend_cmd = command_create(cmd, cmd->num_args - 1);
  backend_cmd->external_arg_data = 1;
  backend_cmd->args[0].data = cmd->args[0].data;
  backend_cmd->args[0].size = cmd->args[0].size;
  for (x = 2; x < cmd->num_args; x++) {
    backend_cmd->args[x - 1].data = cmd->args[x].data;
    backend_cmd->args[x - 1].size = cmd->args[x].size;
  }

  struct backend* b = proxy_backend_for_index(proxy, backend_id);
  struct client_expected_response* e = client_expect_response(c,
      CWAIT_FORWARD_RESPONSE, backend_cmd, proxy->num_backends);
  proxy_try_send_backend_command(b, e, backend_cmd);
  resource_delete_ref(cmd, backend_cmd);
}

void redis_command_PING(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  proxy_send_client_string_response(c, "PONG", RESPONSE_STATUS);
}

void redis_command_ECHO(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  if (cmd->num_args != 2) {
    proxy_send_client_string_response(c, "ERR wrong number of arguments",
        RESPONSE_ERROR);
    return;
  }

  struct response* resp = response_create(cmd, RESPONSE_DATA,
      cmd->args[1].size);
  if (!resp)
    proxy_send_client_string_response(c,
        "PROXYERROR can\'t allocate memory", RESPONSE_ERROR);
  else {
    memcpy(resp->data_value.data, cmd->args[1].data, cmd->args[1].size);
    proxy_send_client_response(c, resp);
    resource_delete_ref(cmd, resp);
  }
}



////////////////////////////////////////////////////////////////////////////////
// command function implementations

void redis_command_partition_by_keys(struct proxy* proxy, struct client* c,
    struct command* cmd, int args_per_key, int wait_type) {

  if ((cmd->num_args - 1) % args_per_key != 0) {
    proxy_send_client_string_response(c, "ERR not enough arguments",
        RESPONSE_ERROR);
    return;
  }

  struct client_expected_response* e = client_expect_response(c, wait_type, cmd,
      proxy->num_backends);

  e->collect_key.args_per_key = args_per_key;
  e->collect_key.num_keys = (cmd->num_args - 1) / e->collect_key.args_per_key;
  e->collect_key.server_to_key_count = (int*)resource_calloc_raw(e, sizeof(int) * proxy->num_backends, free);
  e->collect_key.key_to_server = (uint8_t*)resource_calloc_raw(e, sizeof(uint8_t) * e->collect_key.num_keys, free);
  e->collect_key.index_to_command = (struct command**)resource_calloc_raw(e, sizeof(struct command*) * proxy->num_backends, free);
  if (!e->collect_key.server_to_key_count || !e->collect_key.key_to_server || !e->collect_key.index_to_command) {
    e->error_response = response_printf(e, RESPONSE_ERROR, "PROXYERROR can\'t allocate memory");
    return;
  }

  int x, y;
  for (y = 0; y < e->collect_key.num_keys; y++) {
    int arg_index = (y * args_per_key) + 1;
    int server_index = proxy->index_for_key(proxy, cmd->args[arg_index].data, cmd->args[arg_index].size);
    e->collect_key.key_to_server[y] = server_index;
    e->collect_key.server_to_key_count[server_index]++;
  }

  for (x = 0; x < proxy->num_backends; x++) {
    if (e->collect_key.server_to_key_count[x] == 0)
      continue;
    e->collect_key.index_to_command[x] = command_create(e,
        (e->collect_key.server_to_key_count[x] * e->collect_key.args_per_key) + 1);
    e->collect_key.index_to_command[x]->external_arg_data = 1;
    e->collect_key.index_to_command[x]->num_args = 1;
    e->collect_key.index_to_command[x]->args[0].data = cmd->args[0].data; // same command as the original
    e->collect_key.index_to_command[x]->args[0].size = cmd->args[0].size; // same command as the original
  }

  for (y = 0; y < e->collect_key.num_keys; y++) {
    int server = e->collect_key.key_to_server[y];
    struct command* server_cmd = e->collect_key.index_to_command[server];
    int src_arg_index = (y * e->collect_key.args_per_key) + 1;
    for (x = 0; x < e->collect_key.args_per_key; x++) {
      server_cmd->args[server_cmd->num_args + x].data = cmd->args[src_arg_index + x].data;
      server_cmd->args[server_cmd->num_args + x].size = cmd->args[src_arg_index + x].size;
    }
    server_cmd->args[server_cmd->num_args].annotation = y;
    server_cmd->num_args += e->collect_key.args_per_key;
  }

  for (x = 0; x < proxy->num_backends; x++) {
    if (!e->collect_key.index_to_command[x])
      continue;
    proxy_try_send_backend_command(proxy_backend_for_index(proxy, x), e,
        e->collect_key.index_to_command[x]);
  }
}

void redis_command_partition_by_keys_1_multi(struct proxy* proxy,
    struct client* c, struct command* cmd) {
  redis_command_partition_by_keys(proxy, c, cmd, 1, CWAIT_COLLECT_MULTI_RESPONSES_BY_KEY);
}

void redis_command_partition_by_keys_1_integer(struct proxy* proxy,
    struct client* c, struct command* cmd) {
  redis_command_partition_by_keys(proxy, c, cmd, 1, CWAIT_SUM_INT_RESPONSES);
}

void redis_command_partition_by_keys_2_status(struct proxy* proxy,
    struct client* c, struct command* cmd) {
  redis_command_partition_by_keys(proxy, c, cmd, 2, CWAIT_COLLECT_STATUS_RESPONSES);
}


void redis_command_forward_by_key_index(struct proxy* proxy,
    struct client* c, struct command* cmd, int key_index) {

  if (key_index >= cmd->num_args)
    proxy_send_client_string_response(c, "ERR not enough arguments",
        RESPONSE_ERROR);
  else {
    struct backend* b = proxy_backend_for_key(proxy, cmd->args[key_index].data, cmd->args[key_index].size);
    struct client_expected_response* e = client_expect_response(c,
        CWAIT_FORWARD_RESPONSE, cmd, proxy->num_backends);
    proxy_try_send_backend_command(b, e, cmd);
  }
}

void redis_command_forward_by_key1(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  redis_command_forward_by_key_index(proxy, c, cmd, 1);
}


void redis_command_forward_by_keys(struct proxy* proxy, struct client* c,
    struct command* cmd, int start_key_index, int end_key_index) {

  if (cmd->num_args <= start_key_index) {
    proxy_send_client_string_response(c, "ERR not enough arguments",
        RESPONSE_ERROR);
    return;
  }

  if (end_key_index < 0 || end_key_index > cmd->num_args)
    end_key_index = cmd->num_args;

  // check that the keys all hash to the same server
  int backend_id = proxy->index_for_key(proxy, cmd->args[start_key_index].data,
      cmd->args[start_key_index].size);
  int x;
  for (x = start_key_index + 1; x < end_key_index; x++) {
    if (proxy->index_for_key(proxy, cmd->args[x].data, cmd->args[x].size)
        != backend_id) {
      proxy_send_client_string_response(c,
          "PROXYERROR keys are on different backends", RESPONSE_ERROR);
      return;
    }
  }

  struct backend* b = proxy_backend_for_index(proxy, backend_id);
  struct client_expected_response* e = client_expect_response(c,
      CWAIT_FORWARD_RESPONSE, cmd, proxy->num_backends);
  proxy_try_send_backend_command(b, e, cmd);
}

void redis_command_forward_by_keys_1(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  redis_command_forward_by_keys(proxy, c, cmd, 1, -1);
}

void redis_command_forward_by_keys_1_2(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  redis_command_forward_by_keys(proxy, c, cmd, 1, 3);
}

void redis_command_forward_by_keys_2(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  redis_command_forward_by_keys(proxy, c, cmd, 2, -1);
}

void redis_command_ZACTIONSTORE(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  // this is basically the same as command_forward_by_keys except the number of
  // checked keys is given in arg 2

  if (cmd->num_args <= 3) {
    proxy_send_client_string_response(c, "ERR not enough arguments",
        RESPONSE_ERROR);
    return;
  }

  int64_t x, num_keys = 0;
  if (!parse_integer_field(cmd->args[2].data, cmd->args[2].size, &num_keys) ||
      (num_keys < 1 || num_keys > cmd->num_args - 3)) {
    proxy_send_client_string_response(c, "ERR key count is invalid",
        RESPONSE_ERROR);
    return;
  }

  // check that the keys all hash to the same server
  int backend_id = proxy->index_for_key(proxy, cmd->args[1].data,
      cmd->args[1].size);
  for (x = 0; x < num_keys; x++) {
    if (proxy->index_for_key(proxy, cmd->args[3 + x].data, cmd->args[3 + x].size)
        != backend_id) {
      proxy_send_client_string_response(c,
          "PROXYERROR keys are on different backends", RESPONSE_ERROR);
      return;
    }
  }

  struct backend* b = proxy_backend_for_index(proxy, backend_id);
  struct client_expected_response* e = client_expect_response(c,
      CWAIT_FORWARD_RESPONSE, cmd, proxy->num_backends);
  proxy_try_send_backend_command(b, e, cmd);
}


void redis_command_forward_all(struct proxy* proxy, struct client* c,
    struct command* cmd, int wait_type) {

  struct client_expected_response* e = client_expect_response(c, wait_type, cmd,
      proxy->num_backends);

  int x;
  for (x = 0; x < proxy->num_backends; x++) {

    struct backend* b = proxy_backend_for_index(proxy, x);
    proxy_try_send_backend_command(b, e, cmd);
  }
}

void redis_command_all_collect_responses(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  redis_command_forward_all(proxy, c, cmd, CWAIT_COLLECT_RESPONSES);
}


void redis_command_EVAL(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  if (cmd->num_args < 3) {
    proxy_send_client_string_response(c, "ERR not enough arguments",
        RESPONSE_ERROR);
    return;
  }

  int64_t x, num_keys = 0;
  if (!parse_integer_field(cmd->args[2].data, cmd->args[2].size, &num_keys) ||
      (num_keys < 1 || num_keys > cmd->num_args - 3)) {
    proxy_send_client_string_response(c, "ERR key count is invalid",
        RESPONSE_ERROR);
    return;
  }

  // check that the keys all hash to the same server
  int backend_id = proxy->index_for_key(proxy, cmd->args[3].data,
      cmd->args[3].size);
  for (x = 1; x < num_keys; x++) {
    if (proxy->index_for_key(proxy, cmd->args[x + 3].data, cmd->args[x + 3].size)
        != backend_id) {
      proxy_send_client_string_response(c,
          "PROXYERROR keys are on different backends", RESPONSE_ERROR);
      return;
    }
  }

  struct backend* b = proxy_backend_for_index(proxy, backend_id);
  struct client_expected_response* e = client_expect_response(c,
      CWAIT_FORWARD_RESPONSE, cmd, proxy->num_backends);
  proxy_try_send_backend_command(b, e, cmd);
}

void redis_command_SCRIPT(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  // subcommands:
  // EXISTS - not supported (we can't know which server to send the command to)
  // FLUSH - forward to all backends, aggregate responses
  // KILL - not supported
  // LOAD <script> - forward to all backends, aggregate responses

  if (cmd->num_args < 2) {
    proxy_send_client_string_response(c, "ERR not enough arguments",
        RESPONSE_ERROR);
    return;
  }

  if (cmd->args[1].size == 5 && !memcmp(cmd->args[1].data, "FLUSH", 5))
    redis_command_forward_all(proxy, c, cmd, CWAIT_COLLECT_STATUS_RESPONSES);
  else if (cmd->args[1].size == 4 && !memcmp(cmd->args[1].data, "LOAD", 4))
    redis_command_forward_all(proxy, c, cmd, CWAIT_COLLECT_IDENTICAL_RESPONSES);
  else if (cmd->args[1].size == 6 && !memcmp(cmd->args[1].data, "EXISTS", 6))
    redis_command_forward_all(proxy, c, cmd, CWAIT_COLLECT_RESPONSES);
  else {
    proxy_send_client_string_response(c,
        "PROXYERROR unsupported subcommand", RESPONSE_ERROR);
    return;
  }
}

void redis_command_SCAN(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  if (cmd->num_args < 2) {
    proxy_send_client_string_response(c,
        "ERR not enough arguments", RESPONSE_ERROR);
    return;
  }

  // if cursor is "0" then we're starting a new scan on the first backend
  if (cmd->args[1].size == 1 && *(char*)cmd->args[1].data == '0') {
    struct client_expected_response* e = client_expect_response(c,
        CWAIT_MODIFY_SCAN_RESPONSE, cmd, 1);
    e->scan_backend_num = 0;
    proxy_try_send_backend_command(proxy->backends[0], e, cmd);
    return;
  }

  // cursor is formatted as "backend:cursor"
  int x;
  for (x = 0; x < cmd->args[1].size; x++)
    if (((char*)cmd->args[1].data)[x] == ':')
      break;
  if (x == cmd->args[1].size) {
    proxy_send_client_string_response(c,
        "PROXYERROR cursor format is incorrect", RESPONSE_ERROR);
    return;
  }

  // parse backend_num and cursor_contents
  int64_t backend_id = 0;
  if (!parse_integer_field(cmd->args[1].data, x, &backend_id) ||
      (backend_id < 0 || backend_id >= proxy->num_backends)) {
    proxy_send_client_string_response(c,
        "ERR cursor refers to a nonexistent backend", RESPONSE_ERROR);
    return;
  }

  // create backend command
  struct command* backend_cmd = command_create(cmd, cmd->num_args);
  backend_cmd->external_arg_data = 1;
  for (x = 0; x < cmd->num_args; x++) {
    backend_cmd->args[x].data = cmd->args[x].data;
    backend_cmd->args[x].size = cmd->args[x].size;
  }
  backend_cmd->args[1].data = (void*)((char*)cmd->args[1].data + x);
  backend_cmd->args[1].size = cmd->args[1].size - x;

  // send command
  struct client_expected_response* e = client_expect_response(c,
      CWAIT_MODIFY_SCAN_RESPONSE, backend_cmd, 1);
  e->scan_backend_num = backend_id;
  proxy_try_send_backend_command(proxy->backends[backend_id], e, backend_cmd);
  resource_delete_ref(cmd, backend_cmd);
}

void redis_command_KEYS(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  // CAVEAT EMPTOR: this may double-list incorrectly-distributed keys
  redis_command_forward_all(proxy, c, cmd, CWAIT_COMBINE_MULTI_RESPONSES);
}

void redis_command_DBSIZE(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  // CAVEAT EMPTOR: keys may exist on single redis instances that don't hash to
  // that instance; they will be counted too
  redis_command_forward_all(proxy, c, cmd, CWAIT_SUM_INT_RESPONSES);
}

void redis_command_OBJECT(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  if (cmd->num_args < 3)
    proxy_send_client_string_response(c, "ERR not enough arguments",
        RESPONSE_ERROR);
  else if (cmd->args[1].size != 8 || (
      memcmp(cmd->args[1].data, "REFCOUNT", 8) &&
      memcmp(cmd->args[1].data, "ENCODING", 8) &&
      memcmp(cmd->args[1].data, "IDLETIME", 8)))
    proxy_send_client_string_response(c,
        "PROXYERROR unsupported subcommand", RESPONSE_ERROR);
  else
    redis_command_forward_by_key_index(proxy, c, cmd, 2);
}

void redis_command_MIGRATE(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  if (cmd->num_args < 6)
    proxy_send_client_string_response(c, "ERR not enough arguments",
        RESPONSE_ERROR);
  else
    redis_command_forward_by_key_index(proxy, c, cmd, 3);
}

void redis_command_DEBUG(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  if (cmd->num_args < 3)
    proxy_send_client_string_response(c, "ERR not enough arguments",
        RESPONSE_ERROR);
  else if (cmd->args[1].size != 6 || memcmp(cmd->args[1].data, "OBJECT", 6))
    proxy_send_client_string_response(c,
        "PROXYERROR unsupported subcommand", RESPONSE_ERROR);
  else
    redis_command_forward_by_key_index(proxy, c, cmd, 2);
}

void redis_command_RANDOMKEY(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  struct backend* b = proxy_backend_for_index(proxy, rand() % proxy->num_backends);
  struct client_expected_response* e = client_expect_response(c,
      CWAIT_FORWARD_RESPONSE, cmd, proxy->num_backends);
  proxy_try_send_backend_command(b, e, cmd);
}

void redis_command_CLIENT(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  if (cmd->num_args < 2) {
    proxy_send_client_string_response(c, "ERR not enough arguments",
        RESPONSE_ERROR);
    return;
  }

  if (cmd->args[1].size == 4 && !memcmp(cmd->args[1].data, "LIST", 4)) {
    struct client* this_c;
    int num_clients = 0;
    for (this_c = proxy->client_chain_head; this_c; this_c = this_c->next)
      num_clients++;

    struct response* r = response_create(c, RESPONSE_DATA, num_clients * 128);
    int r_offset = 0;
    for (this_c = proxy->client_chain_head; this_c; this_c = this_c->next) {

      int response_chain_length = 0;
      struct client_expected_response* e;
      for (e = this_c->response_chain_head; e; e = e->next_response)
        response_chain_length++;

      r_offset += sprintf((char*)&r->data_value.data[r_offset],
          "addr=%s:%d name=\"%s\" parser=%d cmdrecv=%d rspsent=%d rspchain=%d\n",
          inet_ntoa(this_c->remote.sin_addr), ntohs(this_c->remote.sin_port),
          this_c->name, this_c->parser ? 1 : 0, this_c->num_commands_received,
          this_c->num_responses_sent, response_chain_length);
    }

    r->data_value.size = r_offset;
    proxy_send_client_response(c, r);
    resource_delete_ref(c, r);

  } else if (cmd->args[1].size == 7 && !memcmp(cmd->args[1].data, "SETNAME", 7)) {
    if (cmd->num_args != 3) {
      proxy_send_client_string_response(c, "ERR not enough arguments",
          RESPONSE_ERROR);
      return;
    }
    if (cmd->args[2].size >= CLIENT_NAME_LENGTH) {
      proxy_send_client_string_response(c, "ERR name is too long",
          RESPONSE_ERROR);
      return;
    }
    memcpy(c->name, cmd->args[2].data, cmd->args[2].size);
    c->name[cmd->args[2].size] = 0;
    proxy_send_client_string_response(c, "OK", RESPONSE_STATUS);

  } else if (cmd->args[1].size == 7 && !memcmp(cmd->args[1].data, "GETNAME", 7)) {
    struct response* r = response_printf(c, RESPONSE_DATA, "%s", c->name);
    proxy_send_client_response(c, r);
    resource_delete_ref(c, r);

  } else
    proxy_send_client_string_response(c, "ERR unknown subcommand",
        RESPONSE_ERROR);
}

void redis_command_unimplemented(struct proxy* proxy, struct client* c,
    struct command* cmd) {
  proxy_send_client_string_response(c, "PROXYERROR command not supported",
      RESPONSE_ERROR);
}

// called when we have no idea what is going on
void redis_command_default(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  printf("UNKNOWN COMMAND: ");
  command_print(cmd, -1);
  printf("\n");
  proxy_send_client_string_response(c, "PROXYERROR unknown command",
      RESPONSE_ERROR);
}



////////////////////////////////////////////////////////////////////////////////
// command table & lookup functions

typedef void (*command_handler)(struct proxy* proxy, struct client* c,
    struct command* cmd);

struct {
  const char* command_str;
  command_handler handler;
} command_definitions[] = {

  // commands that are unimplemented
  {"AUTH",              redis_command_unimplemented}, // password - Authenticate to the server
  {"BLPOP",             redis_command_unimplemented}, // key [key ...] timeout - Remove and get the first element in a list, or block until one is available
  {"BRPOP",             redis_command_unimplemented}, // key [key ...] timeout - Remove and get the last element in a list, or block until one is available
  {"BRPOPLPUSH",        redis_command_unimplemented}, // source destination timeout - Pop a value from a list, push it to another list and return it; or block until one is available
  {"DISCARD",           redis_command_unimplemented}, // - Discard all commands issued after MULTI
  {"EXEC",              redis_command_unimplemented}, // - Execute all commands issued after MULTI
  {"MONITOR",           redis_command_unimplemented}, // - Listen for all requests received by the server in real time
  {"MOVE",              redis_command_unimplemented}, // key db - Move a key to another database
  {"MSETNX",            redis_command_unimplemented}, // key value [key value ...] - Set multiple keys to multiple values, only if none of the keys exist
  {"MULTI",             redis_command_unimplemented}, // - Mark the start of a transaction block
  {"PSUBSCRIBE",        redis_command_unimplemented}, // pattern [pattern ...] - Listen for messages published to channels matching the given patterns
  {"PUBSUB",            redis_command_unimplemented}, // subcommand [argument [argument ...]] - Inspect the state of the Pub/Sub subsystem
  {"PUBLISH",           redis_command_unimplemented}, // channel message - Post a message to a channel
  {"PUNSUBSCRIBE",      redis_command_unimplemented}, // [pattern [pattern ...]] - Stop listening for messages posted to channels matching the given patterns
  {"QUIT",              redis_command_unimplemented}, // - Close the connection
  {"SELECT",            redis_command_unimplemented}, // index - Change the selected database for the current connection
  {"SHUTDOWN",          redis_command_unimplemented}, // [NOSAVE] [SAVE] - Synchronously save the dataset to disk and then shut down the server
  {"SLAVEOF",           redis_command_unimplemented}, // host port - Make the server a slave of another instance, or promote it as master
  {"SUBSCRIBE",         redis_command_unimplemented}, // channel [channel ...] - Listen for messages published to the given channels
  {"SYNC",              redis_command_unimplemented}, // - Internal command used for replication
  {"UNSUBSCRIBE",       redis_command_unimplemented}, // [channel [channel ...]] - Stop listening for messages posted to the given channels
  {"UNWATCH",           redis_command_unimplemented}, // - Forget about all watched keys
  {"WATCH",             redis_command_unimplemented}, // key [key ...] - Watch the given keys to determine execution of the MULTI/EXEC block

  // commands that are implemented
  {"APPEND",            redis_command_forward_by_key1},             // key value - Append a value to a key
  {"BGREWRITEAOF",      redis_command_all_collect_responses},       // - Asynchronously rewrite the append-only file
  {"BGSAVE",            redis_command_all_collect_responses},       // - Asynchronously save db to disk
  {"BITCOUNT",          redis_command_forward_by_key1},             // key [start] [end] - Count set bits in a string
  {"BITOP",             redis_command_forward_by_keys_2},           // operation destkey key [key ...] - Perform bitwise operations between strings
  {"BITPOS",            redis_command_forward_by_key1},             // key bit [start] [end] - Return the position of the first bit set to 1 or 0 in a string
  {"CLIENT",            redis_command_CLIENT},                      // KILL ip:port / LIST / GETNAME / SETNAME name
  {"CONFIG",            redis_command_all_collect_responses},       // GET parameter / REWRITE / SET param value / RESETSTAT
  {"DBSIZE",            redis_command_DBSIZE},                      // - Return the number of keys in the selected database
  {"DEBUG",             redis_command_DEBUG},                       // OBJECT key - Get debugging information about a key
  {"DECR",              redis_command_forward_by_key1},             // key - Decrement the integer value of a key by one
  {"DECRBY",            redis_command_forward_by_key1},             // key decrement - Decrement the integer value of a key by the given number
  {"DEL",               redis_command_partition_by_keys_1_integer}, // key [key ...] - Delete a key
  {"DUMP",              redis_command_forward_by_key1},             // key - Return a serialized version of the value stored at the specified key.
  {"ECHO",              redis_command_ECHO},                        // message - Echo the given string
  {"EVAL",              redis_command_EVAL},                        // script numkeys key [key ...] arg [arg ...] - Execute a Lua script server side
  {"EVALSHA",           redis_command_EVAL},                        // sha1 numkeys key [key ...] arg [arg ...] - Execute a Lua script server side
  {"EXISTS",            redis_command_forward_by_key1},             // key - Determine if a key exists
  {"EXPIRE",            redis_command_forward_by_key1},             // key seconds - Set a keys time to live in seconds
  {"EXPIREAT",          redis_command_forward_by_key1},             // key timestamp - Set the expiration for a key as a UNIX timestamp
  {"FLUSHALL",          redis_command_all_collect_responses},       // - Remove all keys from all databases
  {"FLUSHDB",           redis_command_all_collect_responses},       // - Remove all keys from the current database
  {"GET",               redis_command_forward_by_key1},             // key - Get the value of a key
  {"GETBIT",            redis_command_forward_by_key1},             // key offset - Returns the bit value at offset in the string value stored at key
  {"GETRANGE",          redis_command_forward_by_key1},             // key start end - Get a substring of the string stored at a key
  {"GETSET",            redis_command_forward_by_key1},             // key value - Set the string value of a key and return its old value
  {"HDEL",              redis_command_forward_by_key1},             // key field [field ...] - Delete one or more hash fields
  {"HEXISTS",           redis_command_forward_by_key1},             // key field - Determine if a hash field exists
  {"HGET",              redis_command_forward_by_key1},             // key field - Get the value of a hash field
  {"HGETALL",           redis_command_forward_by_key1},             // key - Get all the fields and values in a hash
  {"HINCRBY",           redis_command_forward_by_key1},             // key field increment - Increment the integer value of a hash field by the given number
  {"HINCRBYFLOAT",      redis_command_forward_by_key1},             // key field increment - Increment the float value of a hash field by the given amount
  {"HKEYS",             redis_command_forward_by_key1},             // key - Get all the fields in a hash
  {"HLEN",              redis_command_forward_by_key1},             // key - Get the number of fields in a hash
  {"HMGET",             redis_command_forward_by_key1},             // key field [field ...] - Get the values of all the given hash fields
  {"HMSET",             redis_command_forward_by_key1},             // key field value [field value ...] - Set multiple hash fields to multiple values
  {"HSCAN",             redis_command_forward_by_key1},             // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate hash fields and associated values
  {"HSET",              redis_command_forward_by_key1},             // key field value - Set the string value of a hash field
  {"HSETNX",            redis_command_forward_by_key1},             // key field value - Set the value of a hash field, only if the field does not exist
  {"HVALS",             redis_command_forward_by_key1},             // key - Get all the values in a hash
  {"INCR",              redis_command_forward_by_key1},             // key - Increment the integer value of a key by one
  {"INCRBY",            redis_command_forward_by_key1},             // key increment - Increment the integer value of a key by the given amount
  {"INCRBYFLOAT",       redis_command_forward_by_key1},             // key increment - Increment the float value of a key by the given amount
  {"INFO",              redis_command_INFO},                        // [backendnum] [section] - Get information and statistics about the server
  {"KEYS",              redis_command_KEYS},                        // pattern - Find all keys matching the given pattern
  {"LASTSAVE",          redis_command_all_collect_responses},       // - Get the UNIX time stamp of the last successful save to disk  
  {"LINDEX",            redis_command_forward_by_key1},             // key index - Get an element from a list by its index
  {"LINSERT",           redis_command_forward_by_key1},             // key BEFORE|AFTER pivot value - Insert an element before or after another element in a list
  {"LLEN",              redis_command_forward_by_key1},             // key - Get the length of a list
  {"LPOP",              redis_command_forward_by_key1},             // key - Remove and get the first element in a list
  {"LPUSH",             redis_command_forward_by_key1},             // key value [value ...] - Prepend one or multiple values to a list
  {"LPUSHX",            redis_command_forward_by_key1},             // key value - Prepend a value to a list, only if the list exists
  {"LRANGE",            redis_command_forward_by_key1},             // key start stop - Get a range of elements from a list
  {"LREM",              redis_command_forward_by_key1},             // key count value - Remove elements from a list
  {"LSET",              redis_command_forward_by_key1},             // key index value - Set the value of an element in a list by its index
  {"LTRIM",             redis_command_forward_by_key1},             // key start stop - Trim a list to the specified range
  {"MGET",              redis_command_partition_by_keys_1_multi},   // key [key ...] - Get the values of all the given keys
  {"MIGRATE",           redis_command_MIGRATE},                     // host port key destination-db timeout [COPY] [REPLACE] - Atomically transfer a key from a Redis instance to another one.
  {"MSET",              redis_command_partition_by_keys_2_status},  // key value [key value ...] - Set multiple keys to multiple values
  {"OBJECT",            redis_command_OBJECT},                      // subcommand [arguments [arguments ...]] - Inspect the internals of Redis objects
  {"PERSIST",           redis_command_forward_by_key1},             // key - Remove the expiration from a key
  {"PEXPIRE",           redis_command_forward_by_key1},             // key milliseconds - Set a keys time to live in milliseconds
  {"PEXPIREAT",         redis_command_forward_by_key1},             // key milliseconds-timestamp - Set the expiration for a key as a UNIX timestamp specified in milliseconds
  {"PFADD",             redis_command_forward_by_key1},             // key element [element ...] - Add all elements to HyperLogLog
  {"PFCOUNT",           redis_command_forward_by_keys_1},           // key [key ...] - Count unique elements in HyperLogLogs
  {"PFMERGE",           redis_command_forward_by_keys_1},           // key [key ...] - Merge HyperLogLogs into one
  {"PING",              redis_command_PING},                        // - Ping the server
  {"PSETEX",            redis_command_forward_by_key1},             // key milliseconds value - Set the value and expiration in milliseconds of a key
  {"PTTL",              redis_command_forward_by_key1},             // key - Get the time to live for a key in milliseconds
  {"RANDOMKEY",         redis_command_RANDOMKEY},                   // - Return a random key from the keyspace
  {"RENAME",            redis_command_forward_by_keys_1},           // key newkey - Rename a key
  {"RENAMENX",          redis_command_forward_by_keys_1},           // key newkey - Rename a key, only if the new key does not exist
  {"RESTORE",           redis_command_forward_by_key1},             // key ttl serialized-value - Create a key using the provided serialized value, previously obtained using DUMP.
  {"RPOP",              redis_command_forward_by_key1},             // key - Remove and get the last element in a list
  {"RPOPLPUSH",         redis_command_forward_by_keys_1},           // source destination - Remove the last element in a list, append it to another list and return it
  {"RPUSH",             redis_command_forward_by_key1},             // key value [value ...] - Append one or multiple values to a list
  {"RPUSHX",            redis_command_forward_by_key1},             // key value - Append a value to a list, only if the list exists
  {"SADD",              redis_command_forward_by_key1},             // key member [member ...] - Add one or more members to a set
  {"SAVE",              redis_command_all_collect_responses},       // - Synchronously save the dataset to disk
  {"SCAN",              redis_command_SCAN},                        // cursor [MATCH pattern] [COUNT count] - Incrementally iterate the keys space
  {"SCARD",             redis_command_forward_by_key1},             // key - Get the number of members in a set
  {"SCRIPT",            redis_command_SCRIPT},                      // KILL / EXISTS name / FLUSH / LOAD data - Kill the script currently in execution.
  {"SDIFF",             redis_command_forward_by_keys_1},           // key [key ...] - Subtract multiple sets
  {"SDIFFSTORE",        redis_command_forward_by_keys_1},           // destination key [key ...] - Subtract multiple sets and store the resulting set in a key
  {"SET",               redis_command_forward_by_key1},             // key value [EX seconds] [PX milliseconds] [NX|XX] - Set the string value of a key
  {"SETBIT",            redis_command_forward_by_key1},             // key offset value - Sets or clears the bit at offset in the string value stored at key
  {"SETEX",             redis_command_forward_by_key1},             // key seconds value - Set the value and expiration of a key
  {"SETNX",             redis_command_forward_by_key1},             // key value - Set the value of a key, only if the key does not exist
  {"SETRANGE",          redis_command_forward_by_key1},             // key offset value - Overwrite part of a string at key starting at the specified offset
  {"SINTER",            redis_command_forward_by_keys_1},           // key [key ...] - Intersect multiple sets
  {"SINTERSTORE",       redis_command_forward_by_keys_1},           // destination key [key ...] - Intersect multiple sets and store the resulting set in a key
  {"SISMEMBER",         redis_command_forward_by_key1},             // key member - Determine if a given value is a member of a set
  {"SLOWLOG",           redis_command_all_collect_responses},       // GET [n] / LEN / RESET
  {"SMEMBERS",          redis_command_forward_by_key1},             // key - Get all the members in a set
  {"SMOVE",             redis_command_forward_by_keys_1_2},         // source destination member - Move a member from one set to another
  {"SORT",              redis_command_forward_by_key1},             // key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination] - Sort the elements in a list, set or sorted set
  {"SPOP",              redis_command_forward_by_key1},             // key - Remove and return a random member from a set
  {"SRANDMEMBER",       redis_command_forward_by_key1},             // key [count] - Get one or multiple random members from a set
  {"SREM",              redis_command_forward_by_key1},             // key member [member ...] - Remove one or more members from a set
  {"SSCAN",             redis_command_forward_by_key1},             // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate Set elements
  {"STRLEN",            redis_command_forward_by_key1},             // key - Get the length of the value stored in a key
  {"SUNION",            redis_command_forward_by_keys_1},           // key [key ...] - Add multiple sets
  {"SUNIONSTORE",       redis_command_forward_by_keys_1},           // destination key [key ...] - Add multiple sets and store the resulting set in a key
  {"TIME",              redis_command_all_collect_responses},       // - Return the current server time
  {"TTL",               redis_command_forward_by_key1},             // key - Get the time to live for a key
  {"TYPE",              redis_command_forward_by_key1},             // key - Determine the type stored at key
  {"ZADD",              redis_command_forward_by_key1},             // key score member [score member ...] - Add one or more members to a sorted set, or update its score if it already exists
  {"ZCARD",             redis_command_forward_by_key1},             // key - Get the number of members in a sorted set
  {"ZCOUNT",            redis_command_forward_by_key1},             // key min max - Count the members in a sorted set with scores within the given values
  {"ZINCRBY",           redis_command_forward_by_key1},             // key increment member - Increment the score of a member in a sorted set
  {"ZINTERSTORE",       redis_command_ZACTIONSTORE},                // destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] - Intersect multiple sorted sets and store the resulting sorted set in a new key
  {"ZLEXCOUNT",         redis_command_forward_by_key1},             // key min max - Count the members in a sorted set with scores within the given values
  {"ZRANGE",            redis_command_forward_by_key1},             // key start stop [WITHSCORES] - Return a range of members in a sorted set, by index
  {"ZRANGEBYLEX",       redis_command_forward_by_key1},             // key min max [LIMIT offset count] - Return a range of members in a sorted set, by lex
  {"ZRANGEBYSCORE",     redis_command_forward_by_key1},             // key min max [WITHSCORES] [LIMIT offset count] - Return a range of members in a sorted set, by score
  {"ZRANK",             redis_command_forward_by_key1},             // key member - Determine the index of a member in a sorted set
  {"ZREM",              redis_command_forward_by_key1},             // key member [member ...] - Remove one or more members from a sorted set
  {"ZREMRANGEBYLEX",    redis_command_forward_by_key1},             // key min max - Remove all members in a sorted set within the given lex range
  {"ZREMRANGEBYRANK",   redis_command_forward_by_key1},             // key start stop - Remove all members in a sorted set within the given indexes
  {"ZREMRANGEBYSCORE",  redis_command_forward_by_key1},             // key min max - Remove all members in a sorted set within the given scores
  {"ZREVRANGE",         redis_command_forward_by_key1},             // key start stop [WITHSCORES] - Return a range of members in a sorted set, by index, with scores ordered from high to low
  {"ZREVRANGEBYSCORE",  redis_command_forward_by_key1},             // key max min [WITHSCORES] [LIMIT offset count] - Return a range of members in a sorted set, by score, with scores ordered from high to low
  {"ZREVRANK",          redis_command_forward_by_key1},             // key member - Determine the index of a member in a sorted set, with scores ordered from high to low
  {"ZSCAN",             redis_command_forward_by_key1},             // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate sorted sets elements and associated scores
  {"ZSCORE",            redis_command_forward_by_key1},             // key member - Get the score associated with the given member in a sorted set
  {"ZUNIONSTORE",       redis_command_ZACTIONSTORE},                // destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] - Add multiple sorted sets and store the resulting sorted set in a new key

  // commands that aren't part of the official protocol
  {"BACKEND",           redis_command_BACKEND},    // key - Get the backend number that the given key hashes to
  {"BACKENDNUM",        redis_command_BACKENDNUM}, // key - Get the backend number that the given key hashes to
  {"BACKENDS",          redis_command_BACKENDS},   // - Get the list of all backend netlocs
  {"FORWARD",           redis_command_FORWARD},    // backendnum command [args] - forward the given command directly to the given backend

  {NULL, NULL}, // end marker
};

#define MAX_COMMANDS_PER_HASH_BUCKET 8

static int8_t hash_to_num_commands[256];
static int16_t hash_to_command_indexes[256][MAX_COMMANDS_PER_HASH_BUCKET];

uint8_t hash_command(const char* cmd, int len) {
  uint8_t hash = 0;
  const char* end;
  for (end = cmd + len; cmd < end; cmd++)
    hash += toupper(*cmd);
  return hash;
}

int build_command_definitions() {
  memset(hash_to_command_indexes, 0, sizeof(hash_to_command_indexes));
  memset(hash_to_num_commands, 0, sizeof(hash_to_num_commands));

  int x;
  for (x = 0; command_definitions[x].command_str; x++) {
    if (!command_definitions[x].handler)
      continue;
    uint8_t hash = hash_command(command_definitions[x].command_str, strlen(command_definitions[x].command_str));
    if (hash_to_num_commands[hash] >= MAX_COMMANDS_PER_HASH_BUCKET) {
      printf("error: too many commands in bucket %02X\n", hash);
      return -1;
    } else {
      hash_to_command_indexes[hash][hash_to_num_commands[hash]] = x;
      hash_to_num_commands[hash]++;
    }
  }

#ifdef DEBUG_COMMAND_COLLISIONS
  int y;
  for (x = 0; x < 256; x++) {
    if (hash_to_num_commands[x] > 1) {
      printf("%02X has %d entries:", x, hash_to_num_commands[x]);
      for (y = 0; y < hash_to_num_commands[x]; y++)
        printf(" %s", command_definitions[hash_to_command_indexes[x][y]].command_str);
      printf("\n");
    }
  }
#endif

  return 0;
}

command_handler handler_for_command(const char* command, int cmdlen) {

  uint8_t hash = hash_command(command, cmdlen);

  int x;
  for (x = 0; x < hash_to_num_commands[hash]; x++)
    if (!strncmp(command, command_definitions[hash_to_command_indexes[hash][x]].command_str, cmdlen))
      return command_definitions[hash_to_command_indexes[hash][x]].handler;
  return redis_command_default;
}


////////////////////////////////////////////////////////////////////////////////
// server functions

void proxy_handle_client_command(struct proxy* proxy, struct client* c,
    struct command* cmd) {

  if (cmd->num_args <= 0) {
    proxy_send_client_string_response(c, "ERR invalid command", RESPONSE_ERROR);
    return;
  }

  int x;
  char* arg0_str = (char*)cmd->args[0].data;
  for (x = 0; x < cmd->args[0].size; x++)
    arg0_str[x] = toupper(arg0_str[x]);

  handler_for_command(arg0_str, cmd->args[0].size)(proxy, c, cmd);

  // need to check for complete responses here because the command handler can
  // add an error item on the queue
  proxy_process_client_response_chain(c);
}


////////////////////////////////////////////////////////////////////////////////
// event callbacks

static void proxy_on_client_input(struct bufferevent *bev, void *ctx) {
  struct client* c = (struct client*)ctx;
  struct evbuffer* in_buffer = bufferevent_get_input(bev);

  if (!c->parser)
    c->parser = command_parser_create(c);

  struct command* cmd;
  while ((cmd = command_parser_continue(c, c->parser, in_buffer))) {
    struct proxy* proxy = (struct proxy*)c->ctx;
    c->num_commands_received++;
    proxy->num_commands_received++;
    proxy_handle_client_command(proxy, c, cmd);
    resource_delete_ref(c, cmd);
  }
  if (c->parser->error)
    printf("warning: parse error in client stream %s (%d)\n", c->name, c->parser->error);
}

static void proxy_on_client_error(struct bufferevent *bev, short events,
    void *ctx) {
  struct client* c = (struct client*)ctx;
  struct proxy* proxy = (struct proxy*)c->ctx;

  if (events & BEV_EVENT_ERROR) {
    int err = EVUTIL_SOCKET_ERROR();
    printf("error: client %s gave %d (%s)\n", c->name, err,
        evutil_socket_error_to_string(err));
  }
  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    if (c->next)
      c->next->prev = c->prev;
    else
      proxy->client_chain_tail = c->prev;
    if (c->prev)
      c->prev->next = c->next;
    else
      proxy->client_chain_head = c->next;
    proxy->num_clients--;

    bufferevent_free(c->bev);
    c->bev = NULL;

    resource_delete_ref(proxy, c);
  }
}

static void proxy_on_backend_input(struct bufferevent *bev, void *ctx) {
  struct backend* b = (struct backend*)ctx;
  struct evbuffer* in_buffer = bufferevent_get_input(bev);
  struct proxy* proxy = (struct proxy*)b->ctx;

  if (!b->parser)
    b->parser = response_parser_create(b);

  int can_continue = 1;
  while (can_continue && !b->parser->error) {

    // if the head of the queue is a forwarding client, then use the forwarding
    // parser (don't allocate a response object)
    struct client_expected_response* e = backend_peek_waiting_client(b);
    if (e && (e->wait_type == CWAIT_FORWARD_RESPONSE)) {
      struct evbuffer* out_buffer = client_get_output_buffer(e->client);
      can_continue = response_parser_continue_forward(b->parser,
          in_buffer, out_buffer);
      if (can_continue) {
        b->num_responses_received++;
        proxy->num_responses_received++;
        e->client->num_responses_sent++;
        proxy->num_responses_sent++;

        // a full response was forwarded; delete the wait object
        backend_get_waiting_client(b);
        client_remove_expected_response(e->client);
      }

    } else {
      struct response* rsp = response_parser_continue(b, b->parser,
          in_buffer);
      can_continue = !!rsp;
      if (rsp) {
        b->num_responses_received++;
        proxy->num_responses_received++;
        proxy_handle_backend_response(proxy, b, rsp);
        resource_delete_ref(b, rsp);
      }
    }
  }

  if (b->parser->error) {
    printf("warning: parse error in backend stream %s (%d); disconnecting backend\n", b->name, b->parser->error);
    proxy_on_backend_error(bev, BEV_EVENT_EOF, b);
  }
}

static void proxy_on_backend_error(struct bufferevent *bev, short events,
    void *ctx) {
  struct backend* b = (struct backend*)ctx;

  if (events & BEV_EVENT_ERROR) {
    int err = EVUTIL_SOCKET_ERROR();
    printf("error: backend %s gave %d (%s)\n", b->name, err,
        evutil_socket_error_to_string(err));
  }
  if (events & BEV_EVENT_EOF) {
    printf("warning: backend %s has disconnected\n", b->name);
  }
  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    bufferevent_free(bev); // this closes the socket
    b->bev = NULL;

    // delete the parser, in case it was in the middle of something
    if (b->parser)
      resource_delete_ref(b, b->parser);
    b->parser = NULL;

    // issue a fake error response to all waiting clients
    if (b->wait_chain_head) {
      struct response* error_response = response_printf(b,
          RESPONSE_ERROR, "CHANNELERROR backend disconnected before sending the response");

      struct client_expected_response* e;
      while ((e = backend_get_waiting_client(b)))
        proxy_handle_backend_response((struct proxy*)b->ctx, b,
            error_response);
      resource_delete_ref(b, error_response);
    }
  }
}

static void proxy_on_listen_error(struct evconnlistener *listener,
    void *ctx) {
  struct proxy* proxy = (struct proxy*)ctx;

  int err = EVUTIL_SOCKET_ERROR();
  printf("error: server socket failed with code %d (%s)\n", err,
      evutil_socket_error_to_string(err));

  event_base_loopexit(proxy->base, NULL);
}

static void proxy_on_client_accept(struct evconnlistener *listener,
    evutil_socket_t fd, struct sockaddr *address, int socklen, void *ctx) {

  struct proxy* proxy = (struct proxy*)ctx;

  int fd_flags = fcntl(fd, F_GETFD, 0);
  if (fd_flags >= 0) {
    fd_flags |= FD_CLOEXEC;
    fcntl(fd, F_SETFD, fd_flags);
  }

  // set up a bufferevent for the new connection
  struct bufferevent *bev = bufferevent_socket_new(proxy->base, fd,
      BEV_OPT_CLOSE_ON_FREE);

  // allocate a struct for this connection
  struct client* c = client_create(proxy, bev);
  if (!c) {
    printf("error: server is out of memory; can\'t allocate a client\n");
    bufferevent_free(bev);
    return;
  }

  c->ctx = proxy;
  network_get_socket_addresses(fd, &c->local, &c->remote);
  sprintf(c->name, "%s:%d (%d)", inet_ntoa(c->remote.sin_addr), ntohs(c->remote.sin_port), fd);

  if (proxy->client_chain_head) {
    c->prev = proxy->client_chain_tail;
    proxy->client_chain_tail->next = c;
    proxy->client_chain_tail = c;
  } else {
    proxy->client_chain_head = c;
    proxy->client_chain_tail = c;
  }
  proxy->num_clients++;
  proxy->num_connections_received++;

  bufferevent_setcb(c->bev, proxy_on_client_input, NULL,
      proxy_on_client_error, c);
  bufferevent_enable(c->bev, EV_READ | EV_WRITE);
}
