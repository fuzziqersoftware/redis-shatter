#include <ctype.h>
#include <errno.h>
#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "debug.h"
#include "network.h"
#include "resource.h"
#include "redis_protocol.h"
#include "redis_backend.h"
#include "redis_client.h"
#include "redis_proxy.h"



////////////////////////////////////////////////////////////////////////////////
// forward declarations for event callbacks

static void redis_proxy_on_client_input(struct bufferevent *bev, void *ctx);
static void redis_proxy_on_client_error(struct bufferevent *bev, short events,
    void *ctx);
static void redis_proxy_on_backend_input(struct bufferevent *bev, void *ctx);
static void redis_proxy_on_backend_error(struct bufferevent *bev, short events,
    void *ctx);
static void redis_proxy_on_listen_error(struct evconnlistener *listener,
    void *ctx);
static void redis_proxy_on_client_accept(struct evconnlistener *listener,
    evutil_socket_t fd, struct sockaddr *address, int socklen, void *ctx);



////////////////////////////////////////////////////////////////////////////////
// backend helpers

static inline int redis_index_for_key(struct redis_proxy* proxy, void* key,
    int64_t size) {
  return ketama_server_for_key(proxy->ketama, key, size);
}

static struct redis_backend* redis_backend_for_index(struct redis_proxy* proxy,
    int index) {

  if (!proxy->backends[index]->bev) {
    int fd = network_connect(proxy->backends[index]->host,
        proxy->backends[index]->port);
    if (fd < 0) {
      printf("error: can\'t connect to backend %s:%d (%d, %d)\n",
          proxy->backends[index]->host, proxy->backends[index]->port, fd, errno);
      return proxy->backends[index];
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

    bufferevent_setcb(proxy->backends[index]->bev, redis_proxy_on_backend_input,
        NULL, redis_proxy_on_backend_error, proxy->backends[index]);
    bufferevent_enable(proxy->backends[index]->bev, EV_READ | EV_WRITE);
  }

  return proxy->backends[index];
}

struct redis_backend* redis_backend_for_key(struct redis_proxy* proxy,
    void* key, int64_t size) {
  return redis_backend_for_index(proxy, redis_index_for_key(proxy, key, size));
}

int redis_proxy_try_send_backend_command(struct redis_backend* b,
    struct redis_client_expected_response* e, struct redis_command* cmd) {

  if (!b) {
    e->error_response = redis_response_printf(e, RESPONSE_ERROR,
        "CHANNELERROR backend is missing");
    return 1;
  }

  struct evbuffer* out = redis_backend_get_output_buffer(b);
  if (!out) {
    e->error_response = redis_response_printf(e, RESPONSE_ERROR,
        "CHANNELERROR backend is not connected");
    return 2;
  }
  redis_write_command(out, cmd);
  redis_backend_add_waiting_client(b, e);
  e->num_responses_expected++;

  return 0;
}



////////////////////////////////////////////////////////////////////////////////
// creation/deletion

static void redis_proxy_delete(struct redis_proxy* proxy) {
  if (proxy->listener)
    evconnlistener_free(proxy->listener);
  if (proxy->base)
    event_base_free(proxy->base);
  free(proxy);
}

struct redis_proxy* redis_proxy_create(void* resource_parent, int listen_fd,
    const char** netlocs, int num_backends) {

  struct redis_proxy* proxy = (struct redis_proxy*)malloc(
      sizeof(struct redis_proxy));
  if (!proxy)
    goto redis_proxy_create_error;

  proxy->backends = (struct redis_backend**)malloc(sizeof(struct redis_backend*) * num_backends);
  if (!proxy->backends)
    goto redis_proxy_create_error;

  proxy->base = event_base_new();
  if (!proxy->base)
    goto redis_proxy_create_error;

  evutil_make_socket_nonblocking(listen_fd);
  proxy->listener = evconnlistener_new(proxy->base,
      redis_proxy_on_client_accept, proxy, LEV_OPT_REUSEABLE, 0, listen_fd);
  if (!proxy->listener)
    goto redis_proxy_create_error;
  evconnlistener_set_error_cb(proxy->listener, redis_proxy_on_listen_error);

  resource_create(resource_parent, proxy, redis_proxy_delete);
  resource_annotate(proxy, "redis_proxy[%d]", listen_fd);
  proxy->listen_fd = listen_fd;

  proxy->ketama = ketama_continuum_create(proxy, num_backends, netlocs);
  proxy->num_backends = num_backends;

  int x;
  for (x = 0; x < proxy->num_backends; x++) {
    proxy->backends[x] = redis_backend_create(proxy, netlocs[x], 0);
    if (!proxy->backends[x])
      printf("proxy: can\'t add backend %s\n", netlocs[x]);
    else {
      printf("proxy: added backend %s\n", netlocs[x]);
      proxy->backends[x]->ctx = proxy;
    }
  }

  if (0) {
redis_proxy_create_error:
    if (proxy) {
      if (proxy->backends)
        free(proxy->backends);
      if (proxy->listener)
        evconnlistener_free(proxy->listener);
      if (proxy->base)
        event_base_free(proxy->base);
      free(proxy);
    }
    return NULL;
  }

  return proxy;
}

void redis_proxy_serve(struct redis_proxy* proxy) {
  event_base_dispatch(proxy->base);
}



////////////////////////////////////////////////////////////////////////////////
// response function implementations

void redis_proxy_complete_response(struct redis_client_expected_response* e) {

  if (!e->error_response) {
    switch (e->wait_type) {

      case CWAIT_FORWARD_RESPONSE:
        redis_write_response(redis_client_get_output_buffer(e->client),
            e->response_to_forward);
        break;

      case CWAIT_COLLECT_STATUS_RESPONSES:
        redis_write_string_response(redis_client_get_output_buffer(e->client),
            "OK", RESPONSE_STATUS);
        break;

      case CWAIT_SUM_INT_RESPONSES:
        redis_write_int_response(redis_client_get_output_buffer(e->client),
            e->int_sum, RESPONSE_INTEGER);
        break;

      case CWAIT_COMBINE_MULTI_RESPONSES: {
        int x, y, num_fields = 0;
        for (x = 0; x < e->collect_multi.num_responses; x++) {
          if (e->collect_multi.responses[x]->response_type != RESPONSE_MULTI)
            e->error_response = redis_response_printf(e, RESPONSE_ERROR,
                "CHANNELERROR an upstream server returned a result of the wrong type");
          else
            num_fields += e->collect_multi.responses[x]->multi_value.num_fields;
        }

        if (e->error_response)
          break;

        struct redis_response* resp = redis_response_create(e, RESPONSE_MULTI,
            num_fields);
        resp->multi_value.num_fields = 0;
        for (x = 0; x < e->collect_multi.num_responses; x++) {
          struct redis_response* current = e->collect_multi.responses[x];
          if (current->multi_value.num_fields <= 0)
            continue; // probably a null reply... redis doesn't give these but w/e
          for (y = 0; y < current->multi_value.num_fields; y++) {
            resp->multi_value.fields[resp->multi_value.num_fields] = current->multi_value.fields[y];
            resp->multi_value.num_fields++;
          }
        }
        redis_write_response(redis_client_get_output_buffer(e->client), resp);
        resource_delete_ref(e, resp);
        break; }

      case CWAIT_COLLECT_RESPONSES: {
        struct redis_response* resp = redis_response_create(e, RESPONSE_MULTI,
            e->collect_multi.num_responses);
        int x;
        for (x = 0; x < e->collect_multi.num_responses; x++) {
          struct redis_response* current = e->collect_multi.responses[x];
          if (!current) {
            resp->multi_value.fields[x] = redis_response_printf(resp, RESPONSE_ERROR, "CHANNELERROR upstream server returned a bad response");
          } else {
            resp->multi_value.fields[x] = current;
            resource_add_ref(resp, current);
          }
        }
        redis_write_response(redis_client_get_output_buffer(e->client), resp);
        resource_delete_ref(e, resp);
        break; }

      case CWAIT_COLLECT_MULTI_RESPONSES_BY_KEY:
        redis_write_response(redis_client_get_output_buffer(e->client),
            e->collect_key.response_in_progress);
        break;

      default:
        redis_write_string_response(redis_client_get_output_buffer(e->client),
            "ERR invalid wait type on response completion", RESPONSE_ERROR);
    }
  }

  if (e->error_response)
    redis_write_response(redis_client_get_output_buffer(e->client),
        e->error_response);
}

void redis_proxy_handle_backend_response(struct redis_proxy* proxy,
    struct redis_backend* b, struct redis_response* r) {

  struct redis_client_expected_response* e = redis_backend_get_waiting_client(b);
  if (!e) {
    printf("error: received response from backend with no waiting client\n");
    return;
  }
  e->num_responses_expected--;
  if (!e->error_response) {

    switch (e->wait_type) {

      case CWAIT_FORWARD_RESPONSE:
        e->response_to_forward = r;
        resource_add_ref(e, r);
        break;

      case CWAIT_COLLECT_STATUS_RESPONSES:
        if (r->response_type != RESPONSE_STATUS)
          e->error_response = redis_response_printf(e, RESPONSE_ERROR,
              "CHANNELERROR an upstream server returned a result of the wrong type");
        break;

      case CWAIT_SUM_INT_RESPONSES:
        if (r->response_type != RESPONSE_INTEGER)
          e->error_response = redis_response_printf(e, RESPONSE_ERROR,
              "CHANNELERROR an upstream server returned a result of the wrong type");
        else
          e->int_sum += r->int_value;
        break;

      case CWAIT_COMBINE_MULTI_RESPONSES:
      case CWAIT_COLLECT_RESPONSES:
        e->collect_multi.responses[e->collect_multi.num_responses] = r;
        e->collect_multi.num_responses++;
        resource_add_ref(e, r);
        break;

      case CWAIT_COLLECT_MULTI_RESPONSES_BY_KEY: {

        if (!e->collect_key.response_in_progress)
          e->collect_key.response_in_progress = redis_response_create(e,
              RESPONSE_MULTI, e->collect_key.num_keys);
        if (!e->collect_key.response_in_progress) {
          e->error_response = redis_response_printf(e, RESPONSE_ERROR,
              "ERR can\'t allocate memory");
          break;
        }

        // TODO we should really do something faster than this
        int backend_id = -1;
        for (backend_id = 0; backend_id < proxy->num_backends; backend_id++)
          if (proxy->backends[backend_id] == b)
            break;

        if (!e->collect_key.index_to_command[backend_id]) {
          e->error_response = redis_response_printf(e, RESPONSE_ERROR,
              "ERR received a response from a server that was not sent a command");
          break;
        }

        if (r->response_type != RESPONSE_MULTI ||
            r->multi_value.num_fields != e->collect_key.index_to_command[backend_id]->num_args - 1) {
          e->error_response = redis_response_printf(e, RESPONSE_ERROR,
              "CHANNELERROR command gave bad result on server %d", backend_id);
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
        e->error_response = redis_response_printf(e, RESPONSE_ERROR,
            "ERR unknown response wait type");
    }
  }

  if (e->num_responses_expected == 0) {
    redis_proxy_complete_response(e);
    redis_client_remove_expected_response(e->client);
  }
}



////////////////////////////////////////////////////////////////////////////////
// command function implementations (NULL = unimplemented)

#define redis_command_AUTH          NULL

#define redis_command_BLPOP         NULL
#define redis_command_BRPOP         NULL
#define redis_command_BRPOPLPUSH    NULL
#define redis_command_CLIENT        NULL
#define redis_command_CONFIG        NULL
#define redis_command_FLUSHDB       NULL
#define redis_command_MONITOR       NULL
#define redis_command_MOVE          NULL
#define redis_command_MSETNX        NULL
#define redis_command_PSUBSCRIBE    NULL
#define redis_command_PUBSUB        NULL
#define redis_command_PUBLISH       NULL
#define redis_command_PUNSUBSCRIBE  NULL
#define redis_command_SAVE          NULL
#define redis_command_SELECT        NULL
#define redis_command_SHUTDOWN      NULL
#define redis_command_SLOWLOG       NULL
#define redis_command_SUBSCRIBE     NULL
#define redis_command_UNSUBSCRIBE   NULL
#define redis_command_UNWATCH       NULL
#define redis_command_WATCH         NULL

#define redis_command_BITOP         NULL
#define redis_command_DISCARD       NULL
#define redis_command_EVAL          NULL
#define redis_command_EVALSHA       NULL
#define redis_command_EXEC          NULL
#define redis_command_MULTI         NULL
#define redis_command_RENAME        NULL
#define redis_command_RENAMENX      NULL
#define redis_command_RPOPLPUSH     NULL
#define redis_command_SCAN          NULL
#define redis_command_SCRIPT        NULL
#define redis_command_SDIFF         NULL
#define redis_command_SDIFFSTORE    NULL
#define redis_command_SINTER        NULL
#define redis_command_SINTERSTORE   NULL
#define redis_command_SLAVEOF       NULL
#define redis_command_SMOVE         NULL
#define redis_command_SUNION        NULL
#define redis_command_SUNIONSTORE   NULL
#define redis_command_SYNC          NULL
#define redis_command_ZINTERSTORE   NULL
#define redis_command_ZUNIONSTORE   NULL

#define redis_command_QUIT          NULL

void redis_command_forward_by_keys(struct redis_proxy* proxy,
    struct redis_client* c, struct redis_command* cmd, int args_per_key,
    int wait_type) {

  if ((cmd->num_args - 1) % args_per_key != 0) {
    redis_write_string_response(redis_client_get_output_buffer(c),
        "ERR not enough arguments", RESPONSE_ERROR);
    return;
  }

  struct redis_client_expected_response* e = redis_client_expect_response(c,
      wait_type, cmd, proxy->num_backends);

  e->collect_key.args_per_key = args_per_key;
  e->collect_key.num_keys = (cmd->num_args - 1) / e->collect_key.args_per_key;
  e->collect_key.server_to_key_count = (int*)resource_calloc_raw(e, sizeof(int) * proxy->num_backends);
  e->collect_key.key_to_server = (uint8_t*)resource_calloc_raw(e, sizeof(uint8_t) * e->collect_key.num_keys);
  e->collect_key.index_to_command = (struct redis_command**)resource_calloc_raw(e, sizeof(struct redis_command*) * proxy->num_backends);
  if (!e->collect_key.server_to_key_count || !e->collect_key.key_to_server || !e->collect_key.index_to_command) {
    e->error_response = redis_response_printf(e, RESPONSE_ERROR, "ERR can\'t allocate memory");
    return;
  }

  int x, y;
  for (y = 0; y < e->collect_key.num_keys; y++) {
    int arg_index = (y * args_per_key) + 1;
    int server_index = redis_index_for_key(proxy, cmd->args[arg_index].data, cmd->args[arg_index].size);
    e->collect_key.key_to_server[y] = server_index;
    e->collect_key.server_to_key_count[server_index]++;
  }

  for (x = 0; x < proxy->num_backends; x++) {
    if (e->collect_key.server_to_key_count[x] == 0)
      continue;
    e->collect_key.index_to_command[x] = redis_command_create(e,
        (e->collect_key.server_to_key_count[x] * e->collect_key.args_per_key) + 1);
    e->collect_key.index_to_command[x]->external_arg_data = 1;
    e->collect_key.index_to_command[x]->num_args = 1;
    e->collect_key.index_to_command[x]->args[0].data = cmd->args[0].data; // same command as the original
    e->collect_key.index_to_command[x]->args[0].size = cmd->args[0].size; // same command as the original
  }

  for (y = 0; y < e->collect_key.num_keys; y++) {
    int server = e->collect_key.key_to_server[y];
    struct redis_command* server_cmd = e->collect_key.index_to_command[server];
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
    redis_proxy_try_send_backend_command(redis_backend_for_index(proxy, x), e,
        e->collect_key.index_to_command[x]);
  }
}

void redis_command_forward_by_keys_1_multi(struct redis_proxy* proxy,
    struct redis_client* c, struct redis_command* cmd) {
  redis_command_forward_by_keys(proxy, c, cmd, 1, CWAIT_COLLECT_MULTI_RESPONSES_BY_KEY);
}

void redis_command_forward_by_keys_1_integer(struct redis_proxy* proxy,
    struct redis_client* c, struct redis_command* cmd) {
  redis_command_forward_by_keys(proxy, c, cmd, 1, CWAIT_SUM_INT_RESPONSES);
}

void redis_command_forward_by_keys_2_status(struct redis_proxy* proxy,
    struct redis_client* c, struct redis_command* cmd) {
  redis_command_forward_by_keys(proxy, c, cmd, 2, CWAIT_COLLECT_STATUS_RESPONSES);
}

void redis_command_forward_by_key_index(struct redis_proxy* proxy,
    struct redis_client* c, struct redis_command* cmd, int key_index) {

  if (key_index >= cmd->num_args)
    redis_write_string_response(redis_client_get_output_buffer(c),
        "ERR not enough arguments", RESPONSE_ERROR);
  else {
    struct redis_backend* b = redis_backend_for_key(proxy, cmd->args[key_index].data, cmd->args[key_index].size);
    struct redis_client_expected_response* e = redis_client_expect_response(c,
        CWAIT_FORWARD_RESPONSE, cmd, proxy->num_backends);
    redis_proxy_try_send_backend_command(b, e, cmd);
  }
}

void redis_command_forward_by_key1(struct redis_proxy* proxy,
    struct redis_client* c, struct redis_command* cmd) {
  redis_command_forward_by_key_index(proxy, c, cmd, 1);
}

void redis_command_forward_all(struct redis_proxy* proxy,
    struct redis_client* c, struct redis_command* cmd, int wait_type) {

  struct redis_client_expected_response* e = redis_client_expect_response(c,
      wait_type, cmd, proxy->num_backends);

  int x;
  for (x = 0; x < proxy->num_backends; x++) {

    struct redis_backend* b = redis_backend_for_index(proxy, x);
    redis_proxy_try_send_backend_command(b, e, cmd);
  }
}

void redis_command_all_collect_responses(struct redis_proxy* proxy,
    struct redis_client* c, struct redis_command* cmd) {
  redis_command_forward_all(proxy, c, cmd, CWAIT_COLLECT_RESPONSES);
}

void redis_command_KEYS(struct redis_proxy* proxy, struct redis_client* c,
    struct redis_command* cmd) {
  // CAVEAT EMPTOR: this may double-list incorrectly-distributed keys
  redis_command_forward_all(proxy, c, cmd, CWAIT_COMBINE_MULTI_RESPONSES);
}

void redis_command_DBSIZE(struct redis_proxy* proxy, struct redis_client* c,
    struct redis_command* cmd) {
  // CAVEAT EMPTOR: keys may exist on single redis instances that don't hash to
  // that instance; they will be counted too
  redis_command_forward_all(proxy, c, cmd, CWAIT_SUM_INT_RESPONSES);
}

void redis_command_OBJECT(struct redis_proxy* proxy, struct redis_client* c,
    struct redis_command* cmd) {

  if (cmd->num_args < 3)
    redis_write_string_response(redis_client_get_output_buffer(c),
        "ERR not enough arguments", RESPONSE_ERROR);
  else if (cmd->args[1].size != 8 || (
      memcmp(cmd->args[1].data, "REFCOUNT", 8) &&
      memcmp(cmd->args[1].data, "ENCODING", 8) &&
      memcmp(cmd->args[1].data, "IDLETIME", 8)))
    redis_write_string_response(redis_client_get_output_buffer(c),
        "ERR unsupported subcommand", RESPONSE_ERROR);
  else
    redis_command_forward_by_key_index(proxy, c, cmd, 2);
}

void redis_command_MIGRATE(struct redis_proxy* proxy, struct redis_client* c,
    struct redis_command* cmd) {
  if (cmd->num_args < 6)
    redis_write_string_response(redis_client_get_output_buffer(c),
        "ERR not enough arguments", RESPONSE_ERROR);
  else
    redis_command_forward_by_key_index(proxy, c, cmd, 3);
}

void redis_command_DEBUG(struct redis_proxy* proxy, struct redis_client* c,
    struct redis_command* cmd) {
  if (cmd->num_args < 3)
    redis_write_string_response(redis_client_get_output_buffer(c),
        "ERR not enough arguments", RESPONSE_ERROR);
  else if (cmd->args[1].size != 6 || memcmp(cmd->args[1].data, "OBJECT", 6))
    redis_write_string_response(redis_client_get_output_buffer(c),
        "ERR unsupported subcommand", RESPONSE_ERROR);
  else
    redis_command_forward_by_key_index(proxy, c, cmd, 2);
}

void redis_command_RANDOMKEY(struct redis_proxy* proxy, struct redis_client* c,
    struct redis_command* cmd) {

  struct redis_backend* b = redis_backend_for_index(proxy, rand() % proxy->num_backends);
  struct redis_client_expected_response* e = redis_client_expect_response(c,
      CWAIT_FORWARD_RESPONSE, cmd, proxy->num_backends);
  redis_proxy_try_send_backend_command(b, e, cmd);
}

void redis_command_PING(struct redis_proxy* proxy, struct redis_client* c,
    struct redis_command* cmd) {
  redis_write_string_response(redis_client_get_output_buffer(c),
      "PONG", RESPONSE_STATUS);
}

void redis_command_ECHO(struct redis_proxy* proxy, struct redis_client* c,
    struct redis_command* cmd) {

  if (cmd->num_args != 2) {
    redis_write_string_response(redis_client_get_output_buffer(c),
        "ERR wrong number of arguments", RESPONSE_ERROR);
    return;
  }

  struct redis_response* resp = redis_response_create(cmd, RESPONSE_DATA,
      cmd->args[1].size);
  if (!resp) {
    redis_write_string_response(redis_client_get_output_buffer(c),
        "ERR can\'t allocate memory", RESPONSE_ERROR);
  } else {
    memcpy(resp->data_value.data, cmd->args[1].data, cmd->args[1].size);
    redis_write_response(redis_client_get_output_buffer(c), resp);
    resource_delete_ref(cmd, resp);
  }
}

// called when we don't know what the fuck is going on
void redis_command_default(struct redis_proxy* proxy, struct redis_client* c,
    struct redis_command* cmd) {

  printf("UNKNOWN COMMAND: ");
  redis_command_print(cmd);
  redis_write_string_response(redis_client_get_output_buffer(c),
      "ERR unknown command", RESPONSE_ERROR);
}



////////////////////////////////////////////////////////////////////////////////
// command table & lookup functions

typedef void (*redis_command_handler)(struct redis_proxy* proxy,
    struct redis_client* c, struct redis_command* cmd);

struct {
  const char* command_str;
  redis_command_handler handler;
} command_definitions[] = {

  // commands that probably will never be implemented
  {"AUTH",              redis_command_AUTH},                      // password - Authenticate to the server
  {"BLPOP",             redis_command_BLPOP},                     // key [key ...] timeout - Remove and get the first element in a list, or block until one is available
  {"BRPOP",             redis_command_BRPOP},                     // key [key ...] timeout - Remove and get the last element in a list, or block until one is available
  {"BRPOPLPUSH",        redis_command_BRPOPLPUSH},                // source destination timeout - Pop a value from a list, push it to another list and return it; or block until one is available
  {"CLIENT",            redis_command_CLIENT},                    // KILL ip:port / LIST / GETNAME / SETNAME name
  {"CONFIG",            redis_command_CONFIG},                    // GET parameter / REWRITE / SET param value / RESETSTAT
  {"MONITOR",           redis_command_MONITOR},                   // - Listen for all requests received by the server in real time
  {"MOVE",              redis_command_MOVE},                      // key db - Move a key to another database
  {"MSETNX",            redis_command_MSETNX},                    // key value [key value ...] - Set multiple keys to multiple values, only if none of the keys exist (just do an EXISTS everywhere first)
  {"PSUBSCRIBE",        redis_command_PSUBSCRIBE},                // pattern [pattern ...] - Listen for messages published to channels matching the given patterns
  {"PUBSUB",            redis_command_PUBSUB},                    // subcommand [argument [argument ...]] - Inspect the state of the Pub/Sub subsystem
  {"PUBLISH",           redis_command_PUBLISH},                   // channel message - Post a message to a channel
  {"PUNSUBSCRIBE",      redis_command_PUNSUBSCRIBE},              // [pattern [pattern ...]] - Stop listening for messages posted to channels matching the given patterns
  {"SAVE",              redis_command_SAVE},                      // - Synchronously save the dataset to disk
  {"SELECT",            redis_command_SELECT},                    // index - Change the selected database for the current connection
  {"SHUTDOWN",          redis_command_SHUTDOWN},                  // [NOSAVE] [SAVE] - Synchronously save the dataset to disk and then shut down the server
  {"SLOWLOG",           redis_command_SLOWLOG},                   // subcommand [argument] - Manages the Redis slow queries log
  {"SUBSCRIBE",         redis_command_SUBSCRIBE},                 // channel [channel ...] - Listen for messages published to the given channels
  {"UNSUBSCRIBE",       redis_command_UNSUBSCRIBE},               // [channel [channel ...]] - Stop listening for messages posted to the given channels
  {"UNWATCH",           redis_command_UNWATCH},                   // - Forget about all watched keys
  {"WATCH",             redis_command_WATCH},                     // key [key ...] - Watch the given keys to determine execution of the MULTI/EXEC block

  // commands that are hard to implement
  {"BITOP",             redis_command_BITOP},                     // operation destkey key [key ...] - Perform bitwise operations between strings
  {"DISCARD",           redis_command_DISCARD},                   // - Discard all commands issued after MULTI
  {"EVAL",              redis_command_EVAL},                      // script numkeys key [key ...] arg [arg ...] - Execute a Lua script server side
  {"EVALSHA",           redis_command_EVALSHA},                   // sha1 numkeys key [key ...] arg [arg ...] - Execute a Lua script server side
  {"EXEC",              redis_command_EXEC},                      // - Execute all commands issued after MULTI
  {"MULTI",             redis_command_MULTI},                     // - Mark the start of a transaction block
  {"RENAME",            redis_command_RENAME},                    // key newkey - Rename a key
  {"RENAMENX",          redis_command_RENAMENX},                  // key newkey - Rename a key, only if the new key does not exist
  {"RPOPLPUSH",         redis_command_RPOPLPUSH},                 // source destination - Remove the last element in a list, append it to another list and return it
  {"SCAN",              redis_command_SCAN},                      // cursor [MATCH pattern] [COUNT count] - Incrementally iterate the keys space
  {"SCRIPT",            redis_command_SCRIPT},                    // KILL / EXISTS name / FLUSH / LOAD data - Kill the script currently in execution.
  {"SDIFF",             redis_command_SDIFF},                     // key [key ...] - Subtract multiple sets
  {"SDIFFSTORE",        redis_command_SDIFFSTORE},                // destination key [key ...] - Subtract multiple sets and store the resulting set in a key
  {"SINTER",            redis_command_SINTER},                    // key [key ...] - Intersect multiple sets
  {"SINTERSTORE",       redis_command_SINTERSTORE},               // destination key [key ...] - Intersect multiple sets and store the resulting set in a key
  {"SLAVEOF",           redis_command_SLAVEOF},                   // host port - Make the server a slave of another instance, or promote it as master
  {"SMOVE",             redis_command_SMOVE},                     // source destination member - Move a member from one set to another
  {"SUNION",            redis_command_SUNION},                    // key [key ...] - Add multiple sets
  {"SUNIONSTORE",       redis_command_SUNIONSTORE},               // destination key [key ...] - Add multiple sets and store the resulting set in a key
  {"SYNC",              redis_command_SYNC},                      // - Internal command used for replication
  {"ZINTERSTORE",       redis_command_ZINTERSTORE},               // destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] - Intersect multiple sorted sets and store the resulting sorted set in a new key
  {"ZUNIONSTORE",       redis_command_ZUNIONSTORE},               // destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] - Add multiple sorted sets and store the resulting sorted set in a new key

  // commands that are easy to implement
  {"APPEND",            redis_command_forward_by_key1},           // key value - Append a value to a key
  {"BGREWRITEAOF",      redis_command_all_collect_responses},     // - Asynchronously rewrite the append-only file
  {"BGSAVE",            redis_command_all_collect_responses},     // - Asynchronously save db to disk
  {"BITCOUNT",          redis_command_forward_by_key1},           // key [start] [end] - Count set bits in a string
  {"DBSIZE",            redis_command_DBSIZE},                    // - Return the number of keys in the selected database
  {"DEBUG",             redis_command_DEBUG},                     // OBJECT key - Get debugging information about a key
  {"DECR",              redis_command_forward_by_key1},           // key - Decrement the integer value of a key by one
  {"DECRBY",            redis_command_forward_by_key1},           // key decrement - Decrement the integer value of a key by the given number
  {"DEL",               redis_command_forward_by_keys_1_integer}, // key [key ...] - Delete a key
  {"DUMP",              redis_command_forward_by_key1},           // key - Return a serialized version of the value stored at the specified key.
  {"ECHO",              redis_command_ECHO},                      // message - Echo the given string
  {"EXISTS",            redis_command_forward_by_key1},           // key - Determine if a key exists
  {"EXPIRE",            redis_command_forward_by_key1},           // key seconds - Set a keys time to live in seconds
  {"EXPIREAT",          redis_command_forward_by_key1},           // key timestamp - Set the expiration for a key as a UNIX timestamp
  {"FLUSHDB",           redis_command_all_collect_responses},     // - Remove all keys from the current database
  {"FLUSHALL",          redis_command_all_collect_responses},     // - Remove all keys from all databases
  {"GET",               redis_command_forward_by_key1},           // key - Get the value of a key
  {"GETBIT",            redis_command_forward_by_key1},           // key offset - Returns the bit value at offset in the string value stored at key
  {"GETRANGE",          redis_command_forward_by_key1},           // key start end - Get a substring of the string stored at a key
  {"GETSET",            redis_command_forward_by_key1},           // key value - Set the string value of a key and return its old value
  {"HDEL",              redis_command_forward_by_key1},           // key field [field ...] - Delete one or more hash fields
  {"HEXISTS",           redis_command_forward_by_key1},           // key field - Determine if a hash field exists
  {"HGET",              redis_command_forward_by_key1},           // key field - Get the value of a hash field
  {"HGETALL",           redis_command_forward_by_key1},           // key - Get all the fields and values in a hash
  {"HINCRBY",           redis_command_forward_by_key1},           // key field increment - Increment the integer value of a hash field by the given number
  {"HINCRBYFLOAT",      redis_command_forward_by_key1},           // key field increment - Increment the float value of a hash field by the given amount
  {"HKEYS",             redis_command_forward_by_key1},           // key - Get all the fields in a hash
  {"HLEN",              redis_command_forward_by_key1},           // key - Get the number of fields in a hash
  {"HMGET",             redis_command_forward_by_key1},           // key field [field ...] - Get the values of all the given hash fields
  {"HMSET",             redis_command_forward_by_key1},           // key field value [field value ...] - Set multiple hash fields to multiple values
  {"HSCAN",             redis_command_forward_by_key1},           // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate hash fields and associated values
  {"HSET",              redis_command_forward_by_key1},           // key field value - Set the string value of a hash field
  {"HSETNX",            redis_command_forward_by_key1},           // key field value - Set the value of a hash field, only if the field does not exist
  {"HVALS",             redis_command_forward_by_key1},           // key - Get all the values in a hash
  {"INCR",              redis_command_forward_by_key1},           // key - Increment the integer value of a key by one
  {"INCRBY",            redis_command_forward_by_key1},           // key increment - Increment the integer value of a key by the given amount
  {"INCRBYFLOAT",       redis_command_forward_by_key1},           // key increment - Increment the float value of a key by the given amount
  {"INFO",              redis_command_all_collect_responses},     // [section] - Get information and statistics about the server
  {"KEYS",              redis_command_KEYS},                      // pattern - Find all keys matching the given pattern
  {"LASTSAVE",          redis_command_all_collect_responses},     // - Get the UNIX time stamp of the last successful save to disk  
  {"LINDEX",            redis_command_forward_by_key1},           // key index - Get an element from a list by its index
  {"LINSERT",           redis_command_forward_by_key1},           // key BEFORE|AFTER pivot value - Insert an element before or after another element in a list
  {"LLEN",              redis_command_forward_by_key1},           // key - Get the length of a list
  {"LPOP",              redis_command_forward_by_key1},           // key - Remove and get the first element in a list
  {"LPUSH",             redis_command_forward_by_key1},           // key value [value ...] - Prepend one or multiple values to a list
  {"LPUSHX",            redis_command_forward_by_key1},           // key value - Prepend a value to a list, only if the list exists
  {"LRANGE",            redis_command_forward_by_key1},           // key start stop - Get a range of elements from a list
  {"LREM",              redis_command_forward_by_key1},           // key count value - Remove elements from a list
  {"LSET",              redis_command_forward_by_key1},           // key index value - Set the value of an element in a list by its index
  {"LTRIM",             redis_command_forward_by_key1},           // key start stop - Trim a list to the specified range
  {"MGET",              redis_command_forward_by_keys_1_multi},   // key [key ...] - Get the values of all the given keys
  {"MIGRATE",           redis_command_MIGRATE},                   // host port key destination-db timeout [COPY] [REPLACE] - Atomically transfer a key from a Redis instance to another one.
  {"MSET",              redis_command_forward_by_keys_2_status},  // key value [key value ...] - Set multiple keys to multiple values
  {"OBJECT",            redis_command_OBJECT},                    // subcommand [arguments [arguments ...]] - Inspect the internals of Redis objects
  {"PERSIST",           redis_command_forward_by_key1},           // key - Remove the expiration from a key
  {"PEXPIRE",           redis_command_forward_by_key1},           // key milliseconds - Set a keys time to live in milliseconds
  {"PEXPIREAT",         redis_command_forward_by_key1},           // key milliseconds-timestamp - Set the expiration for a key as a UNIX timestamp specified in milliseconds
  {"PING",              redis_command_PING},                      // - Ping the server
  {"PSETEX",            redis_command_forward_by_key1},           // key milliseconds value - Set the value and expiration in milliseconds of a key
  {"PTTL",              redis_command_forward_by_key1},           // key - Get the time to live for a key in milliseconds
  {"QUIT",              redis_command_QUIT},                      // - Close the connection
  {"RANDOMKEY",         redis_command_RANDOMKEY},                 // - Return a random key from the keyspace
  {"RESTORE",           redis_command_forward_by_key1},           // key ttl serialized-value - Create a key using the provided serialized value, previously obtained using DUMP.
  {"RPOP",              redis_command_forward_by_key1},           // key - Remove and get the last element in a list
  {"RPUSH",             redis_command_forward_by_key1},           // key value [value ...] - Append one or multiple values to a list
  {"RPUSHX",            redis_command_forward_by_key1},           // key value - Append a value to a list, only if the list exists
  {"SADD",              redis_command_forward_by_key1},           // key member [member ...] - Add one or more members to a set
  {"SCARD",             redis_command_forward_by_key1},           // key - Get the number of members in a set
  {"SET",               redis_command_forward_by_key1},           // key value [EX seconds] [PX milliseconds] [NX|XX] - Set the string value of a key
  {"SETBIT",            redis_command_forward_by_key1},           // key offset value - Sets or clears the bit at offset in the string value stored at key
  {"SETEX",             redis_command_forward_by_key1},           // key seconds value - Set the value and expiration of a key
  {"SETNX",             redis_command_forward_by_key1},           // key value - Set the value of a key, only if the key does not exist
  {"SETRANGE",          redis_command_forward_by_key1},           // key offset value - Overwrite part of a string at key starting at the specified offset
  {"SISMEMBER",         redis_command_forward_by_key1},           // key member - Determine if a given value is a member of a set
  {"SMEMBERS",          redis_command_forward_by_key1},           // key - Get all the members in a set
  {"SORT",              redis_command_forward_by_key1},           // key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination] - Sort the elements in a list, set or sorted set
  {"SPOP",              redis_command_forward_by_key1},           // key - Remove and return a random member from a set
  {"SRANDMEMBER",       redis_command_forward_by_key1},           // key [count] - Get one or multiple random members from a set
  {"SREM",              redis_command_forward_by_key1},           // key member [member ...] - Remove one or more members from a set
  {"SSCAN",             redis_command_forward_by_key1},           // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate Set elements
  {"STRLEN",            redis_command_forward_by_key1},           // key - Get the length of the value stored in a key
  {"TIME",              redis_command_all_collect_responses},     // - Return the current server time
  {"TTL",               redis_command_forward_by_key1},           // key - Get the time to live for a key
  {"TYPE",              redis_command_forward_by_key1},           // key - Determine the type stored at key
  {"ZADD",              redis_command_forward_by_key1},           // key score member [score member ...] - Add one or more members to a sorted set, or update its score if it already exists
  {"ZCARD",             redis_command_forward_by_key1},           // key - Get the number of members in a sorted set
  {"ZCOUNT",            redis_command_forward_by_key1},           // key min max - Count the members in a sorted set with scores within the given values
  {"ZINCRBY",           redis_command_forward_by_key1},           // key increment member - Increment the score of a member in a sorted set
  {"ZRANGE",            redis_command_forward_by_key1},           // key start stop [WITHSCORES] - Return a range of members in a sorted set, by index
  {"ZRANGEBYSCORE",     redis_command_forward_by_key1},           // key min max [WITHSCORES] [LIMIT offset count] - Return a range of members in a sorted set, by score
  {"ZRANK",             redis_command_forward_by_key1},           // key member - Determine the index of a member in a sorted set
  {"ZREM",              redis_command_forward_by_key1},           // key member [member ...] - Remove one or more members from a sorted set
  {"ZREMRANGEBYRANK",   redis_command_forward_by_key1},           // key start stop - Remove all members in a sorted set within the given indexes
  {"ZREMRANGEBYSCORE",  redis_command_forward_by_key1},           // key min max - Remove all members in a sorted set within the given scores
  {"ZREVRANGE",         redis_command_forward_by_key1},           // key start stop [WITHSCORES] - Return a range of members in a sorted set, by index, with scores ordered from high to low
  {"ZREVRANGEBYSCORE",  redis_command_forward_by_key1},           // key max min [WITHSCORES] [LIMIT offset count] - Return a range of members in a sorted set, by score, with scores ordered from high to low
  {"ZREVRANK",          redis_command_forward_by_key1},           // key member - Determine the index of a member in a sorted set, with scores ordered from high to low
  {"ZSCAN",             redis_command_forward_by_key1},           // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate sorted sets elements and associated scores
  {"ZSCORE",            redis_command_forward_by_key1},           // key member - Get the score associated with the given member in a sorted set

  {NULL, NULL}, // end marker
};

#define MAX_COMMANDS_PER_HASH_BUCKET 8

int8_t hash_to_num_commands[256];
int16_t hash_to_command_indexes[256][MAX_COMMANDS_PER_HASH_BUCKET];

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

redis_command_handler handler_for_command(const char* command, int cmdlen) {

  uint8_t hash = hash_command(command, cmdlen);

  int x;
  for (x = 0; x < hash_to_num_commands[hash]; x++)
    if (!strncmp(command, command_definitions[hash_to_command_indexes[hash][x]].command_str, cmdlen))
      return command_definitions[hash_to_command_indexes[hash][x]].handler;
  return redis_command_default;
}


////////////////////////////////////////////////////////////////////////////////
// server functions

void redis_proxy_handle_client_command(struct redis_proxy* proxy,
    struct redis_client* c, struct redis_command* cmd) {

  if (cmd->num_args <= 0) {
    redis_write_string_response(redis_client_get_output_buffer(c),
        "ERR invalid command", RESPONSE_ERROR);
    return;
  }

  int x;
  char* arg0_str = (char*)cmd->args[0].data;
  for (x = 0; x < cmd->args[0].size; x++)
    arg0_str[x] = toupper(arg0_str[x]);

  handler_for_command(arg0_str, cmd->args[0].size)(proxy, c, cmd);

  // need to check for complete responses here because the command handler can
  // add an error item on the queue
  while (c->response_chain_head && c->response_chain_head->num_responses_expected == 0) {
    redis_proxy_complete_response(c->response_chain_head);
    redis_client_remove_expected_response(c);
  }
}



////////////////////////////////////////////////////////////////////////////////
// event callbacks

static void redis_proxy_on_client_input(struct bufferevent *bev, void *ctx) {
  struct redis_client* c = (struct redis_client*)ctx;
  struct evbuffer* in_buffer = bufferevent_get_input(bev);

  if (!c->parser)
    c->parser = redis_command_parser_create(c);

  struct redis_command* cmd;
  while ((cmd = redis_command_parser_continue(c, c->parser, in_buffer))) {
    printf("INPUT COMMAND FROM CLIENT %p\n", c);
    redis_command_print(cmd);
    redis_proxy_handle_client_command((struct redis_proxy*)c->ctx, c, cmd);
    resource_delete_ref(c, cmd);
  }
}

static void redis_proxy_on_client_error(struct bufferevent *bev, short events,
    void *ctx) {
  struct redis_client* c = (struct redis_client*)ctx;

  if (events & BEV_EVENT_ERROR) {
    int err = EVUTIL_SOCKET_ERROR();
    printf("error: client %p gave %d (%s)\n", c, err,
        evutil_socket_error_to_string(err));
  }
  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR))
    resource_delete_ref(c->ctx, c);
}

static void redis_proxy_on_backend_input(struct bufferevent *bev, void *ctx) {
  struct redis_backend* b = (struct redis_backend*)ctx;
  struct evbuffer* in_buffer = bufferevent_get_input(bev);

  if (!b->parser)
    b->parser = redis_response_parser_create(b);

  struct redis_response* rsp;
  while ((rsp = redis_response_parser_continue(b, b->parser, in_buffer))) {
    printf("INPUT RESPONSE FROM BACKEND %p\n", b);
    redis_response_print(rsp);
    redis_proxy_handle_backend_response(b->ctx, b, rsp);
    resource_delete_ref(b, rsp);
  }
}

static void redis_proxy_on_backend_error(struct bufferevent *bev, short events,
    void *ctx) {
  struct redis_backend* b = (struct redis_backend*)ctx;

  if (events & BEV_EVENT_ERROR) {
    int err = EVUTIL_SOCKET_ERROR();
    printf("error: backend %p gave %d (%s)\n", b, err,
        evutil_socket_error_to_string(err));
  }
  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    bufferevent_free(bev); // this closes the socket
    b->bev = NULL;

    // delete the parser, in case it was in the middle of something
    resource_delete_ref(b, b->parser);
    b->parser = NULL;

    // issue a fake error response to all waiting clients
    if (b->wait_chain_head) {
      struct redis_response* error_response = redis_response_printf(b,
          RESPONSE_ERROR, "CHANNELERROR backend disconnected before sending the response");

      struct redis_client_expected_response* e;
      while ((e = redis_backend_get_waiting_client(b)))
        redis_proxy_handle_backend_response((struct redis_proxy*)b->ctx, b,
            error_response);
      resource_delete_ref(b, error_response);
    }
  }
}

static void redis_proxy_on_listen_error(struct evconnlistener *listener,
    void *ctx) {
  struct redis_proxy* proxy = (struct redis_proxy*)ctx;

  int err = EVUTIL_SOCKET_ERROR();
  printf("error: server socket failed with code %d (%s)\n", err,
      evutil_socket_error_to_string(err));

  event_base_loopexit(proxy->base, NULL);
}

static void redis_proxy_on_client_accept(struct evconnlistener *listener,
    evutil_socket_t fd, struct sockaddr *address, int socklen, void *ctx) {

  struct redis_proxy* proxy = (struct redis_proxy*)ctx;

  printf("proxy: new client: fd %d\n", fd);

  int fd_flags = fcntl(fd, F_GETFD, 0);
  if (fd_flags >= 0) {
    fd_flags |= FD_CLOEXEC;
    fcntl(fd, F_SETFD, fd_flags);
  }

  // set up a bufferevent for the new connection
  struct bufferevent *bev = bufferevent_socket_new(proxy->base, fd,
      BEV_OPT_CLOSE_ON_FREE);
  printf("proxy: opened bufferevent %p\n", bev);

  // allocate a struct for this connection
  struct redis_client* c = redis_client_create(proxy, bev);
  if (!c) {
    // if this happens, we're boned anyway
    printf("error: server is out of memory; can\'t allocate a redis_client\n");
    bufferevent_free(bev);
    return;
  }
  c->ctx = proxy;
  network_get_socket_addresses(fd, &c->local, &c->remote);

  bufferevent_setcb(c->bev, redis_proxy_on_client_input, NULL,
      redis_proxy_on_client_error, c);
  bufferevent_enable(c->bev, EV_READ | EV_WRITE);
}
