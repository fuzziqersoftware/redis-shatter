#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "debug.h"
#include "redis_protocol.h"
#include "redis_backend.h"

static void redis_backend_delete(struct redis_backend* c) {
  // resource abstraction takes care of everything else for us
  if (c->host)
    free(c->host);
  free(c);
}

struct redis_backend* redis_backend_create(void* resource_parent, const char* host, int port) {
  struct redis_backend* b = (struct redis_backend*)malloc(
      sizeof(struct redis_backend));
  if (!b)
    return NULL;
  resource_create(resource_parent, b, redis_backend_delete);
  resource_annotate(b, "redis_backend[%s:%d]", host, port);

  if (port == 0) {
    // either the host is actually host:port, or the port is (implied) 6379
    if (strchr(host, ':')) {
      char* host_copy = strdup(host);
      char* sep = strchr(host_copy, ':');
      *sep = 0;
      b->host = host_copy;
      b->port = atoi(sep + 1);
    } else {
      b->host = strdup(host);
      b->port = DEFAULT_REDIS_PORT;
    }
  } else {
    b->host = strdup(host);
    b->port = port;
  }
  sprintf(b->name, "%s:%d", b->host, b->port);

  b->wait_chain_head = NULL;
  b->wait_chain_tail = NULL;
  return b;
}

struct evbuffer* redis_backend_get_output_buffer(struct redis_backend* b) {
  if (!b->bev)
    return NULL;
  return bufferevent_get_output(b->bev);
}

void redis_backend_print(const struct redis_backend* b, int indent) {
  if (indent < 0)
    indent = -indent;
  else
    print_indent(indent);

  if (!b) {
    printf("redis_backend@NULL");
    return;
  }

  printf("redis_backend@%p[ctx=%p, host=%s, port=%d, bev=%p, num_commands_sent=%d, num_responses_received=%d, parser=",
      b, b->ctx, b->host, b->port, b->bev, b->num_commands_sent, b->num_responses_received);
  redis_response_parser_print(b->parser, -(indent + 2));
  printf(", wait_chain=[\n");

  struct redis_client_expected_response* e = b->wait_chain_head;
  for (; e; e = e->next_wait) {
    redis_client_expected_response_print(e, indent + 1);
    printf(",\n");
  }

  print_indent(indent);
  printf("]]");
}

void redis_backend_add_waiting_client(struct redis_backend* b,
    struct redis_client_expected_response* e) {

  if (b->wait_chain_head == NULL) {
    b->wait_chain_head = e;
    b->wait_chain_tail = b->wait_chain_head;
  } else {
    b->wait_chain_tail->next_wait = e;
    b->wait_chain_tail = b->wait_chain_tail->next_wait;
  }
  resource_add_ref(b, e);
}

struct redis_client_expected_response* redis_backend_get_waiting_client(
    struct redis_backend* b) {

  if (!b->wait_chain_head)
    return NULL;

  struct redis_client_expected_response* e = b->wait_chain_head;
  b->wait_chain_head = b->wait_chain_head->next_wait;
  if (b->wait_chain_head == NULL)
    b->wait_chain_tail = NULL;
  resource_delete_ref(b, e);

  return e;
}
