#include <stdlib.h>
#include <stdio.h>

#include "debug.h"
#include "resource.h"
#include "redis_client.h"

static void redis_client_delete(struct redis_client* c) {
  if (c->bev) {
    printf("client: freeing bufferevent %p\n", c->bev);
    bufferevent_free(c->bev);
  }
  free(c);
}

struct redis_client* redis_client_create(void* resource_parent,
    struct bufferevent* bev) {

  struct redis_client* c = (struct redis_client*)calloc(1, sizeof(struct redis_client));
  if (!c)
    return NULL;
  resource_create(resource_parent, c, redis_client_delete);
  resource_annotate(c, "redis_client[%p]", bev);
  c->bev = bev;
  return c;
}

struct evbuffer* redis_client_get_output_buffer(struct redis_client* c) {
  return bufferevent_get_output(c->bev);
}

static struct redis_client_expected_response* redis_client_expected_response_create(
  void* resource_parent, int wait_type, struct redis_command* cmd, int size) {

  int size_needed = sizeof(struct redis_client_expected_response);
  if (wait_type == CWAIT_COMBINE_MULTI_RESPONSES)
    size_needed += (size * sizeof(struct redis_response*));

  struct redis_client_expected_response* e =
      (struct redis_client_expected_response*)resource_calloc(resource_parent, size_needed);
  if (!e)
    return NULL;
  resource_annotate(e, "redis_client_expected_response[%d, %p, %d]", wait_type, cmd, size);
  resource_add_ref(e, cmd);
  e->original_command = cmd;
  e->wait_type = wait_type;
  return e;
}

struct redis_client_expected_response* redis_client_expect_response(
    struct redis_client* c, int wait_type, struct redis_command* cmd,
    int size) {

  if (c->response_chain_head == NULL) {
    c->response_chain_head = redis_client_expected_response_create(c, wait_type, cmd, size);
    c->response_chain_tail = c->response_chain_head;
  } else {
    c->response_chain_tail->next_response = redis_client_expected_response_create(c, wait_type, cmd, size);
    c->response_chain_tail = c->response_chain_tail->next_response;
  }
  c->response_chain_tail->client = c;

  return c->response_chain_tail;
}

void redis_client_remove_expected_response(struct redis_client* c) {

  if (!c->response_chain_head)
    return;

  struct redis_client_expected_response* to_delete = c->response_chain_head;
  c->response_chain_head = c->response_chain_head->next_response;
  if (c->response_chain_head == NULL)
    c->response_chain_tail = NULL;
  resource_delete_ref(c, to_delete);
}
