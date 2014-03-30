#include <stdlib.h>
#include <stdio.h>

#include "debug.h"
#include "resource.h"
#include "client.h"

static void client_delete(struct client* c) {
  if (c->bev) {
    bufferevent_free(c->bev);
    c->bev = NULL;
  }
  free(c);
}

struct client* client_create(void* resource_parent, struct bufferevent* bev) {

  struct client* c = (struct client*)resource_calloc(
      resource_parent, sizeof(struct client), client_delete);
  if (!c)
    return NULL;
  resource_annotate(c, "client[%p]", bev);
  c->bev = bev;
  return c;
}

void client_print(const struct client* c, int indent) {

  if (indent < 0)
    indent = -indent;
  else
    print_indent(indent);

  if (!c) {
    printf("client@NULL");
    return;
  }

  printf("client@%p[ctx=%p, bev=%p, num_commands_received=%d, num_responses_sent=%d, parser=",
      c, c->ctx, c->bev, c->num_commands_received, c->num_responses_sent);
  command_parser_print(c->parser, -(indent + 1));
  printf(", response_chain=[");

  struct client_expected_response* e = c->response_chain_head;
  for (; e; e = e->next_response) {
    printf("\n");
    client_expected_response_print(e, indent + 1);
    printf(",");
  }

  print_indent(indent);
  printf("]]");
}

struct evbuffer* client_get_output_buffer(struct client* c) {
  if (!c->bev)
    return NULL;
  return bufferevent_get_output(c->bev);
}

static struct client_expected_response* client_expected_response_create(
  void* resource_parent, int wait_type, struct command* cmd, int size) {

  int size_needed = sizeof(struct client_expected_response);
  if (wait_type == CWAIT_COMBINE_MULTI_RESPONSES)
    size_needed += (size * sizeof(struct response*));

  struct client_expected_response* e =
      (struct client_expected_response*)resource_calloc(resource_parent,
          size_needed, free);
  if (!e)
    return NULL;
  resource_annotate(e, "client_expected_response[%d, %p, %d]", wait_type, cmd, size);
  resource_add_ref(e, cmd);
  e->original_command = cmd;
  e->wait_type = wait_type;
  return e;
}

struct client_expected_response* client_expect_response(
    struct client* c, int wait_type, struct command* cmd,
    int size) {

  if (c->response_chain_head == NULL) {
    c->response_chain_head = client_expected_response_create(c, wait_type, cmd, size);
    c->response_chain_tail = c->response_chain_head;
  } else {
    c->response_chain_tail->next_response = client_expected_response_create(c, wait_type, cmd, size);
    c->response_chain_tail = c->response_chain_tail->next_response;
  }
  c->response_chain_tail->client = c;
  resource_add_ref(c->response_chain_tail, c);

  return c->response_chain_tail;
}

void client_remove_expected_response(struct client* c) {

  if (!c->response_chain_head)
    return;

  struct client_expected_response* to_delete = c->response_chain_head;
  c->response_chain_head = c->response_chain_head->next_response;
  if (c->response_chain_head == NULL)
    c->response_chain_tail = NULL;

  resource_delete_ref(c, to_delete);
}

static const char* client_cwait_type_name(int type) {
  switch (type) {
    case CWAIT_FORWARD_RESPONSE:
      return "CWAIT_FORWARD_RESPONSE";
    case CWAIT_COLLECT_STATUS_RESPONSES:
      return "CWAIT_COLLECT_STATUS_RESPONSES";
    case CWAIT_SUM_INT_RESPONSES:
      return "CWAIT_SUM_INT_RESPONSES";
    case CWAIT_COMBINE_MULTI_RESPONSES:
      return "CWAIT_COMBINE_MULTI_RESPONSES";
    case CWAIT_COLLECT_RESPONSES:
      return "CWAIT_COLLECT_RESPONSES";
    case CWAIT_COLLECT_MULTI_RESPONSES_BY_KEY:
      return "CWAIT_COLLECT_MULTI_RESPONSES_BY_KEY";
    case CWAIT_COLLECT_IDENTICAL_RESPONSES:
      return "CWAIT_COLLECT_IDENTICAL_RESPONSES";
    default:
      return "UNKNOWN_CWAIT_TYPE";
  }
}

void client_expected_response_print(const struct client_expected_response* e, int indent) {

  if (indent < 0)
    indent = -indent;
  else
    print_indent(indent);

  if (!e) {
    printf("client_expected_response@NULL");
    return;
  }

  printf("client_expected_response@%p[client=%p, next_wait=%p, next_response=%p,\n", e, e->client, e->next_wait, e->next_response);
  print_indent(indent + 1);
  printf("original_command=");
  command_print(e->original_command, -(indent + 1));
  printf(", wait_type=%s, num_responses_expected=%d,\n", client_cwait_type_name(e->wait_type), e->num_responses_expected);
  print_indent(indent + 1);
  printf("error_response=");
  response_print(e->error_response, -(indent + 1));
  printf(", ");

  switch (e->wait_type) {

    case CWAIT_FORWARD_RESPONSE:
      printf("response_to_forward=");
      response_print(e->response_to_forward, -(indent + 1));
      printf("]");
      break;

    case CWAIT_COLLECT_STATUS_RESPONSES:
      printf("no internal state for this wait type]");
      break;

    case CWAIT_SUM_INT_RESPONSES:
      printf("int_sum=%lld]", e->int_sum);
      break;

    case CWAIT_COMBINE_MULTI_RESPONSES:
    case CWAIT_COLLECT_RESPONSES:
    case CWAIT_COLLECT_IDENTICAL_RESPONSES: {
      int x;
      printf("responses=[\n");
      for (x = 0; x < e->collect_multi.num_responses; x++) {
        response_print(e->collect_multi.responses[x], indent + 2);
        printf(",\n");
      }
      print_indent(indent + 1);
      printf("]");
      break; }

    case CWAIT_COLLECT_MULTI_RESPONSES_BY_KEY:
      printf("TODO: implement printing internal vars for this wait type]");
      break;

    default:
      printf("internal vars unavailable for this wait type]");
  }
}
