#ifndef __BACKEND_H
#define __BACKEND_H

#include <event2/bufferevent.h>
#include <event2/buffer.h>

#include "resource.h"
#include "client.h"
#include "protocol.h"

struct backend {
  struct resource res;

  void* ctx;

  char name[0x40];

  char* host;
  int port;

  struct bufferevent* bev;
  struct response_parser* parser;

  int num_responses_received;
  int num_commands_sent;

  struct sockaddr_in local;
  struct sockaddr_in remote;

  struct client_expected_response* wait_chain_head;
  struct client_expected_response* wait_chain_tail;
};

struct backend* backend_create(void* resource_parent, const char* host,
    int port);
void backend_print(const struct backend* b, int indent);

struct evbuffer* backend_get_output_buffer(struct backend* c);

void backend_add_waiting_client(struct backend* b,
    struct client_expected_response* e);
struct client_expected_response* backend_peek_waiting_client(
    struct backend* b);
struct client_expected_response* backend_get_waiting_client(
    struct backend* b);

#endif // __BACKEND_H
