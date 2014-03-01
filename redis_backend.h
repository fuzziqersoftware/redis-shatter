#ifndef __REDIS_BACKEND_H
#define __REDIS_BACKEND_H

#include <event2/bufferevent.h>
#include <event2/buffer.h>

#include "resource.h"
#include "redis_client.h"
#include "redis_protocol.h"

#define DEFAULT_REDIS_PORT 6379

struct redis_backend {
  struct resource res;

  void* ctx;

  char name[0x40];

  char* host;
  int port;

  struct bufferevent* bev;
  struct redis_response_parser* parser;

  int num_responses_received;
  int num_commands_sent;

  struct sockaddr_in local;
  struct sockaddr_in remote;

  struct redis_client_expected_response* wait_chain_head;
  struct redis_client_expected_response* wait_chain_tail;
};

struct redis_backend* redis_backend_create(void* resource_parent,
    const char* host, int port);
void redis_backend_print(struct redis_backend* b, int indent);

struct evbuffer* redis_backend_get_output_buffer(struct redis_backend* c);

void redis_backend_add_waiting_client(struct redis_backend* b,
    struct redis_client_expected_response* e);
struct redis_client_expected_response* redis_backend_get_waiting_client(
    struct redis_backend* b);

#endif // __REDIS_BACKEND_H
