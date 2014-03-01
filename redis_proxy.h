#ifndef __REDIS_SERVER_H
#define __REDIS_SERVER_H

#include "ketama.h"
#include "redis_backend.h"

struct redis_proxy {
	struct resource res;

	struct event_base* base;
	struct evconnlistener* listener;
	int listen_fd;

  struct ketama_continuum* ketama;

  int num_commands_received;
  int num_commands_sent;
  int num_responses_received;
  int num_responses_sent;
  int num_connections_received;
  time_t start_time;
  int process_num;
  int num_processes;

  int num_backends;
  struct redis_backend** backends;

  int num_clients;
  struct redis_client* client_chain_head;
  struct redis_client* client_chain_tail;
};

int build_command_definitions();

struct redis_proxy* redis_proxy_create(void* resource_parent, int listen_fd,
    const char** netlocs, int num_backends);
void redis_proxy_serve(struct redis_proxy* proxy);
void redis_proxy_print(struct redis_proxy* proxy, int indent);

#endif // __REDIS_SERVER_H
