#ifndef __PROXY_H
#define __PROXY_H

#include "ketama.h"
#include "backend.h"

struct proxy {
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
  struct backend** backends;

  int num_clients;
  struct client* client_chain_head;
  struct client* client_chain_tail;

  char hash_begin_delimiter;
  char hash_end_delimiter;
  int (*index_for_key)(const struct proxy*, const void*, int64_t);
};

int disable_command(const char* command_name);
int build_command_definitions();

struct proxy* proxy_create(void* resource_parent, int listen_fd,
    const char** netlocs, int num_backends, char hash_begin_delimiter,
    char hash_end_delimiter);
void proxy_serve(struct proxy* proxy);
void proxy_print(const struct proxy* proxy, int indent);

#endif // __REDIS_PROXY_H
