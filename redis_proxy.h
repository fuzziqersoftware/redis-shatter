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

  int num_backends;
  struct redis_backend** backends;
};

int build_command_definitions();

struct redis_proxy* redis_proxy_create(void* resource_parent, int listen_fd,
    const char** netlocs, int num_backends);
void redis_proxy_serve(struct redis_proxy* proxy);

#endif // __REDIS_SERVER_H
