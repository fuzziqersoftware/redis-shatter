#ifndef __REDIS_MULTICLIENT_H
#define __REDIS_MULTICLIENT_H

#include "ketama.h"
#include "redis_socket.h"
#include "redis_client.h"

typedef struct {
  resource res;

  pthread_mutex_t lock;

  ketama_continuum* ketama;

  int num_clients;
  redis_client* clients[0];
} redis_multiclient;

redis_multiclient* redis_multiclient_create(void* resource_parent, int num_clients, const char** netlocs);
void redis_multiclient_delete(redis_multiclient* mc);

int redis_index_for_key(redis_multiclient* mc, void* key, int64_t size);
redis_client* redis_client_for_index(void* resource_parent, redis_multiclient* mc, int index);
redis_client* redis_client_for_key(void* resource_parent, redis_multiclient* mc, void* key, int64_t size);

#endif // __REDIS_MULTICLIENT_H
