#ifndef __REDIS_CLIENT_H
#define __REDIS_CLIENT_H

#include <pthread.h>

#include "resource.h"
#include "redis_socket.h"
#include "redis_protocol.h"

typedef struct _redis_response_wait_entry {
  resource res;
  pthread_cond_t ready;
  struct _redis_response_wait_entry* next;
} redis_response_wait_entry;

typedef struct {
  resource res;

  pthread_mutex_t lock;

  char* host;
  int port;
  redis_socket* sock;

  redis_response_wait_entry* wait_chain_head;
  redis_response_wait_entry* wait_chain_tail;
} redis_client;

redis_client* redis_client_create(void* resource_parent, const char* host, int port);
void redis_client_delete(redis_client* c);

redis_response* redis_client_exec_command(redis_client* client, redis_command* cmd);

#endif // __REDIS_CLIENT_H
