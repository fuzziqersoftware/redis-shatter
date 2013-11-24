#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "debug.h"
#include "ketama.h"
#include "redis_socket.h"
#include "redis_client.h"
#include "redis_multiclient.h"

redis_multiclient* redis_multiclient_create(void* resource_parent, int num_netlocs, const char** netlocs) {
  redis_multiclient* mc = (redis_multiclient*)malloc(sizeof(redis_multiclient) + num_netlocs * sizeof(redis_client*));
  if (!mc)
    return NULL;
  resource_create(resource_parent, mc, redis_multiclient_delete);

  mc->ketama = ketama_continuum_create(mc, num_netlocs, netlocs);
  mc->num_clients = num_netlocs;

  int x;
  for (x = 0; x < mc->num_clients; x++) {
    mc->clients[x] = redis_client_create(mc, netlocs[x], 0);
    if (!mc->clients[x])
      printf("warning: failed to connect to backend %s\n", netlocs[x]);
  }

  pthread_mutex_init(&mc->lock, NULL);
  return mc;
}

void redis_multiclient_delete(redis_multiclient* mc) {
  pthread_mutex_lock(&mc->lock);
  free(mc); // resource does everything else for us
}

redis_client* redis_client_for_key(redis_multiclient* mc, void* key, int64_t size) {
  pthread_mutex_lock(&mc->lock);
  int server_index = ketama_server_for_key(mc->ketama, key, size);
  redis_client* client = mc->clients[server_index];
  if (client->sock->error1) {
    printf("warning: client for server %d (%s:%d) shows error (%d, %d); reconnecting\n",
        server_index, client->host, client->port, client->sock->error1, client->sock->error2);
    client = redis_client_create(mc, client->host, client->port);
    resource_delete_ref(mc, mc->clients[server_index]);
    mc->clients[server_index] = client;
  }
  pthread_mutex_unlock(&mc->lock);
  return client;
}
