#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "debug.h"
#include "redis_socket.h"
#include "redis_protocol.h"
#include "redis_client.h"

redis_client* redis_client_create(void* resource_parent, const char* host, int port) {
  redis_client* c = (redis_client*)malloc(sizeof(redis_client));
  if (!c)
    return NULL;
  resource_create(NULL, c, redis_client_delete);

  redis_socket* sock = redis_connect(c, host, port, NULL, c);
  if (!sock) {
    resource_delete(c, 1);
    return NULL;
  }

  c->wait_chain_head = NULL;
  c->wait_chain_tail = NULL;
  c->sock = sock;
  pthread_mutex_init(&c->lock, NULL);
  return c;
}

void redis_client_delete(redis_client* c) {
  // resource abstraction takes care of everything else for us
  free(c);
}

redis_response* redis_client_exec_command(redis_client* client, redis_command* cmd) {

  // we can get away with this being on the stack since nothing refers to it
  // outside of this thread - we havethe guarantee that when the command has
  // been processed, this wait entry has no inbound references.
  redis_response_wait_entry entry;
  resource_create(client, &entry, NULL);
  entry.next = NULL;
  pthread_cond_init(&entry.ready, NULL);

  // you are allowed to read from the client socket if and only if your thread
  // is the head entry in the callback chain

  pthread_mutex_lock(&client->lock);
  redis_send_command(client->sock, cmd);
  if (client->wait_chain_tail) {
    // others are waiting; wait for them to notify us
    client->wait_chain_tail->next = &entry;
    resource_add_ref(client->wait_chain_tail, &entry);
    client->wait_chain_tail = &entry;
    pthread_cond_wait(&entry.ready, &client->lock);
  } else {
    // nobody else is waiting, but put the entry here so they'll know they have to wait
    client->wait_chain_head = client->wait_chain_tail = &entry;
  }
  pthread_mutex_unlock(&client->lock);

  // yay we can read now
  // TODO: figure out what happens if redis_error occurs here and deal with it
  // (locking/lists on the client may be a problem)
  redis_response* response = redis_receive_response(cmd, client->sock);

  // notify the next thread, if any
  pthread_mutex_lock(&client->lock);
  client->wait_chain_head = entry.next;
  resource_delete_ref(client, &entry);
  if (client->wait_chain_head)
    pthread_cond_signal(&client->wait_chain_head->ready);
  else
    client->wait_chain_tail = NULL;
  pthread_mutex_unlock(&client->lock);

  return response;
}
