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
  resource_create(resource_parent, c, redis_client_delete);

  redis_socket* sock = NULL;
  if (port == 0) {
    // either the host is actually host:port, or the port is (implied) 6379
    if (strchr(host, ':')) {
      char* host_copy = strdup(host);
      char* sep = strchr(host_copy, ':');
      *sep = 0;
      c->host = host_copy;
      c->port = atoi(sep + 1);
    } else {
      c->host = strdup(host);
      c->port = DEFAULT_REDIS_PORT;
    }
  } else {
    c->host = strdup(host);
    c->port = port;
  }
  sock = redis_connect(c, c->host, c->port, NULL, c);

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
  if (c->host)
    free(c->host);
  free(c);
}

static int redis_client_send_command_and_wait_for_response(redis_client* client, redis_command* cmd, redis_response_wait_entry* entry) {

  resource_create(client, entry, NULL);
  entry->next = NULL;
  pthread_cond_init(&entry->ready, NULL);

  // you are allowed to read from the client socket if and only if your thread
  // is the head entry in the callback chain

  pthread_mutex_lock(&client->lock);

  int error = setjmp(client->sock->error_jmp);
  if (!error)
    redis_send_command(client->sock, cmd);
  else {
    pthread_mutex_unlock(&client->lock);
    return error;
  }

  if (client->wait_chain_tail) {
    // others are waiting; wait for them to notify us
    client->wait_chain_tail->next = entry;
    resource_add_ref(client->wait_chain_tail, entry);
    client->wait_chain_tail = entry;
    pthread_cond_wait(&entry->ready, &client->lock);
  } else {
    // nobody else is waiting, but put the entry here so they'll know they have to wait
    client->wait_chain_head = client->wait_chain_tail = entry;
  }
  pthread_mutex_unlock(&client->lock);

  return 0;
}

static void redis_client_notify_next_thread(redis_client* client, redis_response_wait_entry* entry) {

  // notify the next thread, if any
  pthread_mutex_lock(&client->lock);
  client->wait_chain_head = entry->next;
  resource_delete_ref(client, entry);
  if (client->wait_chain_head)
    pthread_cond_signal(&client->wait_chain_head->ready);
  else
    client->wait_chain_tail = NULL;
  pthread_mutex_unlock(&client->lock);
}

redis_response* redis_client_exec_command(redis_client* client, redis_command* cmd) {

  // we can get away with this being on the stack since nothing refers to it
  // outside of this thread - we have the guarantee that when the command has
  // been processed, this wait entry has no inbound references.
  // TODO this isn't true; we need to deal with redis_error cases too
  redis_response_wait_entry entry;
  if (redis_client_send_command_and_wait_for_response(client, cmd, &entry))
    return redis_response_printf(cmd, RESPONSE_ERROR, "ERR channel error on send (%d, %d)", client->sock->error1, client->sock->error2);

  // if the client shows an error, some dude upstream caused an error, but the
  // command had already been pipelined so we have to fail :(
  redis_response* response;
  if (client->sock->error1) {
    response = redis_response_printf(cmd, RESPONSE_ERROR, "ERR upstream channel error (%d, %d)", client->sock->error1, client->sock->error2);
  } else {
    // yay we can read now
    int error = setjmp(client->sock->error_jmp);
    if (!error)
      response = redis_receive_response(cmd, client->sock);
    else
      response = redis_response_printf(cmd, RESPONSE_ERROR, "ERR channel error on receive (%d, %d)", client->sock->error1, client->sock->error2);
  }

  redis_client_notify_next_thread(client, &entry);
  return response;
}
