#ifndef __RESOURCE_H
#define __RESOURCE_H

#include <pthread.h>
#include <stdint.h>

typedef struct resource {
  pthread_mutex_t mutex;
  uint64_t num_inbound_refs;
  uint64_t num_outbound_refs;
  uint64_t outbound_refs_space;
  struct resource** outbound_refs;
  void (*free)(void*);
} resource;

void resource_create(void* parent, void* r, void* free);
void resource_delete(void* r, int num_explicit_refs);

void resource_add_ref(void* r, void* target);
void resource_delete_ref(void* r, void* target);

#endif // __RESOURCE_H
