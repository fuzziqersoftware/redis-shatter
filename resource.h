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

  uint8_t data[0];
} resource;

void resource_create(void* parent, void* r, void* free);
void resource_delete(void* r, int num_explicit_refs);

void resource_add_ref(void* r, void* target);
void resource_delete_ref(void* r, void* target);

resource* resource_malloc(void* parent, int size);
resource* resource_calloc(void* parent, int size);

#define resource_create_var(local_res, type, name, size) \
  resource* __##name##__res = resource_calloc(local_res, size); \
  type name = __##name##__res ? (type)&__##name##__res->data[0] : NULL

#define resource_create_array(local_res, type, name, count) \
  resource_create_var(local_res, type*, name, sizeof(type) * (count))

#endif // __RESOURCE_H
