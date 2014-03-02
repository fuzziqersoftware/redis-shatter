#ifndef __RESOURCE_H
#define __RESOURCE_H

#include <pthread.h>
#include <stdint.h>

struct resource {
  pthread_mutex_t mutex;
  uint64_t num_inbound_refs;
  uint64_t num_outbound_refs;
  uint64_t outbound_refs_space;
  struct resource** outbound_refs;
  void (*free)(void*);
#ifdef DEBUG_RESOURCES
  char annotation[0x100];
#endif

  uint8_t data[0];
};

void resource_create(void* parent, void* r, void* free_fn);
void resource_delete(void* r, int num_explicit_refs);
int64_t resource_count();
int64_t resource_refcount();
int64_t resource_size();

void resource_add_ref(void* r, void* target);
void resource_delete_ref(void* r, void* target);
void resource_delete_explicit_ref(void* r);

void print_resource_tree(void* root);

struct resource* resource_malloc(void* parent, int size, void* free_fn);
struct resource* resource_calloc(void* parent, int size, void* free_fn);
void* resource_malloc_raw(void* parent, int size, void* free_fn);
void* resource_calloc_raw(void* parent, int size, void* free_fn);

#define resource_create_var(local_res, type, name, size) \
  struct resource* name##_resource = resource_calloc(local_res, size); \
  type name = name##_resource ? (type)&name##_resource->data[0] : NULL

#define resource_create_array(local_res, type, name, count) \
  resource_create_var(local_res, type*, name, sizeof(type) * (count))

#ifdef DEBUG_RESOURCES
#define resource_annotate(r, ...) sprintf(((struct resource*)(r))->annotation, __VA_ARGS__)
#else
#define resource_annotate(r, ...)
#endif

#endif // __RESOURCE_H
