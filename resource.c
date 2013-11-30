#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>

#include "debug.h"
#include "redis_socket.h"



// base resources (i.e. those whose only inbound reference is from a stack)
//   have refcount=0, but are never autofreed since they have no inbound refs

#ifdef DEBUG_RESOURCES
pthread_mutex_t all_resources_lock;
int total_num_resources = 0;
int total_num_refs = 0;
resource** all_resources = NULL;
#endif

void resource_add_ref(void* _r, void* _target) {

  resource* r = (resource*)_r;
  resource* target = (resource*)_target;

  pthread_mutex_lock(&r->mutex);
  pthread_mutex_lock(&target->mutex);

  if (r->num_outbound_refs >= r->outbound_refs_space) {
    if (r->outbound_refs_space == 0)
      r->outbound_refs_space = 16;
    else
      r->outbound_refs_space <<= 1;
    r->outbound_refs = (resource**)realloc(r->outbound_refs,
        r->outbound_refs_space * sizeof(resource*));
    if (!r->outbound_refs) {
      printf("[%016llX] error: cannot allocate space for references\n",
          (uint64_t)pthread_self());
      debug_abort_stacktrace();
    }
  }

  r->outbound_refs[r->num_outbound_refs] = target;
  r->num_outbound_refs++;
  target->num_inbound_refs++;

#ifdef DEBUG_RESOURCES
  if (all_resources == NULL)
    pthread_mutex_init(&all_resources_lock, NULL);
  pthread_mutex_lock(&all_resources_lock);
  total_num_refs++;
  printf("[%016llX] (%d, %d) resource: added reference %p\"%s\"(>%lld/%lld>) -> %p\"%s\"(>%lld/%lld>)\n",
      (int64_t)pthread_self(), total_num_resources, total_num_refs, r, r->annotation,
      r->num_inbound_refs, r->num_outbound_refs, target, target->annotation,
      target->num_inbound_refs, target->num_outbound_refs);
  pthread_mutex_unlock(&all_resources_lock);
#endif

  pthread_mutex_unlock(&target->mutex);
  pthread_mutex_unlock(&r->mutex);
}

void resource_delete_ref(void* _r, void* _target) {

  resource* r = (resource*)_r;
  resource* target = (resource*)_target;

  pthread_mutex_lock(&r->mutex);
  pthread_mutex_lock(&target->mutex);

  // TODO: binary search that shit, or use a hash set (maybe better if there
  // tend to be a lot of refs)
  int x;
  for (x = 0; x < r->num_outbound_refs; x++)
    if (r->outbound_refs[x] == target)
      break;
  if (x >= r->num_outbound_refs) {
    printf("[%016llX] error: deleting reference that does not exist: %p->%p\n",
        (uint64_t)pthread_self(), r, target);
    debug_abort_stacktrace();
  }
  r->num_outbound_refs--;
  target->num_inbound_refs--;
  memcpy(&r->outbound_refs[x], &r->outbound_refs[x + 1],
      sizeof(struct resource*) * (r->num_outbound_refs - x));

#ifdef DEBUG_RESOURCES
  if (all_resources == NULL)
    pthread_mutex_init(&all_resources_lock, NULL);
  pthread_mutex_lock(&all_resources_lock);
  total_num_refs--;
  printf("[%016llX] (%d, %d) resource: removed reference %p\"%s\"(>%lld/%lld>) -> %p\"%s\"(>%lld/%lld>)\n",
      pthread_self(), total_num_resources, total_num_refs, r, r->annotation,
      r->num_inbound_refs, r->num_outbound_refs, target, target->annotation,
      target->num_inbound_refs, target->num_outbound_refs);
  pthread_mutex_unlock(&all_resources_lock);
#endif

  if (target->num_inbound_refs < 0)
    debug_abort_stacktrace();

  pthread_mutex_unlock(&target->mutex);
  pthread_mutex_unlock(&r->mutex);

  // theoretically it should be safe to do this outside the mutex; if the
  // refcount is zero then no other thread should be able to touch this object
  if (target->num_inbound_refs == 0)
    resource_delete(target, 0);
}

void resource_create(void* _parent, void* _r, void* free_fn) {

  resource* parent = (resource*)_parent;
  resource* r = (resource*)_r;

  pthread_mutex_init(&r->mutex, NULL);
  r->num_inbound_refs = (_parent ? 0 : 1);
  r->num_outbound_refs = 0;
  r->outbound_refs_space = 0;
  r->outbound_refs = NULL;
  r->free = (void (*)(void*))free_fn;

#ifdef DEBUG_RESOURCES
  r->annotation[0] = 0;

  if (all_resources == NULL)
    pthread_mutex_init(&all_resources_lock, NULL);
  pthread_mutex_lock(&all_resources_lock);
  total_num_resources++;
  printf("[%016llX] (%d, %d) resource: created %p\"%s\"(>%lld explicit)\n",
      pthread_self(), total_num_resources, total_num_refs, r, r->annotation,
      r->num_inbound_refs);

  all_resources = (resource**)realloc(all_resources, sizeof(resource*) * total_num_resources);
  all_resources[total_num_resources - 1] = r;
  pthread_mutex_unlock(&all_resources_lock);
#endif

  if (parent)
    resource_add_ref(parent, r);
}

void resource_delete(void* _r, int num_explicit_refs) {

  resource* r = (resource*)_r;

  if (r->num_inbound_refs != num_explicit_refs) {
    // oh fuck
    printf("[%016llX] error: deleting resource %p with refcount == %lld\n",
        (uint64_t)pthread_self(), r, r->num_inbound_refs);
    debug_abort_stacktrace();
  }

#ifdef DEBUG_RESOURCES
  if (all_resources == NULL)
    pthread_mutex_init(&all_resources_lock, NULL);
  pthread_mutex_lock(&all_resources_lock);

  int x;
  for (x = 0; x < total_num_resources; x++)
    if (all_resources[x] == r)
      break;
  if (x == total_num_resources) {
    printf("[%016llX] error: deleting resource that does not exist: %p\"%s\"\n",
        pthread_self(), r, r->annotation);
    debug_abort_stacktrace();
  }
  total_num_resources--;
  memcpy(&all_resources[x], &all_resources[x + 1], sizeof(resource*) * (total_num_resources - x));
  all_resources[total_num_resources] = NULL;

  printf("[%016llX] (%d, %d) resource: deleting %p\"%s\"(>%lld explicit)\n",
      pthread_self(), total_num_resources, total_num_refs, r, r->annotation,
      r->num_inbound_refs);
  pthread_mutex_unlock(&all_resources_lock);
#endif

  while (r->num_outbound_refs)
    resource_delete_ref(r, r->outbound_refs[r->num_outbound_refs - 1]);

  if (r->outbound_refs)
    free(r->outbound_refs);
  if (r->free)
    r->free(r);
}

resource* resource_malloc(void* parent, int size) {
  size += sizeof(resource);
  resource* r = (resource*)malloc(size);
  if (!r)
    return NULL;
  resource_create(parent, r, free);
  return r;
}

resource* resource_calloc(void* parent, int size) {
  resource* r = resource_malloc(parent, size);
  if (r)
    memset(&r->data, 0, size);
  return r;
}
