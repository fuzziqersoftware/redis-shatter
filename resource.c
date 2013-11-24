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
int total_num_resources = 0;
int total_num_refs = 0;
#endif

void resource_add_ref(void* _r, void* _target) {

  resource* r = (resource*)_r;
  resource* target = (resource*)_target;

  if (r->num_outbound_refs >= r->outbound_refs_space) {
    if (r->outbound_refs_space == 0)
      r->outbound_refs_space = 16;
    else
      r->outbound_refs_space <<= 1;
    r->outbound_refs = (resource**)realloc(r->outbound_refs,
        r->outbound_refs_space * sizeof(resource*));
    if (!r->outbound_refs) {
      printf("error: cannot allocate space for references\n");
      debug_abort_stacktrace();
    }
  }

  r->outbound_refs[r->num_outbound_refs] = target;
  r->num_outbound_refs++;
  target->num_inbound_refs++;

#ifdef DEBUG_RESOURCES
  total_num_refs++;
  printf("(%d, %d) resource: added reference %p+%p(>%lld/%lld>) -> %p(>%lld/%lld>)\n",
      total_num_resources, total_num_refs, r, r->outbound_refs,
      r->num_inbound_refs, r->num_outbound_refs, target,
      target->num_inbound_refs, target->num_outbound_refs);
#endif
}

void resource_delete_ref(void* _r, void* _target) {

  resource* r = (resource*)_r;
  resource* target = (resource*)_target;

  // TODO: binary search that shit, or use a hash set (maybe better if there
  // tend to be a lot of refs)
  int x;
  for (x = 0; x < r->num_outbound_refs; x++)
    if (r->outbound_refs[x] == target)
      break;
  if (x >= r->num_outbound_refs) {
    printf("error: deleting reference that does not exist\n");
    debug_abort_stacktrace();
  }
  r->num_outbound_refs--;
  target->num_inbound_refs--;
  memcpy(&r->outbound_refs[x], &r->outbound_refs[x + 1],
      sizeof(struct resource*) * (r->num_outbound_refs - x));

#ifdef DEBUG_RESOURCES
  total_num_refs--;
  printf("(%d, %d) resource: removed reference %p+%p(>%lld/%lld>) -> %p(>%lld/%lld>)\n",
      total_num_resources, total_num_refs, r, r->outbound_refs,
      r->num_inbound_refs, r->num_outbound_refs, target,
      target->num_inbound_refs, target->num_outbound_refs);
#endif

  if (target->num_inbound_refs < 0)
    debug_abort_stacktrace();
  if (target->num_inbound_refs == 0)
    resource_delete(target, 0);
}

void resource_create(void* _parent, void* _r, void* free_fn) {

  resource* parent = (resource*)_parent;
  resource* r = (resource*)_r;

  r->num_inbound_refs = (_parent ? 0 : 1);
  r->num_outbound_refs = 0;
  r->outbound_refs_space = 0;
  r->outbound_refs = NULL;
  r->free = (void (*)(void*))free_fn;

#ifdef DEBUG_RESOURCES
  total_num_resources++;
  printf("(%d, %d) resource: created %p+%p(>%lld explicit)\n",
      total_num_resources, total_num_refs, r, r->outbound_refs,
      r->num_inbound_refs);
#endif

  if (parent)
    resource_add_ref(parent, r);
}

void resource_delete(void* _r, int num_explicit_refs) {

  resource* r = (resource*)_r;

  if (r->num_inbound_refs != num_explicit_refs) {
    // oh fuck
    printf("error: deleting resource with refcount == %lld\n",
        r->num_inbound_refs);
    debug_abort_stacktrace();
  }

#ifdef DEBUG_RESOURCES
  total_num_resources--;
  printf("(%d, %d) resource: deleting %p+%p(>%lld explicit)\n",
      total_num_resources, total_num_refs, r, r->outbound_refs,
      r->num_inbound_refs);
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
