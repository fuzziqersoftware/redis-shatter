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
#include "resource.h"



#ifdef THREADED_RESOURCES

static void resource_init_lock(pthread_mutex_t* m) {
  pthread_mutex_init(m, NULL);
}

static void resource_lock(pthread_mutex_t* m) {
  pthread_mutex_lock(m);
}

static void resource_unlock(pthread_mutex_t* m) {
  pthread_mutex_unlock(m);
}

#else

#define resource_init_lock(m)
#define resource_lock(m)
#define resource_unlock(m)

#endif



// base resources (i.e. those whose only inbound reference is from a stack)
//   have refcount=0, but are never autofreed since they have no inbound refs

static int64_t total_num_resources = 0;
static int64_t total_num_refs = 0;
static int64_t total_resource_size = 0;
static int resource_inited = 0;
#ifdef DEBUG_RESOURCES
static pthread_mutex_t all_resources_lock;
static struct resource** all_resources = NULL;
#endif

int64_t resource_count() {
  return total_num_resources;
}

int64_t resource_refcount() {
  return total_num_refs;
}

int64_t resource_size() {
  return total_resource_size;
}

static inline void resource_global_init() {
  if (!resource_inited) {
    resource_init_lock(&all_resources_lock);
    resource_inited = 1;
  }
}

void resource_add_ref(void* _r, void* _target) {

  struct resource* r = (struct resource*)_r;
  struct resource* target = (struct resource*)_target;

  resource_lock(&r->mutex);
  resource_lock(&target->mutex);

  if (r->num_outbound_refs >= r->outbound_refs_space) {
    if (r->outbound_refs_space == 0)
      r->outbound_refs_space = 16;
    else
      r->outbound_refs_space <<= 1;
    r->outbound_refs = (struct resource**)realloc(r->outbound_refs,
        r->outbound_refs_space * sizeof(struct resource*));
    if (!r->outbound_refs) {
      printf("[%016llX] error: cannot allocate space for references\n",
          (uint64_t)pthread_self());
      debug_abort_stacktrace();
    }
  }

  r->outbound_refs[r->num_outbound_refs] = target;
  r->num_outbound_refs++;
  target->num_inbound_refs++;

  resource_global_init();
  resource_lock(&all_resources_lock);
  total_num_refs++;

#ifdef DEBUG_RESOURCES
  printf("[%016llX] (%d, %d) resource: added reference %p\"%s\"(>%lld/%lld>) -> %p\"%s\"(>%lld/%lld>)\n",
      (int64_t)pthread_self(), total_num_resources, total_num_refs, r, r->annotation,
      r->num_inbound_refs, r->num_outbound_refs, target, target->annotation,
      target->num_inbound_refs, target->num_outbound_refs);
#endif
  resource_unlock(&all_resources_lock);

  resource_unlock(&target->mutex);
  resource_unlock(&r->mutex);
}

void resource_delete_ref(void* _r, void* _target) {

  struct resource* r = (struct resource*)_r;
  struct resource* target = (struct resource*)_target;

  resource_lock(&r->mutex);
  resource_lock(&target->mutex);

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

  resource_global_init();
  resource_lock(&all_resources_lock);
  total_num_refs--;

#ifdef DEBUG_RESOURCES
  printf("[%016llX] (%d, %d) resource: removed reference %p\"%s\"(>%lld/%lld>) -> %p\"%s\"(>%lld/%lld>)\n",
      (int64_t)pthread_self(), total_num_resources, total_num_refs, r, r->annotation,
      r->num_inbound_refs, r->num_outbound_refs, target, target->annotation,
      target->num_inbound_refs, target->num_outbound_refs);
#endif

  resource_unlock(&all_resources_lock);

  if (target->num_inbound_refs < 0)
    debug_abort_stacktrace();

  resource_unlock(&target->mutex);
  resource_unlock(&r->mutex);

  // theoretically it should be safe to do this outside the mutex; if the
  // refcount is zero then no other thread should be able to touch this object
  if (target->num_inbound_refs == 0)
    resource_delete(target, 0);
}

void resource_delete_explicit_ref(void* _r) {
  struct resource* r = (struct resource*)_r;
  r->num_inbound_refs--;

  if (r->num_inbound_refs == 0)
    resource_delete(r, 0);
}

void resource_create(void* _parent, void* _r, void* free_fn) {

  struct resource* parent = (struct resource*)_parent;
  struct resource* r = (struct resource*)_r;

  resource_init_lock(&r->mutex);
  r->num_inbound_refs = (_parent ? 0 : 1);
  r->num_outbound_refs = 0;
  r->outbound_refs_space = 0;
  r->outbound_refs = NULL;
  r->free = (void (*)(void*))free_fn;

  resource_global_init();
  resource_lock(&all_resources_lock);
  total_num_resources++;

#ifdef DEBUG_RESOURCES
  printf("[%016llX] (%d, %d) resource: created %p(>%lld explicit)\n",
      (int64_t)pthread_self(), total_num_resources, total_num_refs, r,
      r->num_inbound_refs);
  r->annotation[0] = 0;

  all_resources = (struct resource**)realloc(all_resources, sizeof(struct resource*) * total_num_resources);
  all_resources[total_num_resources - 1] = r;
#endif

  resource_unlock(&all_resources_lock);

  if (parent)
    resource_add_ref(parent, r);
}

void resource_delete(void* _r, int num_explicit_refs) {

  struct resource* r = (struct resource*)_r;

  if (r->num_inbound_refs != num_explicit_refs) {
    // oh fuck
    printf("[%016llX] error: deleting resource %p with refcount == %lld\n",
        (uint64_t)pthread_self(), r, r->num_inbound_refs);
    debug_abort_stacktrace();
  }

  resource_global_init();
  resource_lock(&all_resources_lock);

#ifdef DEBUG_RESOURCES
  int x;
  for (x = 0; x < total_num_resources; x++)
    if (all_resources[x] == r)
      break;
  if (x == total_num_resources) {
    printf("[%016llX] error: deleting resource that does not exist: %p\"%s\"\n",
        (int64_t)pthread_self(), r, r->annotation);
    debug_abort_stacktrace();
  }
#endif

  total_num_resources--;

#ifdef DEBUG_RESOURCES
  memcpy(&all_resources[x], &all_resources[x + 1], sizeof(struct resource*) * (total_num_resources - x));
  all_resources[total_num_resources] = NULL;

  printf("[%016llX] (%d, %d) resource: deleting %p\"%s\"(>%lld explicit)\n",
      (int64_t)pthread_self(), total_num_resources, total_num_refs, r, r->annotation,
      r->num_inbound_refs);
#endif
  resource_unlock(&all_resources_lock);

  while (r->num_outbound_refs)
    resource_delete_ref(r, r->outbound_refs[r->num_outbound_refs - 1]);

  if (r->outbound_refs)
    free(r->outbound_refs);
  if (r->free)
    r->free(r);
}

static void print_resource_tree_again(struct resource* root,
    int indent) {
  print_indent(indent);
#ifdef DEBUG_RESOURCES
  printf("%p\"%s\"\n", root, root->annotation);
#else
  printf("%p\n", root);
#endif
  int y;
  for (y = 0; y < root->num_outbound_refs; y++)
    print_resource_tree_again(root->outbound_refs[y], indent + 1);
}

void print_resource_tree(void* root) {
  print_resource_tree_again((struct resource*)root, 0);
}

struct resource* resource_malloc(void* parent, int size, void* free_fn) {
  size += sizeof(struct resource);
  struct resource* r = (struct resource*)malloc(size);
  if (!r)
    return NULL;
  resource_create(parent, r, free_fn);
  return r;
}

struct resource* resource_calloc(void* parent, int size, void* free_fn) {
  struct resource* r = resource_malloc(parent, size, free_fn);
  if (r)
    memset(&r->data, 0, size);
  return r;
}

void* resource_malloc_raw(void* parent, int size, void* free_fn) {
  struct resource* r = resource_malloc(parent, size, free_fn);
  if (!r)
    return NULL;
  resource_annotate(r, "resource_malloc_raw(%d)", size);
  return &r->data[0];
}

void* resource_calloc_raw(void* parent, int size, void* free_fn) {
  struct resource* r = resource_calloc(parent, size, free_fn);
  if (!r)
    return NULL;
  resource_annotate(r, "resource_calloc_raw(%d)", size);
  return &r->data[0];
}
