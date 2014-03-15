#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "debug.h"
#include "resource.h"



// base resources (i.e. those whose only inbound reference is from a stack)
// have refcount=0, but are never autofreed since they have no inbound refs

static int64_t total_num_resources = 0;
static int64_t total_num_refs = 0;
static int64_t total_resource_size = 0;
#ifdef DEBUG_RESOURCES
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



void resource_create(void* _parent, void* _r, void* free_fn) {

  struct resource* parent = (struct resource*)_parent;
  struct resource* r = (struct resource*)_r;

  r->num_inbound_refs = 0;
  r->num_outbound_refs = 0;
  r->outbound_refs_space = 0;
  r->outbound_refs = NULL;
  r->free = (void (*)(void*))free_fn;

  total_num_resources++;

#ifdef DEBUG_RESOURCES
  printf("(%d, %d) resource: created %p(>%lld explicit)\n",
      total_num_resources, total_num_refs, r, r->num_inbound_refs);
  r->annotation[0] = 0;

  all_resources = (struct resource**)realloc(all_resources, sizeof(struct resource*) * total_num_resources);
  all_resources[total_num_resources - 1] = r;
#endif

  resource_add_ref(parent, r);
}

static void resource_delete(void* _r, int num_explicit_refs) {

  struct resource* r = (struct resource*)_r;

  if (r->num_inbound_refs != num_explicit_refs) {
    // oh fuck
    printf("error: deleting resource %p with refcount == %llu\n", r,
        r->num_inbound_refs);
    debug_abort_stacktrace();
  }

#ifdef DEBUG_RESOURCES
  int x;
  for (x = 0; x < total_num_resources; x++)
    if (all_resources[x] == r)
      break;
  if (x == total_num_resources) {
    printf("error: deleting resource that does not exist: %p\"%s\"\n", r,
        r->annotation);
    debug_abort_stacktrace();
  }
#endif

  total_num_resources--;

#ifdef DEBUG_RESOURCES
  memmove(&all_resources[x], &all_resources[x + 1], sizeof(struct resource*) * (total_num_resources - x));
  all_resources[total_num_resources] = NULL;

  printf("(%d, %d) resource: deleting %p\"%s\"(>%lld explicit)\n",
      total_num_resources, total_num_refs, r, r->annotation,
      r->num_inbound_refs);
#endif

  while (r->num_outbound_refs)
    resource_delete_ref(r, r->outbound_refs[r->num_outbound_refs - 1]);

  if (r->outbound_refs)
    free(r->outbound_refs);
  if (r->free)
    r->free(r);
}



void resource_add_ref(void* _r, void* _target) {

  struct resource* r = (struct resource*)_r;
  struct resource* target = (struct resource*)_target;

  if (r) {
    if (r->num_outbound_refs >= r->outbound_refs_space) {
      if (r->outbound_refs_space == 0)
        r->outbound_refs_space = 16;
      else
        r->outbound_refs_space <<= 1;
      r->outbound_refs = (struct resource**)realloc(r->outbound_refs,
          r->outbound_refs_space * sizeof(struct resource*));
      if (!r->outbound_refs) {
        printf("error: cannot allocate space for references\n");
        debug_abort_stacktrace();
      }
    }
  r  ->outbound_refs[r->num_outbound_refs] = target;
    r->num_outbound_refs++;
  }

  target->num_inbound_refs++;
  total_num_refs++;

#ifdef DEBUG_RESOURCES
  if (r)
    printf("(%d, %d) resource: added reference %p\"%s\"(>%lld/%lld>) -> %p\"%s\"(>%lld/%lld>)\n",
        total_num_resources, total_num_refs, r, r->annotation,
        r->num_inbound_refs, r->num_outbound_refs, target, target->annotation,
        target->num_inbound_refs, target->num_outbound_refs);
  else
    printf("(%d, %d) resource: added reference NULL -> %p\"%s\"(>%lld/%lld>)\n",
        total_num_resources, total_num_refs, target, target->annotation,
        target->num_inbound_refs, target->num_outbound_refs);
#endif
}

void resource_delete_ref(void* _r, void* _target) {

  struct resource* r = (struct resource*)_r;
  struct resource* target = (struct resource*)_target;

  // if r is NULL, we're deleting an explicit (stack) ref
  if (r) {
    // we should do binary search or use a hash set here if there tend to be a
    // lot of refs per object - currently there aren't so this is fine
    int x;
    for (x = 0; x < r->num_outbound_refs; x++)
      if (r->outbound_refs[x] == target)
        break;
    if (x >= r->num_outbound_refs) {
      printf("error: deleting reference that does not exist: %p->%p\n", r,
          target);
      debug_abort_stacktrace();
    }
    r->num_outbound_refs--;
    memmove(&r->outbound_refs[x], &r->outbound_refs[x + 1],
        sizeof(struct resource*) * (r->num_outbound_refs - x));
  }

  target->num_inbound_refs--;
  total_num_refs--;

#ifdef DEBUG_RESOURCES
  if (r)
    printf("(%d, %d) resource: removed reference %p\"%s\"(>%lld/%lld>) -> %p\"%s\"(>%lld/%lld>)\n",
        total_num_resources, total_num_refs, r, r->annotation,
        r->num_inbound_refs, r->num_outbound_refs, target, target->annotation,
        target->num_inbound_refs, target->num_outbound_refs);
  else
    printf("(%d, %d) resource: removed reference NULL -> %p\"%s\"(>%lld/%lld>)\n",
        total_num_resources, total_num_refs, target, target->annotation,
        target->num_inbound_refs, target->num_outbound_refs);
#endif

  if (target->num_inbound_refs < 0)
    debug_abort_stacktrace();

  // theoretically it should be safe to do this outside the mutex; if the
  // refcount is zero then no other thread should be able to touch this object
  if (target->num_inbound_refs == 0)
    resource_delete(target, 0);
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

struct resource* resource_strdup(void* parent, const char* s, void* free_fn) {
  int len = strlen(s);
  struct resource* r = resource_malloc(parent, len + 1, free_fn);
  if (r)
    strcpy((char*)&r->data, s);
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

char* resource_strdup_raw(void* parent, const char* s, void* free_fn) {
  struct resource* r = resource_strdup(parent, s, free_fn);
  if (!r)
    return NULL;
  resource_annotate(r, "resource_strdup");
  return (char*)&r->data[0];
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
