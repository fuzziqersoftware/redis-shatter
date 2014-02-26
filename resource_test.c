#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "debug.h"
#include "resource.h"

#define test_assert(cond) { \
  if (!(cond)) { \
    num_failures++; \
    printf("failed (%s:%d): %s\n", __FILE__, __LINE__, (#cond)); \
  } \
}

int num_freed_resources;
void* freed_resources[10];

int index_for_freed_resource(void* res) {
  int x;
  for (x = 0; x < num_freed_resources; x++)
    if (freed_resources[x] == res)
      return x;
  return (-1);
}

void free_resource(void* res) {
  freed_resources[num_freed_resources] = res;
  num_freed_resources++;
}

#define check_counts(extra_count, extra_refs) { \
  test_assert(base_num_resources + extra_count == resource_count()); \
  test_assert(base_resource_refs + extra_refs == resource_refcount()); }

#define check_counts_and_size(extra_count, extra_refs) { \
  check_counts(extra_count, extra_refs); \
  test_assert(base_resource_size == resource_size()); }

int main(int argc, char* argv[]) {

  printf("resource tests\n");
  int num_failures = 0;

  int64_t base_num_resources = resource_count();
  int64_t base_resource_refs = resource_refcount();
  int64_t base_resource_size = resource_size();
  {
    printf("-- simple create & delete, no parent\n");
    num_freed_resources = 0;
    struct resource res;
    resource_create(NULL, &res, free_resource);
    check_counts(1, 0);
    test_assert(num_freed_resources == 0);
    resource_delete(&res, 1);
    test_assert(num_freed_resources == 1);
    test_assert(freed_resources[0] == &res);
    check_counts_and_size(0, 0);
  }


  {
    printf("-- create & delete, no free function\n");
    // basically just make sure it doesn't segfault
    num_freed_resources = 0;
    struct resource res;
    resource_create(NULL, &res, NULL);
    check_counts(1, 0);
    test_assert(num_freed_resources == 0);
    resource_delete(&res, 1);
    test_assert(num_freed_resources == 0);
    check_counts_and_size(0, 0);
  }

  {
    printf("-- create & delete parent only\n");
    num_freed_resources = 0;
    struct resource parent, res;
    resource_create(NULL, &parent, free_resource);
    check_counts(1, 0);
    resource_create(&parent, &res, free_resource);
    check_counts(2, 1);
    test_assert(num_freed_resources == 0);
    resource_delete(&parent, 1);
    test_assert(num_freed_resources == 2);
    test_assert(freed_resources[0] == &res); // child should be deleted first
    test_assert(freed_resources[1] == &parent);
    check_counts_and_size(0, 0);
  }

  {
    printf("-- create, addref, delete parent & explicit ref behavior\n");
    num_freed_resources = 0;
    struct resource parent, res;
    resource_create(NULL, &parent, free_resource);
    check_counts(1, 0);
    resource_create(NULL, &res, free_resource);
    check_counts(2, 0);
    test_assert(num_freed_resources == 0);
    resource_add_ref(&parent, &res);
    check_counts(2, 1);
    test_assert(num_freed_resources == 0);
    resource_delete(&parent, 1);
    check_counts(1, 0);
    test_assert(num_freed_resources == 1);
    test_assert(freed_resources[0] == &parent); // res should not be deleted; it has an explicit inbound ref
    resource_delete(&res, 1);
    test_assert(num_freed_resources == 2);
    test_assert(freed_resources[0] == &parent);
    test_assert(freed_resources[1] == &res);
    check_counts_and_size(0, 0);
  }

  {
    printf("-- delete tree of 5 resources\n");
    num_freed_resources = 0;
    struct resource r1, r2, r3, r4, r5;
    resource_create(NULL, &r1, free_resource);
    check_counts(1, 0);
    resource_create(&r1, &r2, free_resource);
    check_counts(2, 1);
    resource_create(&r2, &r3, free_resource);
    check_counts(3, 2);
    resource_create(&r2, &r4, free_resource);
    check_counts(4, 3);
    resource_create(&r1, &r5, free_resource);
    check_counts(5, 4);
    test_assert(num_freed_resources == 0);
    resource_delete(&r1, 1);
    test_assert(num_freed_resources == 5);
    // make sure resources are deleted only after everything they reference
    test_assert(index_for_freed_resource(&r5) < index_for_freed_resource(&r1));
    test_assert(index_for_freed_resource(&r4) < index_for_freed_resource(&r2));
    test_assert(index_for_freed_resource(&r3) < index_for_freed_resource(&r2));
    test_assert(index_for_freed_resource(&r2) < index_for_freed_resource(&r1));
    check_counts_and_size(0, 0);
  }

  {
    printf("-- delete resources with multiple references\n");
    num_freed_resources = 0;
    struct resource parent, res;
    resource_create(NULL, &parent, free_resource);
    check_counts(1, 0);
    resource_create(&parent, &res, free_resource);
    check_counts(2, 1);
    test_assert(num_freed_resources == 0);
    resource_add_ref(&parent, &res);
    check_counts(2, 2);
    resource_add_ref(&parent, &res);
    check_counts(2, 3);
    test_assert(num_freed_resources == 0);
    resource_delete(&parent, 1);
    test_assert(num_freed_resources == 2);
    test_assert(freed_resources[0] == &res); // child should be deleted first
    test_assert(freed_resources[1] == &parent);
    check_counts_and_size(0, 0);
  }

  {
    printf("-- resource_delete_ref can cause resource deletion\n");
    num_freed_resources = 0;
    struct resource parent, res;
    resource_create(NULL, &parent, free_resource);
    check_counts(1, 0);
    resource_create(&parent, &res, free_resource);
    check_counts(2, 1);
    test_assert(num_freed_resources == 0);
    resource_add_ref(&parent, &res);
    check_counts(2, 2);
    resource_add_ref(&parent, &res);
    check_counts(2, 3);
    test_assert(num_freed_resources == 0);
    resource_delete_ref(&parent, &res);
    check_counts(2, 2);
    test_assert(num_freed_resources == 0);
    resource_delete_ref(&parent, &res);
    check_counts(2, 1);
    test_assert(num_freed_resources == 0);
    resource_delete_ref(&parent, &res);
    check_counts(1, 0);
    test_assert(num_freed_resources == 1);
    test_assert(freed_resources[0] == &res);
    resource_delete(&parent, 1);
    test_assert(num_freed_resources == 2);
    test_assert(freed_resources[0] == &res); // child should be deleted first
    test_assert(freed_resources[1] == &parent);
    check_counts_and_size(0, 0);
  }

  if (num_failures)
    printf("%d failures during test run\n", num_failures);
  else
    printf("all tests passed\n");

  return 0;
}
