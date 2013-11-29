#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

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

int main(int argc, char* argv[]) {

  printf("resource tests\n");
  int num_failures = 0;

  {
    printf("-- simple create & delete, no parent\n");
    num_freed_resources = 0;
    resource res;
    resource_create(NULL, &res, free_resource);
    test_assert(num_freed_resources == 0);
    resource_delete(&res, 1);
    test_assert(num_freed_resources == 1);
    test_assert(freed_resources[0] == &res);
  }

  {
    printf("-- create & delete, no free function\n");
    // basically just make sure it doesn't segfault
    num_freed_resources = 0;
    resource res;
    resource_create(NULL, &res, NULL);
    test_assert(num_freed_resources == 0);
    resource_delete(&res, 1);
    test_assert(num_freed_resources == 0);
  }

  {
    printf("-- create & delete parent only\n");
    num_freed_resources = 0;
    resource parent, res;
    resource_create(NULL, &parent, free_resource);
    resource_create(&parent, &res, free_resource);
    test_assert(num_freed_resources == 0);
    resource_delete(&parent, 1);
    test_assert(num_freed_resources == 2);
    test_assert(freed_resources[0] == &res); // child should be deleted first
    test_assert(freed_resources[1] == &parent);
  }

  {
    printf("-- create, addref, delete parent & explicit ref behavior\n");
    num_freed_resources = 0;
    resource parent, res;
    resource_create(NULL, &parent, free_resource);
    resource_create(NULL, &res, free_resource);
    test_assert(num_freed_resources == 0);
    resource_add_ref(&parent, &res);
    test_assert(num_freed_resources == 0);
    resource_delete(&parent, 1);
    test_assert(num_freed_resources == 1);
    test_assert(freed_resources[0] == &parent); // res should not be deleted; it has an explicit inbound ref
    resource_delete(&res, 1);
    test_assert(num_freed_resources == 2);
    test_assert(freed_resources[0] == &parent);
    test_assert(freed_resources[1] == &res);
  }

  {
    printf("-- delete tree of 5 resources\n");
    num_freed_resources = 0;
    resource r1, r2, r3, r4, r5;
    resource_create(NULL, &r1, free_resource);
    resource_create(&r1, &r2, free_resource);
    resource_create(&r2, &r3, free_resource);
    resource_create(&r2, &r4, free_resource);
    resource_create(&r1, &r5, free_resource);
    test_assert(num_freed_resources == 0);
    resource_delete(&r1, 1);
    test_assert(num_freed_resources == 5);
    // make sure resources are deleted only after everything they reference
    test_assert(index_for_freed_resource(&r5) < index_for_freed_resource(&r1));
    test_assert(index_for_freed_resource(&r4) < index_for_freed_resource(&r2));
    test_assert(index_for_freed_resource(&r3) < index_for_freed_resource(&r2));
    test_assert(index_for_freed_resource(&r2) < index_for_freed_resource(&r1));
  }

  {
    printf("-- delete resources with multiple references\n");
    num_freed_resources = 0;
    resource parent, res;
    resource_create(NULL, &parent, free_resource);
    resource_create(&parent, &res, free_resource);
    test_assert(num_freed_resources == 0);
    resource_add_ref(&parent, &res);
    resource_add_ref(&parent, &res);
    test_assert(num_freed_resources == 0);
    resource_delete(&parent, 1);
    test_assert(num_freed_resources == 2);
    test_assert(freed_resources[0] == &res); // child should be deleted first
    test_assert(freed_resources[1] == &parent);
  }

  {
    printf("-- resource_delete_ref can cause resource deletion\n");
    num_freed_resources = 0;
    resource parent, res;
    resource_create(NULL, &parent, free_resource);
    resource_create(&parent, &res, free_resource);
    test_assert(num_freed_resources == 0);
    resource_add_ref(&parent, &res);
    resource_add_ref(&parent, &res);
    test_assert(num_freed_resources == 0);
    resource_delete_ref(&parent, &res);
    test_assert(num_freed_resources == 0);
    resource_delete_ref(&parent, &res);
    test_assert(num_freed_resources == 0);
    resource_delete_ref(&parent, &res);
    test_assert(num_freed_resources == 1);
    test_assert(freed_resources[0] == &res);
    resource_delete(&parent, 1);
    test_assert(num_freed_resources == 2);
    test_assert(freed_resources[0] == &res); // child should be deleted first
    test_assert(freed_resources[1] == &parent);
  }

  if (num_failures)
    printf("%d failures during test run\n", num_failures);
  else
    printf("all tests passed\n");

  return 0;
}
