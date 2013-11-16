#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "debug.h"
#include "ketama.h"

#define test_assert(cond) { \
  if (!(cond)) { \
    num_failures++; \
    printf("failed (%s:%d): %s\n", __FILE__, __LINE__, (#cond)); \
  } \
}

int main(int argc, char* argv[]) {

  printf("ketama tests\n");
  int x, num_failures = 0;

  {
    printf("-- create empty continuum\n");
    ketama_continuum* c = ketama_create_continuum(0, NULL);
    test_assert(c->num_hosts == 0);
    for (x = 0; x < 0x10000; x++)
      test_assert(c->points[x] == 0xFF);
    ketama_delete_continuum(c);
  }

  {
    printf("-- create continuum with one host\n");
    const char* hosts[1] = {"host1:8080"};
    ketama_continuum* c = ketama_create_continuum(1, hosts);
    test_assert(c->num_hosts == 1);
    for (x = 0; x < 0x10000; x++)
      test_assert(c->points[x] == 0);
    ketama_delete_continuum(c);
  }

  {
    printf("-- make sure all points are set\n");
    const char* hosts[3] = {"host1:8080", "host2:8080", "host3:8080"};
    ketama_continuum* c = ketama_create_continuum(3, hosts);
    test_assert(c->num_hosts == 3);
    for (x = 0; x < 0x10000; x++)
      test_assert(c->points[x] <= 2);
    ketama_delete_continuum(c);
  }

  {
    printf("-- create continuum with two hosts and check point balance\n");
    const char* hosts[2] = {"host1:8080", "host2:8080"};
    ketama_continuum* c = ketama_create_continuum(2, hosts);
    test_assert(c->num_hosts == 2);
    int host_counts[2] = {0, 0};
    for (x = 0; x < 0x10000; x++) {
      test_assert(!(c->points[x] & ~1)); // better be 0 or 1
      host_counts[c->points[x]]++;
    }
    test_assert(host_counts[0] + host_counts[1] == 0x10000);
    test_assert(abs(host_counts[0] - host_counts[1]) < 0x1000); // allow 1/16th deviation
    ketama_delete_continuum(c);
  }

  {
    printf("-- check host removal affecting other hosts\n");
    const char* hosts[3] = {"host1:8080", "host2:8080", "host3:8080"};
    ketama_continuum* c1 = ketama_create_continuum(3, hosts);
    ketama_continuum* c2 = ketama_create_continuum(2, hosts); // host3 was removed
    test_assert(c1->num_hosts == 3);
    test_assert(c2->num_hosts == 2);
    for (x = 0; x < 0x10000; x++) {
      if (c1->points[x] == 2) {
        test_assert(c2->points[x] < 2);
      } else {
        test_assert(c1->points[x] == c2->points[x]);
      }
    }
    ketama_delete_continuum(c1);
    ketama_delete_continuum(c2);
  }

  if (num_failures)
    printf("%d failures during test run\n", num_failures);
  else
    printf("all tests passed\n");

  return 0;
}
