#ifndef __KETAMA_H
#define __KETAMA_H

#include <stdint.h>

#include "resource.h"



uint64_t fnv1a_64_continue_string(const char* data, uint64_t hash);
uint64_t fnv1a_64_continue(const void* data, uint64_t size, uint64_t hash);
uint64_t fnv1a_64_start(const void* data, uint64_t size);



typedef struct {
  resource res;
  int num_hosts;
  uint8_t points[0x10000];
  char* hosts[0];
} ketama_continuum;

ketama_continuum* ketama_continuum_create(void* resource_parent, int num_hosts, const char** hosts);
void ketama_continuum_delete(ketama_continuum* c);
uint8_t ketama_server_for_key(ketama_continuum* c, void* key, int64_t size);
const char* ketama_hostname_for_point(ketama_continuum* c, int host_index);

#endif // __KETAMA_H
