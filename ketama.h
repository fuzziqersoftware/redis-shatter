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

uint16_t ketama_server_hash(const char* host, uint16_t point);
ketama_continuum* ketama_create_continuum(void* resource_parent, int num_hosts, const char** hosts);
void ketama_delete_continuum(ketama_continuum* c);
uint8_t ketama_server_for_key(ketama_continuum* c, const char* key);
const char* ketama_hostname_for_point(ketama_continuum* c, int host_index);

#endif // __KETAMA_H
