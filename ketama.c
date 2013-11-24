#include <stdint.h>
#include <string.h>
#include <stdlib.h>

#include "resource.h"
#include "ketama.h"



uint64_t fnv1a_64_continue_string(const char* data, uint64_t hash) {
  const uint8_t *data_ptr = (const uint8_t*)data;
  for (; *data_ptr != 0; data_ptr++)
    hash = (hash ^ (uint64_t)*data_ptr) * 0x00000100000001B3;
  return hash;
}

uint64_t fnv1a_64_continue(const void* data, uint64_t size, uint64_t hash) {
  const uint8_t *data_ptr = (const uint8_t*)data;
  const uint8_t *end_ptr = data_ptr + size;

  for (; data_ptr != end_ptr; data_ptr++)
    hash = (hash ^ (uint64_t)*data_ptr) * 0x00000100000001B3;
  return hash;
}

uint64_t fnv1a_64_start(const void* data, uint64_t size) {
  return fnv1a_64_continue(data, size, 0xCBF29CE484222325);
}



#define POINTS_PER_HOST 160 // must be divisible by 4

static uint16_t ketama_server_hash(void* key, int64_t size, uint16_t pt) {
  return fnv1a_64_continue(key, size, fnv1a_64_start(&pt, sizeof(uint16_t)));
}

ketama_continuum* ketama_continuum_create(void* resource_parent, int num_hosts, const char** hosts) {
  if (num_hosts > 254)
    return NULL; // too many hosts; need to use an int16_t or something

  int x, y, host_space_needed = 0;
  for (x = 0; x < num_hosts; x++)
    host_space_needed += (strlen(hosts[x]) + 1);

  ketama_continuum* c = (ketama_continuum*)malloc(sizeof(ketama_continuum) +
      sizeof(char*) * num_hosts + host_space_needed);
  if (!c)
    return NULL;
  resource_create(resource_parent, c, ketama_continuum_delete);
  c->num_hosts = num_hosts;
  memset(c->points, 0xFF, 65536);

  char* host_ptr = (char*)(&c->hosts[c->num_hosts]);
  for (x = 0; x < c->num_hosts; x++) {
    strcpy(host_ptr, hosts[x]);
    c->hosts[x] = host_ptr;

    for (y = 0; y < POINTS_PER_HOST; y++)
      c->points[ketama_server_hash(host_ptr, strlen(host_ptr), y)] = x;

    host_ptr += (strlen(host_ptr) + 1);
  }

  uint8_t current_host = 0xFF;
  for (x = 0xFFFF; (x > 0) && (current_host == 0xFF); x--)
    if (c->points[x] != 0xFF)
      current_host = c->points[x];

  if (current_host == 0xFF)
    return c; // you asked us to make an empty continuum, so we did :(

  for (x = 0; x < 0x10000; x++) {
    if (c->points[x] == 0xFF)
      c->points[x] = current_host;
    else
      current_host = c->points[x];
  }

  return c;
}

void ketama_continuum_delete(ketama_continuum* c) {
  free(c);
}

uint8_t ketama_server_for_key(ketama_continuum* c, void* key, int64_t size) {
  return c->points[ketama_server_hash(key, size, 0)];
}

const char* ketama_hostname_for_point(ketama_continuum* c, int host_index) {
  return c->hosts[host_index];
}
