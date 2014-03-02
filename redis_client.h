#ifndef __REDIS_SOCKET_H
#define __REDIS_SOCKET_H

#include <event2/bufferevent.h>
#include <netinet/in.h>
#include <stdint.h>

#include "resource.h"
#include "redis_protocol.h"

#define ERROR_ASSERTION_FAILED     1
#define ERROR_OUT_OF_MEMORY        2

#define CWAIT_FORWARD_RESPONSE                   0
#define CWAIT_COLLECT_STATUS_RESPONSES           1
#define CWAIT_SUM_INT_RESPONSES                  2
#define CWAIT_COMBINE_MULTI_RESPONSES            3
#define CWAIT_COLLECT_RESPONSES                  4
#define CWAIT_COLLECT_MULTI_RESPONSES_BY_KEY     5
#define CWAIT_COLLECT_IDENTICAL_RESPONSES        6

struct redis_client;

struct redis_client_expected_response {
  struct resource res;

  struct redis_client* client;
  struct redis_client_expected_response* next_wait;
  struct redis_client_expected_response* next_response;

  struct redis_command* original_command;
  int wait_type;
  int num_responses_expected;
  struct redis_response* error_response;

  struct redis_response* response_to_forward;
  int64_t int_sum;
  struct {
    int num_responses;
    int expected_response_type;
    struct redis_response* responses[0];
  } collect_multi;
  struct {
  	int num_keys;
  	int args_per_key;
    int* server_to_key_count;
    uint8_t* key_to_server;
    struct redis_command** index_to_command;
    struct redis_response* response_in_progress;
  } collect_key;
};

struct redis_client {
  struct resource res;

  char name[0x40];

  struct redis_client* prev;
  struct redis_client* next;

  void* ctx;
  struct bufferevent* bev;
  struct redis_command_parser* parser;

  int num_commands_received;
  int num_responses_sent;

  struct sockaddr_in local;
  struct sockaddr_in remote;

  struct redis_client_expected_response* response_chain_head;
  struct redis_client_expected_response* response_chain_tail;
};

struct redis_client* redis_client_create(void* resource_parent, struct bufferevent* bev);
void redis_client_print(struct redis_client* c, int indent);
struct evbuffer* redis_client_get_output_buffer(struct redis_client* c);

struct redis_client_expected_response* redis_client_expect_response(
    struct redis_client* c, int wait_type, struct redis_command* cmd, int size);
void redis_client_remove_expected_response(struct redis_client* c);
void redis_client_expected_response_print(struct redis_client_expected_response* e, int indent);

#endif // __REDIS_SOCKET_H
