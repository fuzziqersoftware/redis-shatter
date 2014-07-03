#ifndef __CLIENT_H
#define __CLIENT_H

#include <event2/bufferevent.h>
#include <netinet/in.h>
#include <stdint.h>

#include "resource.h"
#include "protocol.h"

#define ERROR_ASSERTION_FAILED     1
#define ERROR_OUT_OF_MEMORY        2

#define CWAIT_FORWARD_RESPONSE                   0
#define CWAIT_COLLECT_STATUS_RESPONSES           1
#define CWAIT_SUM_INT_RESPONSES                  2
#define CWAIT_COMBINE_MULTI_RESPONSES            3
#define CWAIT_COLLECT_RESPONSES                  4
#define CWAIT_COLLECT_MULTI_RESPONSES_BY_KEY     5
#define CWAIT_COLLECT_IDENTICAL_RESPONSES        6
#define CWAIT_MODIFY_SCAN_RESPONSE               7
#define CWAIT_MODIFY_SCRIPT_EXISTS_RESPONSE      8

#define CLIENT_NAME_LENGTH    0x40

struct client;

struct client_expected_response {
  struct resource res;

  struct client* client;
  struct client_expected_response* next_wait;
  struct client_expected_response* next_response;

  struct command* original_command;
  int wait_type;
  int num_responses_expected;
  struct response* error_response;

  struct response* response_to_forward;
  int64_t int_sum;
  struct {
    int num_responses;
    int expected_response_type;
    struct response* responses[0];
  } collect_multi;
  struct {
  	int num_keys;
  	int args_per_key;
    int* server_to_key_count;
    uint8_t* key_to_server;
    struct command** index_to_command;
    struct response* response_in_progress;
  } collect_key;
  int64_t scan_backend_num;
};

struct client {
  struct resource res;

  char name[CLIENT_NAME_LENGTH];
  int should_disconnect;

  struct client* prev;
  struct client* next;

  void* ctx;
  struct bufferevent* bev;
  struct command_parser* parser;

  int num_commands_received;
  int num_responses_sent;

  struct sockaddr_in local;
  struct sockaddr_in remote;

  struct client_expected_response* response_chain_head;
  struct client_expected_response* response_chain_tail;
};

struct client* client_create(void* resource_parent, struct bufferevent* bev);
void client_print(const struct client* c, int indent);
struct evbuffer* client_get_output_buffer(struct client* c);

struct client_expected_response* client_expect_response(
    struct client* c, int wait_type, struct command* cmd, int size);
void client_remove_expected_response(struct client* c);
void client_expected_response_print(const struct client_expected_response* e, int indent);

#endif // __CLIENT_H
