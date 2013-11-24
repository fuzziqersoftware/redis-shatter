#ifndef __REDIS_SOCKET_H
#define __REDIS_SOCKET_H

#include <pthread.h>
#include <stdint.h>
#include <netinet/in.h>
#include <setjmp.h>

#include "resource.h"

#define ERROR_ASSERTION_FAILED     1
#define ERROR_DISCONNECTED         2
#define ERROR_OUT_OF_MEMORY        3
#define ERROR_RECV                 4
#define ERROR_SEND                 5
#define ERROR_LINE_TOO_LONG        6

#define DEFAULT_READ_BUFFER_SIZE   1024

#define DEFAULT_REDIS_PORT 6379

typedef struct redis_socket {
  resource res;

  void (*thread_func)(struct redis_socket*);
  pthread_t thread;
  jmp_buf error_jmp;

  void* data;

  int socket;
  struct sockaddr_in local;
  struct sockaddr_in remote;

  int error1;
  int error2;

  int read_pos;
  int read_available;
  int read_buffer_size;
  char* read_buffer;
} redis_socket;

typedef void (*redis_socket_thread_func)(redis_socket*);

void redis_error(redis_socket* sock, int error1, int error2) __attribute__ ((noreturn));
void redis_assert(redis_socket* sock, int condition);

void redis_socket_read(redis_socket* sock, void* data, int size);
void redis_socket_write(redis_socket* sock, const void* data, int size);

void redis_socket_read_line(redis_socket* sock, char* line, int max_size);

int redis_listen(int port, redis_socket_thread_func thread_func, void* data, int register_data);
redis_socket* redis_connect(void* resource_parent, const char* host, int port, redis_socket_thread_func thread_func, void* data);

#endif // __REDIS_SOCKET_H
