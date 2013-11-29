#include <errno.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>

#include "debug.h"
#include "resource.h"
#include "redis_socket.h"



void redis_error(redis_socket* sock, int error1, int error2) {
  sock->error1 = error1;
  sock->error2 = error2;
  longjmp(sock->error_jmp, sock->error1);
}

void redis_assert(redis_socket* sock, int condition) {
  if (!condition)
    redis_error(sock, ERROR_ASSERTION_FAILED, 0);
}



// create/delete functions

redis_socket* redis_socket_new(int fd) {
  redis_socket* sock = (redis_socket*)calloc(1, sizeof(redis_socket));
  if (!sock)
    return NULL;
  sock->socket = fd;
  sock->read_buffer_size = DEFAULT_READ_BUFFER_SIZE;
  sock->read_buffer = (char*)malloc(sock->read_buffer_size);
  return sock;
}

void redis_socket_close(redis_socket* sock) {
  close(sock->socket);
  if (sock->read_buffer)
    free(sock->read_buffer);
  free(sock);
}



// basic i/o functions

// read buffer states:
//   empty - read full chunk into read buffer if possible; set read_available, read_pos
//   partial - copy out of read buffer first, then reset read buffer and go to above

void redis_socket_read(redis_socket* sock, void* data, int size) {
  int recvd = 0;
  int r;

  if (sock->read_pos < sock->read_available) {
    int data_to_read = sock->read_available - sock->read_pos;
    if (data_to_read > size)
      data_to_read = size;
    memcpy(data, &sock->read_buffer[sock->read_pos], data_to_read);
    recvd += data_to_read;
    sock->read_pos += data_to_read;
  }

  while (recvd < size) {
    // if we're going to recv into the buffer, then we'de better have already
    // returned everything that was in the buffer before
    redis_assert(sock, sock->read_pos == sock->read_available);

    r = recv(sock->socket, sock->read_buffer, sock->read_buffer_size, 0);
    if (r < 0)
      redis_error(sock, ERROR_RECV, errno);
    if (r == 0)
      redis_error(sock, ERROR_DISCONNECTED, 0);

    sock->read_pos = 0;
    sock->read_available = r;

    int data_to_read = sock->read_available; // - sock->read_pos (which is 0)
    if (data_to_read > (size - recvd))
      data_to_read = size - recvd;
    memcpy((char*)data + recvd, &sock->read_buffer[sock->read_pos], data_to_read);
    recvd += data_to_read;
    sock->read_pos += data_to_read;
  }

#ifdef DEBUG_SOCKET_DATA
  printf("<%d: [", sock->socket);
  int x;
  for (x = 0; x < size; x++) {
    int ch = *((char*)data + x);
    if (ch < 0x20 || ch > 0x7F)
      printf("\\x%02X", ch);
    else
      printf("%c", ch);
  }
  printf("]\n");
  //printf("> Received from client %d\n", sock->socket);
  //print_data(data, size);
#endif
}

void redis_socket_write(redis_socket* sock, const void* data, int size) {
  int sent = 0;
  int r;

  while (sent < size) {
    r = send(sock->socket, (const char*)data + sent, size - sent, 0);
    if (r < 0)
      redis_error(sock, ERROR_SEND, errno);
    if (r == 0)
      redis_error(sock, ERROR_DISCONNECTED, 0);
    sent += r;
  }

#ifdef DEBUG_SOCKET_DATA
  printf(">%d: [", sock->socket);
  int x;
  for (x = 0; x < size; x++) {
    int ch = *((char*)data + x);
    if (ch < 0x20 || ch > 0x7F)
      printf("\\x%02X", ch);
    else
      printf("%c", ch);
  }
  printf("]\n");
  //printf("> Sent to client %d\n", sock->socket);
  //print_data(data, size);
#endif
}

void redis_socket_read_line(redis_socket* sock, char* line, int max_size) {
  char* end_ptr = line + max_size;
  redis_socket_read(sock, line, 2);
  line += 2;
  while (line < end_ptr) {
    if (line[-2] == '\r' && line[-1] == '\n') {
      line[-2] = 0;
      return;
    }
    redis_socket_read(sock, line, 1);
    line++;
  }
  redis_error(sock, ERROR_LINE_TOO_LONG, 0);
}



// server functions

static int listen_fd = -1;

static void* client_thread(void* _data) {

  redis_socket* sock = (redis_socket*)_data;
  int ret = setjmp(sock->error_jmp);
  if (!ret)
    sock->thread_func(sock);

  if (sock->error1)
    printf("socket %d disconnected with error %d, %d\n", sock->socket,
        sock->error1, sock->error2);
  else
    printf("socket %d disconnected with no error\n", sock->socket);

  resource_delete(sock, 1);
  return NULL;
}

int redis_listen(int port, void (*thread_func)(redis_socket*), void* data, int register_data) {

  listen_fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listen_fd == -1)
    return -1;

  int yes = 1;
  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1)
    printf("warning: failed to set reuseaddr on listening socket\n");

  struct sockaddr_in service_addr;
  service_addr.sin_family = PF_INET;
  service_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  service_addr.sin_port = htons(port);
  if (bind(listen_fd, (struct sockaddr*)(&service_addr),
      sizeof(service_addr)) != 0) {
    printf("error: failed to bind socket on port %d\n", port);
    return -2;
  }

  if (listen(listen_fd, 10) != 0) {
    printf("error: failed to listen on socket\n");
    return -3;
  }

  struct sockaddr_in connection;
  socklen_t sockaddr_size;
  int new_sockfd;
  while (listen_fd != -1) {

    sockaddr_size = sizeof(struct sockaddr_in);
    new_sockfd = accept(listen_fd, (struct sockaddr*)(&connection),
                        &sockaddr_size);

    if (new_sockfd == -1)
      continue;

    redis_socket* sock = redis_socket_new(new_sockfd);
    if (!sock) {
      printf("error: failed to create thread for client on socket %d\n",
          new_sockfd);
      close(new_sockfd);
      continue;
    }
    resource_create(NULL, sock, redis_socket_close);

    resource_add_ref(sock, data);
    sock->data = data;
    sock->thread_func = thread_func;
    sockaddr_size = sizeof(struct sockaddr_in);
    getsockname(sock->socket, (struct sockaddr*)(&sock->local),
        &sockaddr_size);
    sockaddr_size = sizeof(struct sockaddr_in);
    getpeername(sock->socket, (struct sockaddr*)(&sock->remote),
        &sockaddr_size);

    if (pthread_create(&sock->thread, NULL, client_thread, sock)) {
      printf("error: failed to create thread for client on socket %d\n",
          sock->socket);
      resource_delete(sock, 1);
    }
  }

  if (listen_fd != -1)
    close(listen_fd);
  listen_fd = -1;

  return 0;
}

void redis_interrupt_server() {
  if (listen_fd != -1)
    close(listen_fd);
  listen_fd = -1;
}

redis_socket* redis_connect(void* resource_parent, const char* host, int port, redis_socket_thread_func thread_func, void* data) {

  struct sockaddr_in remote;
  remote.sin_family = AF_INET;
  remote.sin_port = htons(port);
  memset(&remote.sin_zero, 0, 8);

  if (isdigit(host[0]))
    remote.sin_addr.s_addr = inet_addr(host);
  else {
    struct hostent* host_obj = gethostbyname(host);
    if (host_obj == NULL)
      return NULL;
    remote.sin_addr.s_addr = ((struct in_addr*)*host_obj->h_addr_list)->s_addr;
  }

  int sock_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock_fd == -1)
    return NULL;

  if (connect(sock_fd, (struct sockaddr*)&remote, sizeof(struct sockaddr_in)) == -1) {
    close(sock_fd);
    return NULL;
  }

  redis_socket* sock = redis_socket_new(sock_fd);
  if (!sock) {
    close(sock_fd);
    return NULL;
  }
  resource_create(resource_parent, sock, redis_socket_close);

  sock->data = data;
  sock->thread_func = thread_func;
  socklen_t sockaddr_size = sizeof(struct sockaddr_in);
  getsockname(sock->socket, (struct sockaddr*)(&sock->local),
      &sockaddr_size);
  sockaddr_size = sizeof(struct sockaddr_in);
  getpeername(sock->socket, (struct sockaddr*)(&sock->remote),
      &sockaddr_size);

  if (sock->thread && pthread_create(&sock->thread, NULL, client_thread, sock)) {
    printf("error: failed to create thread for client on socket %d\n",
        sock->socket);
    if (resource_parent)
      resource_delete_ref(resource_parent, sock);
    else
      resource_delete(sock, 1);
    return NULL;
  }

  return sock;
}
