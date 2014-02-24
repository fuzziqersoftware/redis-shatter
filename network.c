#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "network.h"

#define NWERROR_DNS_FAILURE    -1

int network_listen(const char* addr, int port, int backlog) {

  int listen_fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listen_fd == -1) {
    return NWERROR_SOCKET;
  }

  int y = 1;
  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &y, sizeof(y)) == -1) {
    close(listen_fd);
    return NWERROR_REUSEADDR;
  }

  struct sockaddr_in local;
  local.sin_family = PF_INET;
  local.sin_port = htons(port);

  if (!addr)
    local.sin_addr.s_addr = htonl(INADDR_ANY);
  else if (isdigit(addr[0]))
    local.sin_addr.s_addr = inet_addr(addr);
  else {
    struct hostent* host_obj = gethostbyname(addr);
    if (host_obj == NULL)
      return NWERROR_DNS_FAILURE;
    local.sin_addr.s_addr = ((struct in_addr*)*host_obj->h_addr_list)->s_addr;
  }

  if (bind(listen_fd, (struct sockaddr*)(&local), sizeof(local)) != 0) {
    close(listen_fd);
    return NWERROR_BIND;
  }

  if (listen(listen_fd, backlog) != 0) {
    close(listen_fd);
    return NWERROR_LISTEN;
  }
  return listen_fd;
}

int network_connect(const char* host, int port) {

  struct sockaddr_in remote;
  remote.sin_family = AF_INET;
  remote.sin_port = htons(port);
  memset(&remote.sin_zero, 0, 8);

  if (isdigit(host[0]))
    remote.sin_addr.s_addr = inet_addr(host);
  else {
    struct hostent* host_obj = gethostbyname(host);
    if (host_obj == NULL)
      return NWERROR_DNS_FAILURE;
    remote.sin_addr.s_addr = ((struct in_addr*)*host_obj->h_addr_list)->s_addr;
  }

  int sock_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock_fd == -1)
    return NWERROR_SOCKET;

  if (connect(sock_fd, (struct sockaddr*)&remote, sizeof(struct sockaddr_in)) == -1) {
    close(sock_fd);
    return NWERROR_CONNECT;
  }

  return sock_fd;
}

void network_get_socket_addresses(int fd, struct sockaddr_in* local,
    struct sockaddr_in* remote) {
  socklen_t len;
  if (local) {
    len = sizeof(struct sockaddr_in);
    getsockname(fd, (struct sockaddr*)local, &len);
  }
  if (remote) {
    len = sizeof(struct sockaddr_in);
    getpeername(fd, (struct sockaddr*)remote, &len);
  }
}

const char* network_error_str(int error) {
  switch (error) {
    case NWERROR_DNS_FAILURE:
      return "DNS failure";
    case NWERROR_CONNECT:
      return "Can\'t connect to remote server";
    case NWERROR_SOCKET:
      return "Can\'t open socket";
    case NWERROR_REUSEADDR:
      return "Can\'t reuse server address";
    case NWERROR_BIND:
      return "Can\'t bind socket";
    case NWERROR_LISTEN:
      return "Can\'t listen on socket";
  }
  return "Unknown error";
}
