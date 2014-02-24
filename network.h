#ifndef __NETWORK_H
#define __NETWORK_H

#include <netinet/in.h>

#define NWERROR_DNS_FAILURE    -1
#define NWERROR_CONNECT        -2
#define NWERROR_SOCKET         -3
#define NWERROR_REUSEADDR      -4
#define NWERROR_BIND           -5
#define NWERROR_LISTEN         -6

int network_listen(const char* addr, int port, int backlog);
int network_connect(const char* host, int port);

void network_get_socket_addresses(int fd, struct sockaddr_in* local,
    struct sockaddr_in* remote);

const char* network_error_str(int error);

#endif // __NETWORK_H
