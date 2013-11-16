CC=gcc
OBJECTS=debug.o resource.o redis_socket.o redis_protocol.o redis_client.o redis_server.o main.o
CFLAGS=-g -Wall -DDEBUG_SOCKET_DATA
LDFLAGS=
EXECUTABLE=redis-shatter

TESTS=test_resource_exec test_ketama_exec

all: $(EXECUTABLE) $(TESTS)

$(EXECUTABLE): $(OBJECTS)
	g++ $(LDFLAGS) -o $(EXECUTABLE) $^

test_resource_exec: test_resource.o resource.o
	g++ $(LDFLAGS) -o test_resource_exec $^

test_ketama_exec: test_ketama.o ketama.o
	g++ $(LDFLAGS) -o test_ketama_exec $^

clean:
	-rm *.o $(EXECUTABLE) $(TESTS)

.PHONY: clean
