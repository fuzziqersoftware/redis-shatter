CC=gcc
OBJECTS=debug.o resource.o redis_socket.o redis_protocol.o redis_client.o redis_multiclient.o redis_server.o main.o ketama.o
CFLAGS=-g -Wall -DDEBUG_RESOURCES
LDFLAGS=
EXECUTABLE=redis-shatter

TESTS=test_resource_exec test_ketama_exec

all: $(EXECUTABLE) test

$(EXECUTABLE): $(OBJECTS)
	g++ $(LDFLAGS) -o $(EXECUTABLE) $^

test: $(TESTS)
	./run_tests.sh

test_resource_exec: test_resource.o resource.o
	g++ $(LDFLAGS) -o test_resource_exec $^

test_ketama_exec: test_ketama.o ketama.o resource.o
	g++ $(LDFLAGS) -o test_ketama_exec $^

clean:
	-rm *.o $(EXECUTABLE) $(TESTS)

.PHONY: clean
