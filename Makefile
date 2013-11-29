CC=gcc
OBJECTS=debug.o resource.o redis_socket.o redis_protocol.o redis_client.o redis_multiclient.o redis_server.o main.o ketama.o
CFLAGS=-g -Wall -pg
LDFLAGS=-pg
EXECUTABLE=redis-shatter

TESTS=resource_test ketama_test

all: $(EXECUTABLE) test

$(EXECUTABLE): $(OBJECTS)
	g++ $(LDFLAGS) -o $(EXECUTABLE) $^

test: $(TESTS)
	./run_tests.sh

resource_test: resource_test.o resource.o
	g++ $(LDFLAGS) -o resource_test $^

ketama_test: ketama_test.o ketama.o resource.o
	g++ $(LDFLAGS) -o ketama_test $^

clean:
	rm -rf *.dSYM *.o $(EXECUTABLE) $(TESTS) gmon.out

.PHONY: clean
