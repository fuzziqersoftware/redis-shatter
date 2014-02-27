CC=gcc
OBJECTS=debug.o resource.o redis_client.o redis_protocol.o redis_backend.o main.o ketama.o network.o redis_proxy.o
CFLAGS=-g -Wall
LDFLAGS=-levent
EXECUTABLE=redis-shatter

TESTS=resource_test ketama_test redis_protocol_test

all: $(EXECUTABLE) test

$(EXECUTABLE): $(OBJECTS)
	g++ $(LDFLAGS) -o $(EXECUTABLE) $^

test: $(TESTS)
	./run_tests.sh

redis_protocol_test: redis_protocol_test.o redis_protocol.o resource.o
	g++ $(LDFLAGS) -o redis_protocol_test $^

resource_test: resource_test.o resource.o
	g++ $(LDFLAGS) -o resource_test $^

ketama_test: ketama_test.o ketama.o resource.o
	g++ $(LDFLAGS) -o ketama_test $^

clean:
	rm -rf *.dSYM *.o $(EXECUTABLE) $(TESTS) gmon.out

.PHONY: clean
