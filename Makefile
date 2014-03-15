CC=gcc
OBJECTS=debug.o resource.o redis_client.o redis_protocol.o redis_backend.o main.o ketama.o network.o redis_proxy.o
CFLAGS=-g -Wall # -DDEBUG_RESOURCES # -DDEBUG_COMMAND_IO
LDFLAGS=-levent
EXECUTABLE=redis-shatter

TESTS=resource_test ketama_test redis_protocol_test

all: $(EXECUTABLE) test

$(EXECUTABLE): $(OBJECTS)
	g++ -o $(EXECUTABLE) $^ $(LDFLAGS)

test: $(TESTS)
	./run_tests.sh

redis_protocol_test: redis_protocol_test.o redis_protocol.o resource.o debug.o
	g++ -o redis_protocol_test $^ $(LDFLAGS)

resource_test: resource_test.o resource.o debug.o
	g++ -o resource_test $^ $(LDFLAGS)

ketama_test: ketama_test.o ketama.o resource.o debug.o
	g++ -o ketama_test $^ $(LDFLAGS)

clean:
	rm -rf *.dSYM *.o $(EXECUTABLE) $(TESTS) gmon.out

.PHONY: clean
