CC=gcc
OBJECTS=debug.o resource.o client.o protocol.o backend.o main.o ketama.o network.o proxy.o
CFLAGS=-g -Wall # -DDEBUG_COMMAND_IO # -DDEBUG_RESOURCES
LDFLAGS=-levent
EXECUTABLE=redis-shatter

TESTS=resource_test ketama_test protocol_test functional_test

all: $(EXECUTABLE) test

$(EXECUTABLE): $(OBJECTS)
	g++ -o $(EXECUTABLE) $^ $(LDFLAGS)

test: $(TESTS)
	./run_tests.sh

protocol_test: protocol_test.o protocol.o resource.o debug.o
	g++ -o protocol_test $^ $(LDFLAGS)

resource_test: resource_test.o resource.o debug.o
	g++ -o resource_test $^ $(LDFLAGS)

ketama_test: ketama_test.o ketama.o resource.o debug.o
	g++ -o ketama_test $^ $(LDFLAGS)

functional_test: functional_test.o protocol.o network.o resource.o debug.o
	g++ -o functional_test $^ $(LDFLAGS)

clean:
	rm -rf *.dSYM *.o $(EXECUTABLE) $(TESTS) gmon.out

.PHONY: clean
