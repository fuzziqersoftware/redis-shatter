CXX=g++
OBJECTS=Protocol.o Proxy.o Main.o
CXXFLAGS=-g -Wall -Werror -std=c++14
LDFLAGS=-levent -lphosg -g -std=c++14
EXECUTABLE=redis-shatter

TESTS=ProtocolTest FunctionalTest

all: $(EXECUTABLE) $(TESTS)

$(EXECUTABLE): $(OBJECTS)
	g++ -o $(EXECUTABLE) $^ $(LDFLAGS)

test: $(TESTS)
	./run_tests.sh

ProtocolTest: ProtocolTest.o Protocol.o
	g++ -o ProtocolTest $^ $(LDFLAGS)

FunctionalTest: FunctionalTest.o Protocol.o
	g++ -o FunctionalTest $^ $(LDFLAGS)

clean:
	rm -rf *.dSYM *.o $(EXECUTABLE) $(TESTS) gmon.out

.PHONY: clean
