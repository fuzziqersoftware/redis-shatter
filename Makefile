CXX=g++
OBJECTS=NutcrackerConsistentHashRing.o Protocol.o Proxy.o Main.o
CXXFLAGS=-g -Wall -Werror -std=c++14 -I/opt/local/include
LDFLAGS=-levent -lphosg -lpthread -g -std=c++14 -L/opt/local/lib
EXECUTABLE=redis-shatter

TESTS=ProtocolTest FunctionalTest

all: $(EXECUTABLE) $(TESTS)

$(EXECUTABLE): $(OBJECTS)
	g++ -o $(EXECUTABLE) $^ $(LDFLAGS)

test: all
	./run_tests.sh

ProtocolTest: ProtocolTest.o Protocol.o
	g++ -o ProtocolTest $^ $(LDFLAGS)

FunctionalTest: FunctionalTest.o Protocol.o
	g++ -o FunctionalTest $^ $(LDFLAGS)

clean:
	rm -rf *.dSYM *.o $(EXECUTABLE) $(TESTS) gmon.out

.PHONY: clean
