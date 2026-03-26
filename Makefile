CXX ?= g++
CXXFLAGS ?= -Wall -Wextra -O2 -pthread

SERVER_SRCS = src/server.cpp src/hashtable.cpp src/avl.cpp src/zset.cpp src/heap.cpp src/thread_pool.cpp
CLIENT_SRCS = src/client.cpp

all: myredis client

myredis: $(SERVER_SRCS)
	$(CXX) $(CXXFLAGS) -Isrc -o $@ $^

client: $(CLIENT_SRCS)
	$(CXX) $(CXXFLAGS) -Isrc -o $@ $^

clean:
	rm -f myredis client src/myredis src/client

.PHONY: all clean
