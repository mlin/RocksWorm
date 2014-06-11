OBJS=build/test_httpd.o build/roundtrips.o
CC=g++

all: build/integration_tests

test: all
	build/rocksdb-on-cloud/unit_tests
	build/integration_tests

build/integration_tests: rocksdb-on-cloud $(OBJS)
	g++ -o $@ -g -pthread \
		-Lbuild/rocksdb-on-cloud/googletest/src/googletest-build -Lbuild/lib \
		-lgtest_main -lgtest $(OBJS) -lrocksdb-on-cloud -lrocksdb -ljemalloc -lz -lsnappy -lbz2 -lmicrohttpd -lcurl

rocksdb-on-cloud:
	mkdir -p build/rocksdb-on-cloud
	CXXFLAGS= cd build/rocksdb-on-cloud && \
		cmake -Dtest=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=.. ../../../.. && \
		make && \
		make install

build/%.o: %.cc
	$(CC) -c -g -Wall -std=c++11 \
		-Ibuild/rocksdb-on-cloud/googletest/src/googletest/include \
		-Ibuild/include -o $@ $^

clean:
	rm -rf build

.PHONY: all test rocksdb-on-cloud clean