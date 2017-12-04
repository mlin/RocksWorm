OBJS=build/test_httpd.o build/roundtrips.o

all:
	$(MAKE) rocksdb-on-cloud
	$(MAKE) build/integration_tests

test: all
	build/rocksdb-on-cloud/unit_tests
	build/integration_tests

build/integration_tests: $(OBJS) test_httpd.h build/lib/librocksdb.a build/lib/librocksdb-on-cloud.a
	g++ -o $@ -g -pthread \
		-Lbuild/rocksdb-on-cloud/googletest/src/googletest-build -Lbuild/lib \
		-lgtest_main -lgtest $(OBJS) -lrocksdb-on-cloud -lrocksdb -ljemalloc -lz -lsnappy -lbz2 -lzstd -lmicrohttpd -lcurl -lrt

rocksdb-on-cloud:
	mkdir -p build/rocksdb-on-cloud
	CXXFLAGS= cd build/rocksdb-on-cloud && \
		cmake -Dtest=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=.. -B./ -H../../../.. && \
		make -j4 && \
		make install

build/%.o: %.cc
	g++ -c -g -Wall -std=c++14 \
		-Ibuild/rocksdb-on-cloud/googletest/src/googletest/include -Ibuild/include \
		-o $@ $^

clean:
	rm -rf build

.PHONY: all test rocksdb-on-cloud clean
