OBJS=build/test_httpd.o build/roundtrips.o

all:
	$(MAKE) RocksWorm
	$(MAKE) build/integration_tests

test: all
	build/RocksWorm/unit_tests
	build/integration_tests

build/integration_tests: $(OBJS) test_httpd.h build/lib/librocksdb.a build/lib/libRocksWorm.a
	g++ -o $@ -g -pthread \
		-Lbuild/RocksWorm/external/src/googletest-build -Lbuild/lib \
		-lgtest_main -lgtest $(OBJS) -lRocksWorm -lrocksdb -ljemalloc -lz -lsnappy -lbz2 -lzstd -lmicrohttpd -lcurl -lrt

RocksWorm:
	mkdir -p build/RocksWorm
	CXXFLAGS= cd build/RocksWorm && \
		cmake -Dtest=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=.. -B./ -H../../../.. && \
		make -j4 && \
		make install

build/%.o: %.cc
	g++ -c -g -Wall -std=c++14 \
		-Ibuild/RocksWorm/external/src/googletest/include -Ibuild/include \
		-o $@ $^

clean:
	rm -rf build

.PHONY: all test RocksWorm clean
