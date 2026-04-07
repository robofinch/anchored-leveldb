build:	bench_rbedrock_leveldb

link:	leveldb-mcpe

bench_rbedrock_leveldb:	leveldb-mcpe/build ../../third-party/dylib_compression
	g++ -O3 -mcpu=native -flto -fno-rtti -std=c++17 main.cc \
	-Lleveldb-mcpe/build/ -Ileveldb-mcpe/util/ -Ileveldb-mcpe/include/ \
	-lleveldb -lz \
	-o bench_rbedrock_leveldb
# -O3 -mcpu=native -flto -fno-rtti -L../../third-party/(dy)lib_compression
# -L../../third-party/(dy)lib_compression seems to be around 16% slower than the system zlib

leveldb-mcpe:
	ln -s ../../third-party/rbedrock/src/vendor/leveldb-mcpe leveldb-mcpe
