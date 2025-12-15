build:	bench_mojang_leveldb

link:	mojang-leveldb

bench_mojang_leveldb:	mojang-leveldb/build ../../third-party/dylib_compression
	export MACOSX_DEPLOYMENT_TARGET=15.6 && g++ \
	-O3 -march=native -flto -fno-rtti -std=c++17 main.cc \
	-Lmojang-leveldb/build/ -Imojang-leveldb/util/ -Imojang-leveldb/include/ \
	-lleveldb -lz \
	-o bench_mojang_leveldb
# -O3 -march=native -flto -fno-rtti -L../../third-party/(dy)lib_compression
# -L../../third-party/(dy)lib_compression seems to be around 16% slower than the system zlib

mojang-leveldb:
	ln -s ../../third-party/mojang-leveldb mojang-leveldb
