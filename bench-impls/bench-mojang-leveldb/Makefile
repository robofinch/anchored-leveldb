build:
	export MACOSX_DEPLOYMENT_TARGET=15.6 && g++ \
	-O2 -fno-rtti -std=c++17 main.cc \
	-Lmojang-leveldb/build/ -Imojang-leveldb/util/ -Imojang-leveldb/include/ \
	-L../../third-party/lib_compression -lleveldb -lz -lzstd \
	-o bench_mojang_leveldb

link:
	ln -s ../../third-party/mojang-leveldb mojang-leveldb
