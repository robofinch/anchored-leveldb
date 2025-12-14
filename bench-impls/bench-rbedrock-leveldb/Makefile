build:
	export MACOSX_DEPLOYMENT_TARGET=15.6 && g++ \
	-O2 -fno-rtti -std=c++17 main.cc \
	-Lleveldb-mcpe/build/ -Ileveldb-mcpe/util/ -Ileveldb-mcpe/include/ \
	-lleveldb -lz \
	-o bench_rbedrock_leveldb

link:
	ln -s ../../third-party/rbedrock/src/vendor/leveldb-mcpe leveldb-mcpe
