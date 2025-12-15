# Stuff specific to my computer. May need to be redefined for you to build.
CMAKE_PATH = /Applications/CMake.app/Contents/bin/cmake
SNAPPY_PATH = $(shell brew --prefix)/include/
PERF_FLAGS = -O3 -march=native -flto -fno-rtti
LINKER_PERF_FLAGS = -O3 -march=native -flto -fno-rtti
# -O3 -march=native -flto -fno-rtti
COMPRESSION_PATH = -I../../include_compression
COMPRESSION_LPATH = -L../../dylib_compression

COMMON_DEFINES = -DCMAKE_BUILD_TYPE=Release -DLEVELDB_BUILD_TESTS=OFF -DLEVELDB_BUILD_BENCHMARKS=OFF -DCMAKE_CXX_STANDARD=17 -DCMAKE_C_STANDARD=17
GOOGLE_LEVELDB_DEFINES = -DHAVE_SNAPPY=ON -DCMAKE_C_FLAGS="${CMAKE_C_FLAGS} $(COMPRESSION_PATH) $(PERF_FLAGS)" -DCMAKE_CXX_FLAGS="${CMAKE_CXX_FLAGS} $(COMPRESSION_PATH) $(PERF_FLAGS)" -DCMAKE_EXE_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS} $(LINKER_PERF_FLAGS)" -DCMAKE_SHARED_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS} $(LINKER_PERF_FLAGS)"
MOJANG_LEVELDB_DEFINES = -DHAVE_SNAPPY=OFF -DHAVE_ZSTD=OFF -DHAVE_ZLIB=ON -DLEVELDB_SUPPORT_LEGACY_ZLIB_ENUM=ON -DCMAKE_C_FLAGS="${CMAKE_C_FLAGS} $(COMPRESSION_PATH) $(PERF_FLAGS)" -DCMAKE_CXX_FLAGS="${CMAKE_CXX_FLAGS} $(COMPRESSION_PATH) $(PERF_FLAGS)" -DCMAKE_SHARED_LINKER_FLAGS="${CMAKE_SHARED_LINKER_FLAGS} $(COMPRESSION_LPATH) $(LINKER_PERF_FLAGS)" -DCMAKE_EXE_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS} $(COMPRESSION_LPATH) $(LINKER_PERF_FLAGS)"
RBEDROCK_LEVELDB_DEFINES = -DHAVE_SNAPPY=OFF -DCMAKE_C_FLAGS="${CMAKE_C_FLAGS} $(PERF_FLAGS)" -DCMAKE_CXX_FLAGS="${CMAKE_CXX_FLAGS} $(PERF_FLAGS)" -DCMAKE_EXE_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS} $(LINKER_PERF_FLAGS)" -DCMAKE_SHARED_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS} $(LINKER_PERF_FLAGS)"

all:	build_flamegraph build_google_leveldb build_mojang_leveldb build_rbedrock_leveldb

dylib_compression lib_compression:
	cd zlib-rs/libz-rs-sys-cdylib \
	&& RUSTFLAGS="-Ctarget-cpu=native -Cllvm-args=-enable-dfa-jump-thread" \
	cargo build --release --no-default-features --features "c-allocator" \
	--package libz-rs-sys-cdylib
	mkdir -p dylib_compression && cd dylib_compression \
	&& ln -sf ../zlib-rs/libz-rs-sys-cdylib/target/release/libz_rs.dylib libz.dylib \
	&& ln -sf ../zlib-rs/libz-rs-sys-cdylib/target/release/libz_rs.dylib libzlib.dylib \
	&& ln -sf ../zlib-rs/libz-rs-sys-cdylib/target/release/libz_rs.dylib zlib.dylib
	mkdir -p lib_compression && cd lib_compression \
	&& ln -sf ../zlib-rs/libz-rs-sys-cdylib/target/release/libz_rs.a libz.a \
	&& ln -sf ../zlib-rs/libz-rs-sys-cdylib/target/release/libz_rs.a libzlib.a \
	&& ln -sf ../zlib-rs/libz-rs-sys-cdylib/target/release/libz_rs.a zlib.a
# Thank you, libz / zlib, for having an obvious way to link.

include_compression:
	mkdir -p include_compression && cd include_compression \
	&& ln -sf $(SNAPPY_PATH)snappy.h snappy.h \
	&& ln -sf $(SNAPPY_PATH)snappy-sinksource.h snappy-sinksource.h \
	&& ln -sf $(SNAPPY_PATH)snappy-stubs-public.h snappy-stubs-public.h \
	&& ln -sf ../zlib-rs/libz-rs-sys-cdylib/include/zlib.h zlib.h \
	&& ln -sf ../zlib-rs/libz-rs-sys-cdylib/include/zconf.h zconf.h \
	&& ln -sf ../zlib-rs/libz-rs-sys-cdylib/include/zlib.map zlib.map

# No building actually needed.
build_flamegraph:

build_google_leveldb:	include_compression
	cd google-leveldb && mkdir -p build && cd build \
	&& $(CMAKE_PATH) $(COMMON_DEFINES) $(GOOGLE_LEVELDB_DEFINES) .. \
	&& $(CMAKE_PATH) --build .

build_mojang_leveldb:	include_compression lib_compression dylib_compression
	cd mojang-leveldb && mkdir -p build && cd build \
	&& $(CMAKE_PATH) $(COMMON_DEFINES) $(MOJANG_LEVELDB_DEFINES) .. \
	&& $(CMAKE_PATH) --build .

build_rbedrock_leveldb:	include_compression
	cd rbedrock/src/vendor/leveldb-mcpe/ && mkdir -p build && cd build \
	&& $(CMAKE_PATH) $(COMMON_DEFINES) $(RBEDROCK_LEVELDB_DEFINES) .. \
	&& $(CMAKE_PATH) --build .

clean:
	rm -rf include_compression
	rm -rf lib_compression
	rm -rf dylib_compression
	cd zlib-rs/libz-rs-sys-cdylib && cargo clean
	cd google-leveldb && rm -rf build
	cd mojang-leveldb && rm -rf build
	cd rbedrock/src/vendor/leveldb-mcpe/ && rm -r build && mkdir -p build && touch build/.gitkeep
