#include <leveldb/cache.h>
#include <leveldb/db.h>
#include <leveldb/decompress_allocator.h>
#include <leveldb/env.h>
#include <leveldb/filter_policy.h>
#include <leveldb/status.h>
#include <leveldb/zlib_compressor.h>
#include <crc32c.h>
// #include "vendor/leveldb-mcpe/include/leveldb/db.h"
// #include "vendor/leveldb-mcpe/include/leveldb/decompress_allocator.h"
// #include "vendor/leveldb-mcpe/include/leveldb/env.h"
// #include "vendor/leveldb-mcpe/include/leveldb/filter_policy.h"
// #include "vendor/leveldb-mcpe/include/leveldb/status.h"
// #include "vendor/leveldb-mcpe/include/leveldb/zlib_compressor.h"
// #include "vendor/leveldb-mcpe/util/crc32c.h"
#include <iostream>

int main() {
	leveldb::Options options;

	//create a bloom filter to quickly tell if a key is in the database or not
	options.filter_policy = leveldb::NewBloomFilterPolicy(10);

	//create a 40 mb cache (we use this on ~1gb devices)
	options.block_cache = leveldb::NewLRUCache(40 * 1024 * 1024);

	//create a 4mb write buffer, to improve compression and touch the disk less
	options.write_buffer_size = 4 * 1024 * 1024;

	options.info_log = nullptr;

	//use the new raw-zip compressor to write (and read)
	options.compressors[0] = new leveldb::ZlibCompressorRaw(-1);

	//also setup the old, slower compressor for backwards compatibility. This will only be used to read old compressed blocks.
	options.compressors[1] = new leveldb::ZlibCompressor();
    // Do not ^C the program, I guess? That might carry a slight risk of corrupting the database
    // with this option enabled AFAIK.
    options.reuse_logs = true;


	//create a reusable memory space for decompression so it allocates less
	leveldb::ReadOptions readOptions;
    // `false` might be more efficient for a bulk scan
    readOptions.fill_cache = false;
	readOptions.decompress_allocator = new leveldb::DecompressAllocator();


    leveldb::DB *db = nullptr;
    leveldb::Status status = leveldb::DB::Open(options, "../put-mc-world-db-here/db", &db);

    if (!status.ok()) {
        return 1;
    }

    leveldb::Iterator *iter = db->NewIterator(readOptions);

    if (iter == nullptr) {
        return 2;
    }

    iter->SeekToFirst();
    uint32_t numEntries = 0;
    uint32_t crc = 0;

    while (iter->Valid()) {
        if (numEntries % 10000 == 0) {
            std::cout << numEntries << " entries\n";
        }

        leveldb::Slice data = iter->key();
        crc = leveldb::crc32c::Extend(crc, data.data(), data.size());
        data = iter->value();
        crc = leveldb::crc32c::Extend(crc, data.data(), data.size());

        numEntries++;
        iter->Next();
    }

    std::cout << numEntries << " total entries; crc32c: " << crc << "\n";
}
