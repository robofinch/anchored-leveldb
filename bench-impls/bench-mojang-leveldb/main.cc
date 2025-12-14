#include <leveldb/cache.h>
#include <leveldb/db.h>
#include <leveldb/decompress_allocator.h>
#include <leveldb/env.h>
#include <leveldb/filter_policy.h>
#include <leveldb/status.h>
#include <crc32c.h>
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

    // Do not ^C the program, I guess? That might carry a slight risk of corrupting the database
    // with this option enabled AFAIK.
    options.reuse_logs = true;


	//create a reusable memory space for decompression so it allocates less
	leveldb::ReadOptions readOptions;
    // `false` might be more efficient for a bulk scan
    readOptions.fill_cache = false;

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
