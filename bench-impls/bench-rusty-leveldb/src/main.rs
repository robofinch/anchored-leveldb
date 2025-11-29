use std::{path::PathBuf, rc::Rc};

use rusty_leveldb::{BloomPolicy, CompressorList, DB, DefaultCmp, LdbIterator, Options, PosixDiskEnv, compressor::NoneCompressor};


fn main() {
    let db = std::fs::canonicalize("../put-mc-world-db-here/db").unwrap();
    open_and_crc32c_with_mcbe_compressors(db);
}

fn open_and_crc32c_with_mcbe_compressors(db_directory: PathBuf) {
    let mut iter = open(db_directory).new_iter().unwrap();

    let mut entry_num: u64 = 0;
    let mut checksum = 0;

    while let Some(entry) = iter.next() {
        if entry_num % 10_000 == 0 {
            println!("{entry_num} entries");
        }
        entry_num += 1;
        checksum = crc32c::crc32c_append(checksum, &entry.0);
        checksum = crc32c::crc32c_append(checksum, &entry.1);
    }

    println!("{entry_num} total entries; crc32c: {checksum}")
}

fn open(db_directory: PathBuf) -> DB {
    let mut compressors = CompressorList::new();

    compressors.set(NoneCompressor);
    compressors.set(compressors::ZlibWithHeader);
    compressors.set(compressors::ZlibWithoutHeader);

    let options = Options {
        cmp: Rc::new(Box::new(DefaultCmp)),
        env: Rc::new(Box::new(PosixDiskEnv::new())),
        log: None,
        create_if_missing: true,
        error_if_exists: false,
        paranoid_checks: false,
        write_buffer_size: 4 << 20,
        max_open_files: 1 << 10,
        max_file_size: 2 << 20,
        block_cache_capacity_bytes: 1 << 30,
        block_size: 4096,
        block_restart_interval: 16,
        reuse_logs: true,
        reuse_manifest: true,
        compressor: 0,
        compressor_list: Rc::new(compressors),
        filter_policy: Rc::new(Box::new(BloomPolicy::new(10))),
    };
    DB::open(db_directory, options).unwrap()
}


mod compressors {
    use std::io::Read as _;

    use flate2::{Compress, Compression, Decompress};
    use flate2::bufread::{ZlibDecoder, ZlibEncoder};

    use rusty_leveldb::{Compressor, CompressorId};

    #[derive(Debug)]
    pub(super) struct ZlibWithHeader;

    impl CompressorId for ZlibWithHeader {
        const ID: u8 = 2;
    }

    impl Compressor for ZlibWithHeader {
        fn encode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
            let mut encoder = ZlibEncoder::new_with_compress(
                &*block,
                Compress::new(Compression::default(), true),
            );

            let mut output_buf = Vec::new();

            encoder.read_to_end(&mut output_buf).unwrap();
            Ok(output_buf)
        }

        fn decode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
            let mut decoder = ZlibDecoder::new_with_decompress(
                &*block,
                Decompress::new(true),
            );

            let mut output_buf = Vec::new();

            decoder.read_to_end(&mut output_buf).unwrap();
            Ok(output_buf)
        }
    }

    #[derive(Debug)]
    pub(super) struct ZlibWithoutHeader;

    impl CompressorId for ZlibWithoutHeader {
        const ID: u8 = 4;
    }

    impl Compressor for ZlibWithoutHeader {
        fn encode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
            let mut encoder = ZlibEncoder::new_with_compress(
                &*block,
                Compress::new(Compression::default(), false),
            );

            let mut output_buf = Vec::new();

            encoder.read_to_end(&mut output_buf).unwrap();
            Ok(output_buf)
        }

        fn decode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
            let mut decoder = ZlibDecoder::new_with_decompress(
                &*block,
                Decompress::new(false),
            );

            let mut output_buf = Vec::new();

            decoder.read_to_end(&mut output_buf).unwrap();
            Ok(output_buf)
        }
    }
}
