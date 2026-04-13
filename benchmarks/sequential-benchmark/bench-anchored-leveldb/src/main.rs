#[expect(clippy::absolute_paths, reason = "triggered by `std::process::ExitCode`")]
#[expect(
    clippy::disallowed_macros,
    clippy::print_stdout,
    // clippy::use_debug,
    reason = "using debug `println!`s in a test is fine",
)]
#[cfg(any(unix, windows))]
fn main() -> std::process::ExitCode {
    use std::env;
    use std::{num::NonZeroU8, path::PathBuf, process::ExitCode, time::Instant};

    use tracing::level_filters::LevelFilter;

    use anchored_leveldb::{DB, OpenOptions};
    use anchored_leveldb::db_options::{
        BadPool, BufferPoolOptions, CacheOptions, ClampOptions, CompactionOptions,
        CompressionOptions, CompressorId, ConsistencyOptions, FilterOptions, FormatSettings,
        LoggerOptions, ManifestOptions, MemtableOptions, SSTableOptions, SeekCompactionOptions,
        SizeCompactionOptions, TracingLogger, WriteThrottlingOptions,
    };
    use anchored_vfs::StandardFS;

    #[allow(clippy::iter_skip_next, reason = "more clear than `nth(1)")]
    let Some(database_directory) = env::args().skip(1).next() else {
        println!("Database directory must be provided as argument");
        return ExitCode::FAILURE;
    };

    #[allow(clippy::unwrap_used, reason = "validated at compile time")]
    let raw_zlib_compression = const {
        CompressorId(NonZeroU8::new(4).unwrap())
    };

    let opts = OpenOptions {
        filesystem:          StandardFS,
        database_directory:  PathBuf::from(database_directory),
        create_if_missing:   false,
        error_if_exists:     false,
        clamp_options:       ClampOptions::BackwardsCompatibilityClamping,
        format:              FormatSettings::mojang_leveldb_format(),
        compression:         CompressionOptions::from_compressor(raw_zlib_compression),
        filter:              FilterOptions::default_bloom_policy(),
        consistency:         ConsistencyOptions::default(),
        logger:              LoggerOptions {
            // TODO: should anchored-leveldb reexport LevelFilter?
            // ..... wait. It should have its *own* enum, probably....
            log_file_filter: LevelFilter::INFO,
            logger_filter:   LevelFilter::INFO,
            custom_logger:   Some(Box::new(TracingLogger)),
        },
        manifest:            ManifestOptions::default(),
        memtable:            MemtableOptions::default(),
        sstable:             SSTableOptions::default(),
        compaction:          CompactionOptions::default(),
        size_compaction:     SizeCompactionOptions::disabled(),
        seek_compaction:     SeekCompactionOptions::disabled(),
        write_throttling:    WriteThrottlingOptions::default(),
        buffer_pool:         BufferPoolOptions::<BadPool>::default(),
        cache:               CacheOptions::default(),
    };

    #[expect(clippy::expect_used, reason = "this is a test")]
    {
        use anchored_leveldb::{db_interface::Close, db_options::{CacheUsage, ReadOptions}};

        let db = DB::open(opts).expect("failed to open DB");
        let read_opts = ReadOptions {
            block_cache_usage: CacheUsage::Ignore,
            table_cache_usage: CacheUsage::Ignore,
            ..Default::default()
        };
        let mut iter = db.iter_with(&read_opts).expect("failed to get iter");

        let initial_start = Instant::now();
        let mut start = initial_start;

        for _ in 0..10_u8 {
            let mut checksum: u32 = 0;
            let mut num_entries: u64 = 0;

            loop {
                let entry = iter.next().expect("failed to read entry");
                let Some(entry) = entry else { break };

                checksum = crc32c::crc32c_append(checksum, entry.key_bytes());
                checksum = crc32c::crc32c_append(checksum, entry.value_bytes());

                num_entries += 1;
                if num_entries % 1_000_000 == 0 {
                    println!("{num_entries} entries");
                }
            }

            println!("{num_entries} total entries; crc32c: {checksum}");
            let next = Instant::now();
            println!("iter time (ms): {}", next.duration_since(start).as_millis());
            start = next;
        }

        println!("total iter time (ms): {}", Instant::now().duration_since(initial_start).as_millis());

        iter.into_db().close(Close::AsSoonAsPossible).1.expect("DB failed to close");
    };

    ExitCode::SUCCESS
}

#[expect(
    clippy::disallowed_macros,
    clippy::print_stdout,
    reason = "using `println` in a test is fine",
)]
#[cfg(not(any(unix, windows)))]
fn main() {
    use anchored_leveldb as _;
    use anchored_vfs as _;

    use crc32c as _;
    use tracing as _;

    println!("Not implemented.");
}
