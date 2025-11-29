use std::collections::HashSet;
use std::{cell::RefCell, hash::Hash, marker::PhantomData, num::NonZeroUsize, path::PathBuf, rc::Rc};

use clone_behavior::{Fast, MirroredClone};
use generic_container::kinds::RcKind;
use mini_moka::unsync::Cache as UnsyncCache;

use anchored_pool::{PooledBuffer, UnboundedBufferPool};
use anchored_sstable::{format_options::CompressorList, Table, TableOptions};
use anchored_sstable::perf_options::{BlockCacheKey, BufferPool, KVCache, UnsyncMokaCache};
use anchored_vfs::StandardFS;
use anchored_vfs::traits::{ReadableFilesystem, WritableFilesystem};

use crate::containers::{DebugWrapper, RefCellKind, RefcountedFamily, RwCellFamily};
use crate::corruption_handler::InternalCorruptionHandler;
use crate::file_tracking::SeeksBetweenCompactionOptions;
use crate::format::{FileNumber, UserKey};
use crate::info_logger::InfoLogger;
use crate::inner_leveldb::{DBWriteImpl, InitOptions, InnerDBOptions, InnerGenericDB};
use crate::leveldb_generics::{LdbFullShared, LdbLockedFullShared, LdbTable};
use crate::memtable::{MemtableSkiplist, UnsyncMemtableSkiplist};
use crate::table_file::TableCacheKey;
use crate::table_traits::{
    BloomPolicy, BytewiseComparator, FilterPolicy, InternalComparator, InternalFilterPolicy,
    LevelDBComparator,
};
use crate::write_batch::WriteBatch;


type UnsyncTable = Table<
    <RcKind as RefcountedFamily>::Container<CompressorList>,
    InternalFilterPolicy<BloomPolicy>,
    InternalComparator<BytewiseComparator>,
    <StandardFS as ReadableFilesystem>::RandomAccessFile,
    UnsyncMokaCache<BlockCacheKey, DebugWrapper<RcKind, PooledBuffer<UnboundedBufferPool>>>,
    UnboundedBufferPool,
    DebugWrapper<RcKind, PooledBuffer<UnboundedBufferPool>>,
>;

type UnsyncLDBG = (
    RcKind,
    RefCellKind,
    UnsyncMemtableSkiplist<BytewiseComparator>,
    StandardFS,
    BloomPolicy,
    BytewiseComparator,
    UnsyncMokaCache<BlockCacheKey, DebugWrapper<RcKind, PooledBuffer<UnboundedBufferPool>>>,
    UnsyncMokaCache<TableCacheKey, DebugWrapper<RcKind, UnsyncTable>>,
    UnboundedBufferPool,
);

impl DBWriteImpl<UnsyncLDBG> for () {
    type Shared = ();
    type SharedMutable = ();

    fn close_writes(shared: LdbFullShared<'_, UnsyncLDBG, ()>) -> Result<(), ()> {
        Err(())
    }

    fn close_writes_after_compaction(shared: LdbFullShared<'_, UnsyncLDBG, ()>) -> Result<(), ()> {
        Err(())
    }

    fn compact_full(shared: LdbFullShared<'_, UnsyncLDBG, ()>) -> Result<(), ()> {
        Err(())
    }

    fn compact_memtable(shared: LdbFullShared<'_, UnsyncLDBG, ()>) -> Result<(), ()> {
        Err(())
    }

    fn compact_range(
        shared:      LdbFullShared<'_, UnsyncLDBG, ()>,
        lower_bound: Option<UserKey<'_>>,
        upper_bound: Option<UserKey<'_>>,
    ) -> Result<(), ()> {
        Err(())
    }

    fn initialize(shared: LdbLockedFullShared<'_, UnsyncLDBG, ()>) {
    }

    fn maybe_start_compaction(shared: LdbLockedFullShared<'_, UnsyncLDBG, ()>) {
    }

    fn pending_compaction_outputs(shared: LdbFullShared<'_, UnsyncLDBG, ()>) -> HashSet<FileNumber> {
        HashSet::new()
    }

    fn split(self) -> (Self::Shared, Self::SharedMutable) {
        ((), ())
    }

    fn wait_for_compaction_to_finish(shared: LdbFullShared<'_, UnsyncLDBG, ()>) {
    }

    fn write(
        shared:      LdbFullShared<'_, UnsyncLDBG, ()>,
        options:     (),
        write_batch: &WriteBatch,
    ) -> Result<(), ()> {
        Err(())
    }
}

pub struct UnsyncDB(InnerGenericDB<UnsyncLDBG, ()>);

impl UnsyncDB {
    pub fn open_and_crc32c_with_mcbe_compressors(db_directory: PathBuf) {
        let mut iter = Self::open(db_directory).0.testing_iter();

        let mut entry_num: u64 = 0;
        let mut checksum = 0;

        while let Some(entry) = iter.next() {
            if entry_num % 10_000 == 0 {
                println!("{entry_num} entries");
            }
            entry_num += 1;
            checksum = crc32c::crc32c_append(checksum, entry.0.0);
            checksum = crc32c::crc32c_append(checksum, entry.1.0);
        }

        println!("{entry_num} total entries; crc32c: {checksum}")
    }

    pub fn open_and_print_with_mcbe_compressors(db_directory: PathBuf) {
        let mut iter = Self::open(db_directory).0.testing_iter();

        let mut entry_num: u64 = 0;
        while let Some(entry) = iter.next() {
            println!("entry {}: (key: {:?}, value: ..)", entry_num, entry.0.0);
            entry_num += 1;
        }
    }

    fn unsync_moka_cache<K: Hash + Eq, V>() -> UnsyncMokaCache<K, V> {
        UnsyncMokaCache(Rc::new(RefCell::new(UnsyncCache::new(100))))
    }

    fn open(db_directory: PathBuf) -> Self {
        let mut compressor_list = CompressorList::new_without_compressors();
        compressor_list.add(self::compressors::ZlibWithHeader);
        compressor_list.add(self::compressors::ZlibWithoutHeader);

        let init_options = InitOptions {
            db_directory,
            table_cache:   Self::unsync_moka_cache(),
            table_options: TableOptions {
                compressor_list:        Rc::new(compressor_list),
                selected_compressor:    4,
                filter_policy:          Some(InternalFilterPolicy(BloomPolicy::default())),
                comparator:             InternalComparator(BytewiseComparator),
                verify_checksums:       true,
                block_cache:            Self::unsync_moka_cache(),
                _data_buffer:           PhantomData,
                buffer_pool:            UnboundedBufferPool::new(5000),
                block_restart_interval: NonZeroUsize::new(16).unwrap(),
                block_size:             4096,
                sync_table:             true,
            },
            db_options: InnerDBOptions {
                create_if_missing:             false,
                error_if_exists:               false,
                verify_recovered_version_set:  true,
                try_reuse_manifest:            true,
                try_reuse_write_ahead_log:     true,
                seek_options:                  SeeksBetweenCompactionOptions::default(),
                iter_read_sample_period:       1 << 20,
                file_size_limit:               2 << 20,
                memtable_size_limit:           4 << 20,
                perform_automatic_compactions: true,
            },
            corruption_handler: InternalCorruptionHandler::test_new(),
            info_logger: InfoLogger::new_without_log_file(),
            write_impl: (),
        };

        Self(InnerGenericDB::open(init_options, RefCell::new(StandardFS)).unwrap())
    }
}

mod compressors {
    use std::{io::Read as _, path::Path, rc::Rc};

    use flate2::{Compress, Compression, Decompress};
    use flate2::bufread::{ZlibDecoder, ZlibEncoder};

    use anchored_sstable::format_options::{
        CompressionError, Compressor, CompressorID, DecompressionError,
    };

    #[derive(Debug)]
    pub(super) struct ZlibWithHeader;

    impl CompressorID for ZlibWithHeader {
        const ID: u8 = 2;
    }

    impl Compressor for ZlibWithHeader {
        fn encode_into(
            &self,
            source:     &[u8],
            output_buf: &mut Vec<u8>,
        ) -> Result<(), CompressionError> {
            let mut encoder = ZlibEncoder::new_with_compress(
                source,
                Compress::new(Compression::default(), true),
            );

            // encoder.read_to_end(output_buf).map_err(CompressionError::from_display)?;
            encoder.read_to_end(output_buf).unwrap();
            Ok(())
        }

        fn decode_into(
            &self,
            source:     &[u8],
            output_buf: &mut Vec<u8>,
        ) -> Result<(), DecompressionError> {
            let mut decoder = ZlibDecoder::new_with_decompress(
                source,
                Decompress::new(true),
            );
            // decoder.read_to_end(output_buf).map_err(DecompressionError::from_display)?;
            decoder.read_to_end(output_buf).map_err(DecompressionError::from_display)?;
            Ok(())
        }
    }

    #[derive(Debug)]
    pub(super) struct ZlibWithoutHeader;

    impl CompressorID for ZlibWithoutHeader {
        const ID: u8 = 4;
    }

    impl Compressor for ZlibWithoutHeader {
        fn encode_into(
            &self,
            source:     &[u8],
            output_buf: &mut Vec<u8>,
        ) -> Result<(), CompressionError> {
            let mut encoder = ZlibEncoder::new_with_compress(
                source,
                Compress::new(Compression::default(), false),
            );

            encoder.read_to_end(output_buf).unwrap();
            // encoder.read_to_end(output_buf).map_err(CompressionError::from_display)?;
            Ok(())
        }

        fn decode_into(
            &self,
            source:     &[u8],
            output_buf: &mut Vec<u8>,
        ) -> Result<(), DecompressionError> {
            let mut decoder = ZlibDecoder::new_with_decompress(
                source,
                Decompress::new(false),
            );

            decoder.read_to_end(output_buf).unwrap();
            // decoder.read_to_end(output_buf).map_err(DecompressionError::from_display)?;
            Ok(())
        }
    }
}

#[cfg(test)]
#[test]
fn open_and_print_with_mcbe_compressors() {
    let world_path = PathBuf::from(
        // Put world location here
        "../../bench-impls/put-mc-world-db-here/db",
    );
    UnsyncDB::open_and_print_with_mcbe_compressors(world_path);
}
