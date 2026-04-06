use std::{num::NonZeroU8, path::PathBuf};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use anchored_vfs::LevelDBFilesystem;

use crate::{internal_logger::InternalLogger, pub_traits::pool::BufferPool};
use crate::{
    pub_typed_bytes::{
        BinaryLogBlockSize, FileSize, Level, NUM_MIDDLE_LEVELS_USIZE, NUM_NONZERO_LEVELS_USIZE,
    },
    table_caches::{BlockCache, TableCache},
    table_format::{InternalComparator, InternalFilterPolicy},
};
use super::dynamic_options::AtomicDynamicOptions;
use super::pub_options::{
    CacheUsage, SeekCompactionOptions, SizeCompactionOptions, WebScale, WriteThrottlingOptions,
};


#[derive(Debug)]
pub(crate) struct InternalOptions<Cmp, Policy, Codecs> {
    pub db_directory:               PathBuf,
    pub cmp:                        InternalComparator<Cmp>,
    pub policy:                     Option<InternalFilterPolicy<Policy>>,
    pub filter_chunk_size_log2:     u8,
    pub codecs:                     Codecs,
    pub binary_log_block_size:      BinaryLogBlockSize,
    pub verify_data_checksums:      bool,
    pub verify_index_checksums:     bool,
    pub unwrap_poison:              bool,
    pub web_scale:                  WebScale,
    pub max_memtable_size:          usize,
    pub max_write_log_file_size:    FileSize,
    pub max_sstable_sizes:          [FileSize; NUM_NONZERO_LEVELS_USIZE.get()],
    pub compaction:                 InternalCompactionOptions,
    pub write_throttling:           WriteThrottlingOptions,
    pub iter_buffer_capacity_limit: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalCompactionOptions {
    pub max_level_for_memtable_flush: Level,
    pub max_compaction_inputs:        [u64; NUM_NONZERO_LEVELS_USIZE.get()],
    pub max_grandparent_overlap:      [u64; NUM_MIDDLE_LEVELS_USIZE.get()],
    pub size_compactions:             SizeCompactionOptions,
    pub seek_compactions:             SeekCompactionOptions,
}

pub(crate) struct InternallyMutableOptions<FS: LevelDBFilesystem, Policy, Pool: BufferPool> {
    pub filesystem:  FS,
    pub dynamic:     AtomicDynamicOptions,
    pub logger:      InternalLogger<FS::WriteFile>,
    pub buffer_pool: Pool,
    pub block_cache: BlockCache<Pool>,
    pub table_cache: TableCache<FS::RandomAccessFile, Policy, Pool>,
}

impl<FS, Policy, Pool> Debug for InternallyMutableOptions<FS, Policy, Pool>
where
    FS:   Debug + LevelDBFilesystem,
    Pool: Debug + BufferPool,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InternallyMutableOptions")
            .field("filesystem", &self.filesystem)
            .field("dynamic", &self.dynamic)
            .field("logger", &self.logger)
            .field("buffer_pool", &self.buffer_pool)
            .field("block_cache", &self.block_cache)
            .field("table_cache", &self.table_cache)
            .finish()
    }
}

/// Does not include the options which are processed earliest on:
/// - `create_if_missing: bool`,
/// - `error_if_exists: bool`,
/// - `clamp_options: ClampOptions`,
/// - `open_corruption_handler: Box<dyn OpenCorruptionHandler<InvalidKey> + Send + Sync>`,
/// - `block_cache_size: u64`
/// - `average_block_size: NonZeroUsize`
/// - `table_cache_capacity: usize`
#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalOpenOptions {
    pub max_reused_manifest_size:  FileSize,
    pub initial_memtable_capacity: usize,
    pub max_reused_write_log_size: FileSize,
    pub memtable_pool_size:        NonZeroU8,
    pub compact_in_background:     bool,
}

/// Does not include whether seeks should be recorded or the sequence number of the `Snapshot`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalReadOptions {
    pub verify_data_checksums:  bool,
    pub verify_index_checksums: bool,
    pub block_cache_usage:      CacheUsage,
    pub table_cache_usage:      CacheUsage,
    // TODO: error handler (with per-db default).
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalWriteOptions {
    // TODO: Some `InternalReadOptions` might need to be included here.
    pub sync: bool,
    // TODO: error handler (with per-db default)
}
