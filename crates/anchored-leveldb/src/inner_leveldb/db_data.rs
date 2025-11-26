use std::path::PathBuf;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use anchored_sstable::perf_options::KVCache;

use crate::{
    containers::RwCellFamily as _,
    corruption_handler::InternalCorruptionHandler,
    file_tracking::SeeksBetweenCompactionOptions,
    info_logger::InfoLogger,
    memtable::Memtable,
    version::VersionSet,
    write_log::WriteLogWriter,
};
use crate::leveldb_generics::{
    LdbDataBuffer, LdbPooledBuffer, LdbSnapshotList, LdbTableOptions, LdbWriteFile, LevelDBGenerics,
};
use super::{fs_guard::FSGuard, write_impl::DBWriteImpl};


pub(crate) struct DBShared<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> {
    pub db_directory:       PathBuf,
    pub filesystem:         FSGuard<LDBG>,
    pub table_cache:        LDBG::TableCache,
    pub table_options:      LdbTableOptions<LDBG>,
    pub db_options:         InnerDBOptions,
    pub corruption_handler: InternalCorruptionHandler<LDBG::Refcounted, LDBG::RwCell>,
    pub write_data:         WriteImpl::Shared,
    // later, a function to get an Instant-like type (yielding Duration from differences)
    // might be put here, to track statistics.
}

impl<LDBG, WriteImpl> Debug for DBShared<LDBG, WriteImpl>
where
    LDBG:                  LevelDBGenerics,
    LDBG::FS:              Debug,
    LDBG::Policy:          Debug,
    LDBG::Cmp:             Debug,
    LDBG::Pool:            Debug,
    LdbPooledBuffer<LDBG>: Debug,
    LdbDataBuffer<LDBG>:   Debug,
    WriteImpl:             DBWriteImpl<LDBG>,
    WriteImpl::Shared:     Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DBShared")
            .field("db_directory",       &self.db_directory)
            .field("filesystem",         &self.filesystem)
            .field("table_cache",        KVCache::debug(&self.table_cache))
            .field("table_options",      &self.table_options)
            .field("db_options",         &self.db_options)
            .field("corruption_handler", &self.corruption_handler)
            .field("write_data",         &self.write_data)
            .finish()
    }
}

// TODO(possible-opt): try using cache line padding
pub(crate) struct DBSharedMutable<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> {
    pub version_set:               VersionSet<LDBG::Refcounted, LdbWriteFile<LDBG>>,
    pub snapshot_list:             LdbSnapshotList<LDBG>,
    pub current_memtable:          Memtable<LDBG::Cmp, LDBG::Skiplist>,
    pub current_log:               WriteLogWriter<LdbWriteFile<LDBG>>,
    pub memtable_under_compaction: Option<Memtable<LDBG::Cmp, LDBG::Skiplist>>,
    pub iter_read_sample_seed:     u64,
    pub info_logger:               InfoLogger<LdbWriteFile<LDBG>>,
    pub write_status:              WriteStatus,
    pub mutable_write_data:        WriteImpl::SharedMutable,
    // later, we could track compaction statistics here
}

impl<LDBG, WriteImpl> Debug for DBSharedMutable<LDBG, WriteImpl>
where
    LDBG:                     LevelDBGenerics,
    LDBG::Skiplist:           Debug,
    LDBG::Cmp:                Debug,
    WriteImpl:                DBWriteImpl<LDBG>,
    WriteImpl::SharedMutable: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DBSharedMutable")
            .field("version_set",               &self.version_set)
            .field("snapshot_list",             LDBG::RwCell::debug(&self.snapshot_list))
            .field("current_memtable",          &self.current_memtable)
            .field("current_log",               &self.current_log)
            .field("memtable_under_compaction", &self.memtable_under_compaction)
            .field("iter_read_sample_seed",     &self.iter_read_sample_seed)
            .field("info_logger",               &self.info_logger)
            .field("write_status",              &self.write_status)
            .field("mutable_write_data",        &self.mutable_write_data)
            .finish()
    }
}

#[expect(clippy::struct_excessive_bools, reason = "the options are given clear names")]
#[derive(Debug, Clone, Copy)]
pub(crate) struct InnerDBOptions {
    pub verify_recovered_version_set:         bool,
    pub verify_new_versions:                  bool,
    /// Whether the database should try to append to the existing manifest file instead of
    /// always creating a new manifest upon opening the database.
    ///
    /// If `true`, an existing manifest file will be reused if
    /// - a previous manifest exists and has a valid name,
    /// - the existing manifest is not too large,
    /// - the filesystem supports efficiently appending to an existing file, and
    /// - reusing the manifest would not carry a risk of corrupting the database.
    ///
    /// If `false`, a new manifest file will always be created and initialized to the semantic
    /// contents of the existing manifest file (with all out-of-date information removed).
    pub try_reuse_manifest:                   bool,
    pub try_reuse_memtable_logs:              bool,
    /// Settings for how many times an unnecessary read to a file must occur before a seek
    /// compaction is triggered on that file.
    pub seek_options:                         SeeksBetweenCompactionOptions,
    pub iter_read_sample_period:              u32,
    /// Limit (TODO: hard or soft?) for the size of write-ahead log files, table files,
    /// and manifest files.
    pub file_size_limit:                      u64,
    pub memtable_size_limit:                  usize,
    pub perform_automatic_compactions:        bool,
}

#[derive(Debug)]
pub(crate) enum WriteStatus {
    WritesAllowed,
    ClosingAfterCompaction,
    Closed,
    WriteError(()),
    CorruptionError(()),
}
