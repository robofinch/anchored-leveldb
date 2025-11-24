#![expect(unsafe_code, reason = "manually drop database lockfile inside Drop impl")]

use std::{mem::ManuallyDrop, path::PathBuf};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use tracing::Level as LogLevel;

use anchored_sstable::perf_options::KVCache;
use anchored_vfs::traits::ReadableFilesystem as _;

use crate::{
    corruption_handler::InternalCorruptionHandler,
    file_tracking::SeeksBetweenCompactionOptions,
    info_logger::InfoLogger,
    memtable::Memtable,
    snapshot::SnapshotList,
    version::set::VersionSet,
    write_log::WriteLogWriter,
};
use crate::{
    containers::{FragileRwCell as _, RwCellFamily as _},
    leveldb_generics::{
        LdbFsCell, LdbPooledBuffer, LdbSharedWriteData, LdbSharedMutableWriteData,
        LdbTableOptions, LevelDBGenerics, Lockfile, WriteFile,
    },
};


pub(crate) struct DBShared<LDBG: LevelDBGenerics> {
    pub db_directory:       PathBuf,
    pub filesystem:         LdbFsCell<LDBG>,
    pub lockfile:           ManuallyDrop<Lockfile<LDBG>>,
    pub table_cache:        LDBG::TableCache,
    pub table_options:      LdbTableOptions<LDBG>,
    pub db_options:         InnerDBOptions,
    pub corruption_handler: InternalCorruptionHandler<LDBG::Refcounted, LDBG::RwCell>,
    pub write_data:         LdbSharedWriteData<LDBG>,
    // later, a function to get an Instant-like type (yielding Duration from differences)
    // might be put here, to track statistics.
}

impl<LDBG: LevelDBGenerics> Drop for DBShared<LDBG> {
    fn drop(&mut self) {
        // SAFETY: we never use `self.lockfile` again; this is the destructor of `self`.
        // (We also don't `drop` or `take` `self.lockfile` in any other function.)
        let lockfile = unsafe { ManuallyDrop::take(&mut self.lockfile) };
        // There's not much we can do if unlocking the lockfile fails.
        if let Err(lock_error) = self.filesystem.write().unlock_and_close(lockfile) {
            tracing::event!(LogLevel::DEBUG, "error while unlocking LOCK file: {lock_error}");
        }
    }
}

impl<LDBG> Debug for DBShared<LDBG>
where
    LDBG:                     LevelDBGenerics,
    LDBG::FS:                 Debug,
    LDBG::Policy:             Debug,
    LDBG::Cmp:                Debug,
    LDBG::Pool:               Debug,
    LdbPooledBuffer<LDBG>:    Debug,
    LdbSharedWriteData<LDBG>: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DBShared")
            .field("db_directory",       &self.db_directory)
            .field("filesystem",         LDBG::RwCell::debug(&self.filesystem))
            .field("lockfile",           &"<LOCK file>")
            .field("table_cache",        KVCache::debug(&self.table_cache))
            .field("table_options",      &self.table_options)
            .field("db_options",         &self.db_options)
            .field("corruption_handler", &self.corruption_handler)
            .field("write_data",         &self.write_data)
            .finish()
    }
}

// TODO(possible-opt): try using cache line padding
pub(crate) struct DBSharedMutable<LDBG: LevelDBGenerics> {
    pub version_set:               VersionSet<LDBG::Refcounted, WriteFile<LDBG>>,
    pub snapshot_list:             SnapshotList<LDBG::Refcounted, LDBG::RwCell>,
    pub current_memtable:          Memtable<LDBG::Cmp, LDBG::Skiplist>,
    pub current_log:               WriteLogWriter<WriteFile<LDBG>>,
    pub memtable_under_compaction: Option<Memtable<LDBG::Cmp, LDBG::Skiplist>>,
    pub iter_read_sample_seed:     u64,
    pub info_logger:               InfoLogger<WriteFile<LDBG>>,
    pub write_status:              WriteStatus,
    pub mutable_write_data:        LdbSharedMutableWriteData<LDBG>,
    // later, we could track compaction statistics here
}

impl<LDBG> Debug for DBSharedMutable<LDBG>
where
    LDBG:                            LevelDBGenerics,
    LDBG::Skiplist:                  Debug,
    LDBG::Cmp:                       Debug,
    LdbSharedMutableWriteData<LDBG>: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DBSharedMutable")
            .field("version_set",               &self.version_set)
            .field("snapshot_list",             &self.snapshot_list)
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
