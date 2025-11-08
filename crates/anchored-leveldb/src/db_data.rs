#![expect(unsafe_code, reason = "manually drop database lockfile inside Drop impl")]

use std::{mem::ManuallyDrop, path::PathBuf};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use anchored_sstable::perf_options::KVCache;
use anchored_vfs::traits::ReadableFilesystem as _;
use oorandom::Rand32;

use crate::leveldb_generics::LdbPooledBuffer;
use crate::{
    db_writer::DBWriter,
    file_tracking::SeeksBetweenCompactionOptions,
    memtable::Memtable,
    snapshot::SnapshotList,
    table_traits::adapters::InternalComparator,
    version::set::VersionSet,
    write_log::WriteLogWriter,
};
use crate::{
    containers::{FragileRwCell as _, RwCellFamily as _},
    leveldb_generics::{LdbFsCell, LdbTableOptions, LevelDBGenerics, Lockfile, WriteFile},
};


pub(crate) struct DBShared<LDBG: LevelDBGenerics, Writer: DBWriter<LDBG>> {
    pub db_directory:  PathBuf,
    pub filesystem:    LdbFsCell<LDBG>, // cache padded?
    pub lockfile:      ManuallyDrop<Lockfile<LDBG>>,
    pub table_cache:   LDBG::TableCache,
    pub table_options: LdbTableOptions<LDBG>,
    pub db_options:    InnerDBOptions,
    pub writer_data:   Writer::Shared,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, Writer: DBWriter<LDBG>> DBShared<LDBG, Writer> {
    #[must_use]
    pub const fn cmp(&self) -> &InternalComparator<LDBG::Cmp> {
        &self.table_options.comparator
    }
}

impl<LDBG: LevelDBGenerics, Writer: DBWriter<LDBG>> Drop for DBShared<LDBG, Writer> {
    fn drop(&mut self) {
        // SAFETY: we never use `self.lockfile` again; this is the destructor of `self`.
        // (We also don't `drop` or `take` `self.lockfile` in any other function.)
        let lockfile = unsafe { ManuallyDrop::take(&mut self.lockfile) };
        // There's nothing we can do if unlocking the lockfile fails.
        let _err = self.filesystem.write().unlock_and_close(lockfile);
    }
}

impl<LDBG, Writer: DBWriter<LDBG>> Debug for DBShared<LDBG, Writer>
where
    LDBG:                  LevelDBGenerics,
    LDBG::FS:              Debug,
    LDBG::Policy:          Debug,
    LDBG::Cmp:             Debug,
    LDBG::Pool:            Debug,
    LdbPooledBuffer<LDBG>: Debug,
    Writer::Shared:        Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DBShared")
            .field("db_directory",  &self.db_directory)
            .field("filesystem",    LDBG::RwCell::debug(&self.filesystem))
            .field("lockfile",      &"<LOCK file>")
            .field("table_cache",   KVCache::debug(&self.table_cache))
            .field("table_options", &self.table_options)
            .field("db_options",    &self.db_options)
            .field("writer_data",   &self.writer_data)
            .finish()
    }
}

// TODO(possible-opt): try using cache line padding
pub(crate) struct DBSharedMutable<LDBG: LevelDBGenerics, Writer: DBWriter<LDBG>> {
    pub version_set:               VersionSet<LDBG::Refcounted, WriteFile<LDBG>>,
    pub snapshot_list:             SnapshotList<LDBG::Refcounted, LDBG::RwCell>,
    pub current_memtable:          Memtable<LDBG::Cmp, LDBG::Skiplist>,
    pub current_log:               WriteLogWriter<WriteFile<LDBG>>,
    pub memtable_under_compaction: Option<Memtable<LDBG::Cmp, LDBG::Skiplist>>,
    pub read_sample_prng:          Rand32,
    pub mutable_writer_data:       Writer::SharedMutable,
    // and more
}

impl<LDBG, Writer> Debug for DBSharedMutable<LDBG, Writer>
where
    LDBG:                  LevelDBGenerics,
    LDBG::Cmp:             Debug,
    LDBG::Skiplist:        Debug,
    Writer:                DBWriter<LDBG>,
    Writer::SharedMutable: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DBSharedMutable")
            .field("version_set",               &self.version_set)
            .field("snapshot_list",             &self.snapshot_list)
            .field("current_memtable",          &self.current_memtable)
            .field("current_log",               &self.current_log)
            .field("memtable_under_compaction", &self.memtable_under_compaction)
            .field("read_sample_prng",          &self.read_sample_prng)
            .field("mutable_writer_data",       &self.mutable_writer_data)
            .finish()
    }
}

#[expect(clippy::struct_excessive_bools, reason = "the options are given clear names")]
#[derive(Debug, Clone, Copy)]
pub(crate) struct InnerDBOptions {
    pub verify_recovered_version_set:         bool,
    pub verify_new_versions:                  bool,
    pub verify_block_checksums_during_writes: bool,
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
    pub read_sample_period:                   u64,
    /// Limit (TODO: hard or soft?) for the size of write-ahead log files, table files,
    /// and manifest files.
    pub file_size_limit:                      u64,
    pub memtable_size_limit:                  usize,
    pub perform_automatic_compactions:        bool,
}
