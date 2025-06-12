mod impls;
mod generic_variants;


pub use self::impls::CloseSuccess;
pub use self::generic_variants::{
    Concurrent, ConcurrentInMemory, ConcurrentLevelDB, ConcurrentLevelDBInMemory,
    ConcurrentWithFSAndLogger, InMemory, LevelDB, LevelDBInMemory, Standard, WithFSAndLogger,
};


use std::{fmt::Debug, path::PathBuf};

use crate::{
    compactor::{CompactorHandle, FSError},
    comparator::{Comparator, InternalComparator},
    compressors::CompressorList,
    container::{Container, MutableContainer},
    filesystem::{FileSystem, MaybeLockableFilesystem},
    filter::{FilterPolicy, InternalFilterPolicy},
    logger::Logger,
};


// ReadOnlyLevelDB
// LevelDB
// ConcurrentLevelDB
// SyncLevelDB

// ReadOnlyFileSystem
// FileSystem
// SyncFileSystem <- for Concurrent or Sync


pub trait LevelDBGenerics: Debug + Sized {
    type FS:              FileSystem;
    type Container<T>:    Container<T>;
    type MutContainer<T>: MutableContainer<T>;
    type Logger:          Logger;
    type Comparator:      Comparator;
    type FilterPolicy:    FilterPolicy;
    type CompactorHandle: CompactorHandle<FSError<Self>>;
}

pub type FileLock<LDBG> = <<LDBG as LevelDBGenerics>::FS as MaybeLockableFilesystem>::FileLock;


// Note that methods on this struct are provided in the modules of the `impls` module.
#[derive(Debug, Clone)]
pub struct CustomLevelDB<LDBG: LevelDBGenerics> {
    root_directory:   PathBuf,
    fs:               LDBG::FS,
    file_lock:        LDBG::Container<FileLock<LDBG>>,

    compactor_handle: LDBG::CompactorHandle,
    logger:           LDBG::Logger,
    comparator:       InternalComparator<LDBG::Comparator>,
    filter_policy:    InternalFilterPolicy<LDBG::FilterPolicy>,
    compressor_list:  LDBG::Container<CompressorList>,

    pod_opts:         PodOptions,

    // compaction uses the current version of versionset, immutable ref to the comparator,
    // immutable access to a bunch of FileMetaData,

    // TODO: figure out what minimal constraints I need to get the compactor to be in a different
    // thread while keeping this stuff threadsafe.

    // memtable: MemTable,
    // compacting_memtable: Option<MemTable>,

    // ldb_log:     Option<LogWriter<BufWriter<Box<dyn Write>>>>,
    // ldb_log_num: Option<FileNum>,
    // table_cache: Shared<TableCache>,
    // version_set: Shared<VersionSet>,
    // snapshots:   SnapshotList,

    // compaction_stats: [CompactionStats; NUM_LEVELS],

    // might need a queue of write batches / "writers"
}

#[derive(Debug, Clone, Copy)]
struct PodOptions {
    // Some of these might not actually be relevant after opening the LevelDB.
    paranoid_corruption_checks: bool,
    write_buffer_size:          usize,
    max_open_files:             usize,
    max_file_size:              usize,
    block_cache_byte_capacity:  usize,
    block_size:                 usize,
    block_restart_interval:     usize,
    compressor:                 u8,
}
