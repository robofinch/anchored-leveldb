use std::{collections::HashSet, thread::JoinHandle};
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::{Arc, Condvar, Mutex},
};

use anchored_vfs::LevelDBFilesystem;

use crate::{
    all_errors::aliases::WriteErrorAlias,
    binary_block_log::WriteLogWriter,
    pub_typed_bytes::FileNumber,
    snapshot::SnapshotList,
    table_file::TableFileBuilder,
    version::VersionSet,
};
use crate::{
    contention_queue::{ContentionQueue, VaryingWriteCommand},
    memtable::{Memtable, MemtableReader},
    options::{InternalOptions, InternallyMutableOptions},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    typed_bytes::{AtomicCloseStatus, NextFileNumber},
};


pub(crate) struct InternalDBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    pub opts:                 InternalOptions<Cmp, Policy, Codecs>,
    pub mut_opts:             InternallyMutableOptions<FS, Policy, Pool>,
    pub mutable_state:        Mutex<SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
    pub compaction_finished:  Condvar,
    pub close_status:         AtomicCloseStatus,
    /// # Correctness
    /// Must be `Some(_)` if and only if `foreground_compactor` is `None`.
    ///
    /// Otherwise, deadlocks or other errors may occur.
    pub background_compactor: Option<BackgroundCompactorHandle>,
    pub contention_queue:     ContentionQueue<
        'static,
        FrontWriterState<FS::WriteFile, Cmp>,
        VaryingWriteCommand,
    >,
    pub snapshot_list:        Arc<Mutex<SnapshotList>>,
}

impl<FS, Cmp, Policy, Codecs, Pool> Debug for InternalDBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     Debug + LevelDBFilesystem<
        RandomAccessFile: Debug,
        WriteFile: Debug,
        Lockfile: Debug,
        Error: Debug,
    >,
    Cmp:    Debug + LevelDBComparator<InvalidKeyError: Debug>,
    Policy: Debug + FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: Debug + CompressionCodecs<
        Encoders: Debug,
        CompressionError: Debug,
        DecompressionError: Debug,
    >,
    Pool:   Debug + BufferPool<PooledBuffer: Debug>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InternalDBState")
            .field("opts",                 &self.opts)
            .field("mut_opts",             &self.mut_opts)
            .field("mutable_state",        &self.mutable_state)
            .field("compaction_finished",  &self.compaction_finished)
            .field("close_status",         &self.close_status)
            .field("background_compactor", &self.background_compactor)
            .field("contention_queue",     &self.contention_queue)
            .field("snapshot_list",        &self.snapshot_list)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct PerHandleState<Decoders> {
    pub decoders: Decoders,
}

pub(crate) struct FrontWriterState<WriteFile, Cmp> {
    pub memtable_writer:   Memtable<Cmp>,
    pub current_write_log: WriteLogWriter<WriteFile>,
    pub next_file_number:  NextFileNumber,
}

impl<WriteFile, Cmp> Debug for FrontWriterState<WriteFile, Cmp> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("FrontWriterState")
            .field("memtable_writer",   &self.memtable_writer)
            .field("current_write_log", &self.current_write_log)
            .field("next_file_number",  &self.next_file_number)
            .finish()
    }
}

pub(crate) struct SharedMutableState<FS, Cmp, Policy, Codecs, Pool: BufferPool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Codecs: CompressionCodecs,
{
    pub lockfile:              Option<FS::Lockfile>,
    pub write_status:          Result<(), WriteErrorAlias<FS, Cmp, Codecs>>,
    pub version_set:           VersionSet<FS::WriteFile>,
    pub current_memtable:      MemtableReader<Cmp>,
    pub iter_read_sample_seed: u64,
    /// # Correctness
    /// Must be `Some(_)` if and only if `background_compactor` is `None`.
    ///
    /// Otherwise, deadlocks or other errors may occur.
    pub foreground_compactor:  Option<
        ForegroundCompactor<FS::WriteFile, Policy, Codecs::Encoders, Pool>,
    >,
    pub compaction_state:      CompactionState<Cmp>,
}

impl<FS, Cmp, Policy, Codecs, Pool> Debug for SharedMutableState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem<WriteFile: Debug, Lockfile: Debug, Error: Debug>,
    Cmp:    LevelDBComparator<InvalidKeyError: Debug>,
    Policy: Debug,
    Codecs: CompressionCodecs<Encoders: Debug, CompressionError: Debug, DecompressionError: Debug>,
    Pool:   Debug + BufferPool<PooledBuffer: Debug>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("SharedMutableState")
            .field("lockfile",              &self.lockfile)
            .field("write_status",          &self.write_status)
            .field("version_set",           &self.version_set)
            .field("current_memtable",      &self.current_memtable)
            .field("iter_read_sample_seed", &self.iter_read_sample_seed)
            .field("foreground_compactor",  &self.foreground_compactor)
            .field("compaction_state",      &self.compaction_state)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct BackgroundCompactorHandle {
    pub start_compaction: Condvar,
    pub compactor_thread: JoinHandle<()>,
}

pub(crate) struct ForegroundCompactor<File, Policy, Encoders, Pool: BufferPool> {
    pub table_builder: TableFileBuilder<File, Policy, Pool>,
    pub encoders:      Encoders,
}

impl<File, Policy, Encoders, Pool> Debug for ForegroundCompactor<File, Policy, Encoders, Pool>
where
    File:     Debug,
    Policy:   Debug,
    Encoders: Debug,
    Pool:     Debug + BufferPool<PooledBuffer: Debug>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("TableFileBuilder")
            .field("table_builder", &self.table_builder)
            .field("encoders",      &self.encoders)
            .finish()
    }
}

pub(crate) struct CompactionState<Cmp: LevelDBComparator> {
    pub has_ongoing_compaction:     bool,
    /// If `true`, do not schedule another compaction.
    ///
    /// Any ongoing compaction is still permitted to complete.
    pub suspending_compactions:     bool,
    pub memtable_under_compaction:  Option<MemtableReader<Cmp>>,
    pub pending_compaction_outputs: HashSet<FileNumber>,
    pub manual_compaction:          (),
    // TODO: compaction stats
}

impl<Cmp: LevelDBComparator> Debug for CompactionState<Cmp> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CompactionState")
            .field("has_ongoing_compaction",     &self.has_ongoing_compaction)
            .field("suspending_compactions",     &self.suspending_compactions)
            .field("memtable_under_compaction",  &self.memtable_under_compaction)
            .field("pending_compaction_outputs", &self.pending_compaction_outputs)
            .field("manual_compaction",          &self.manual_compaction)
            .finish()
    }
}
