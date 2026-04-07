use std::collections::HashSet;
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::{Arc, Condvar, Mutex},
};

use anchored_vfs::LevelDBFilesystem;

use crate::{
    all_errors::aliases::RwErrorKindAlias,
    binary_block_log::WriteLogWriter,
    snapshot::SnapshotList,
    table_file::TableFileBuilder,
    typed_bytes::OwnedInternalKey,
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
    pub_typed_bytes::{CloseStatus, FileNumber, NonZeroLevel},
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
    /// Signaled when a compaction is finished **or** when the database is being closed.
    pub compaction_finished:  Condvar,
    /// Signaled when compactions are resumed **or** when the database is being closed.
    pub resume_compactions:   Condvar,
    /// # Correctness
    /// Must be `Some(_)` if and only if `foreground_compactor` is initially `None`.
    ///
    /// Otherwise, panics, hangs, or other errors may occur.
    pub background_compactor: Option<BackgroundCompactor>,
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
            .field("resume_compactions",   &self.resume_compactions)
            .field("background_compactor", &self.background_compactor)
            .field("contention_queue",     &self.contention_queue)
            .field("snapshot_list",        &self.snapshot_list)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct PerHandleState<Decoders> {
    pub decoders:     Decoders,
    pub iter_key_buf: Vec<u8>,
}

pub(crate) struct FrontWriterState<WriteFile, Cmp> {
    pub memtable_writer:   Memtable<Cmp>,
    pub current_write_log: WriteLogWriter<WriteFile>,
}

impl<WriteFile, Cmp> Debug for FrontWriterState<WriteFile, Cmp> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("FrontWriterState")
            .field("memtable_writer",   &self.memtable_writer)
            .field("current_write_log", &self.current_write_log)
            .finish()
    }
}

pub(crate) struct SharedMutableState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    pub lockfile:                     Option<FS::Lockfile>,
    /// The number of processes which are accessing open database files while intermittently
    /// releasing the database `Mutex`, therefore needing some mechanism to ensure that the
    /// `lockfile` is not unexpectedly released.
    ///
    /// One reference count is held per top-level database iterator (including those internally
    /// used during compactions, `get`, `get_with`, and so on).
    ///
    /// `lockfile` can be set to `None` only if `lockfile_refcount == 0`.
    pub lockfile_refcount:            usize,
    /// The number of lockfile reference counts which are held by the compactor.
    ///
    /// When there is no ongoing compaction, this value is `0`.
    pub compactor_lockfile_refcounts: usize,
    /// The number of `Arc<InnerDBState>` refcounts which are **not** held by the compactor.
    pub non_compactor_arc_refcounts:  usize,
    pub close_status:                 CloseStatus,
    pub write_status:                 Result<(), RwErrorKindAlias<FS, Cmp, Codecs>>,
    pub version_set:                  VersionSet<FS::WriteFile>,
    pub current_memtable:             MemtableReader<Cmp>,
    pub iter_read_sample_seed:        u64,
    /// # Correctness
    /// Must initially be `Some(_)` if and only if `background_compactor` is `None`.
    ///
    /// Should be temporarily replaced with `None` **only briefly**, while a foreground compaction
    /// is in progress (and `compaction_state.has_ongoing_compaction` is `true`). This should be
    /// ensured with `catch_unwind` for security.
    ///
    /// Otherwise, panics, hangs, or other errors may occur.
    pub foreground_compactor:         Option<
        ForegroundCompactor<FS::WriteFile, Policy, Codecs::Encoders, Pool>,
    >,
    pub compaction_state:             CompactionState<Cmp>,
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
            .field("lockfile",                     &self.lockfile)
            .field("lockfile_refcount",            &self.lockfile_refcount)
            .field("compactor_lockfile_refcounts", &self.compactor_lockfile_refcounts)
            .field("non_compactor_arc_refcounts",  &self.non_compactor_arc_refcounts)
            .field("write_status",                 &self.write_status)
            .field("close_status",                 &self.close_status)
            .field("version_set",                  &self.version_set)
            .field("current_memtable",             &self.current_memtable)
            .field("iter_read_sample_seed",        &self.iter_read_sample_seed)
            .field("foreground_compactor",         &self.foreground_compactor)
            .field("compaction_state",             &self.compaction_state)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct BackgroundCompactor {
    /// Signaled when a compaction is started (and there's a background compactor) **or**
    /// when the database is being closed.
    pub start_compaction: Condvar,
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
    /// # Correctness
    ///
    /// This boolean **must** accurately indicate whether a compaction is ongoing (whether just
    /// signaled to start, suspended, or actually in the middle of doing work). This should be
    /// ensured with `catch_unwind` for security.
    ///
    /// Otherwise, hangs or other errors may occur.
    pub has_ongoing_compaction:     bool,
    /// If `true`, do not schedule another compaction.
    ///
    /// Any ongoing compaction is still permitted to complete.
    pub suspending_compactions:     bool,
    pub memtable_under_compaction:  Option<MemtableReader<Cmp>>,
    /// The file numbers of `.ldb`, `MANIFEST-`, and `.dbtmp` files that may be created by an
    /// ongoing compaction soon.
    pub pending_compaction_outputs: HashSet<FileNumber>,
    pub manual_compaction:          ManualCompaction,
    /// A counter to distinguish which manual compaction is currently running, for the sake of
    /// performance rather than correctness.
    ///
    /// Consider the following sequence of events:
    /// - Thread 1 starts a manual compaction.
    /// - Thread 1 waits for the manual compaction to finish.
    /// - Thread 2 wants to start a manual compaction.
    /// - Thread 2 waits for the manual compaction to finish.
    /// - The first manual compaction finishes.
    /// - Thread 2 wakes up and acquires the DB mutex before Thread 1, and starts its compaction.
    /// - Thread 1 wakes up and acquires the DB mutex. It *could* stop waiting, since its
    ///   manual compaction is complete, though it would not be incorrect for it to wait longer
    ///   than necessary.
    ///
    /// If `256` compactions occurred before thread 1 got a chance to acquire the mutex, then it
    /// would proceed to wait slightly longer than necessary. The chance of that occurring should
    /// be negligible, but it does not harm correctness either way.
    pub manual_compaction_counter:  u8,
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
            .field("manual_compaction_counter",  &self.manual_compaction_counter)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct ManualCompaction {
    /// `Some` indicates a manual compaction from level `level.unwrap().prev_level()` into level
    /// `level.unwrap()`.
    ///
    /// `None` indicates that there is not currently a manual compaction.
    pub level:       Option<NonZeroLevel>,
    pub lower_bound: Option<OwnedInternalKey>,
    pub upper_bound: Option<OwnedInternalKey>,
}
