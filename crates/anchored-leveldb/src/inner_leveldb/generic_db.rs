use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::MirroredClone as _;
use new_clone_behavior::{FastMirroredClone as _, MirroredClone, Speed};
use generic_container::FragileTryContainer as _;

use crate::{
    file_tracking::StartSeekCompaction,
    memtable::MemtableLendingIter,
    read_sampling::IterReadSampler,
    snapshot::SnapshotList,
    table_traits::adapters::InternalComparator,
    version::version_struct::Version,
};
use crate::{
    containers::{FragileRwCell as _, RwCellFamily as _},
    leveldb_generics::{
        LdbContainer, LdbFullShared, LdbLockedFullShared, LdbPooledBuffer, LdbRwCell,
        LevelDBGenerics,
    },
    leveldb_iter::{InnerGenericDBIter, InternalIter},
};
use super::{db_shared_access::DBSharedAccess, write_impl::DBWriteImpl};
use super::{
    builder::BuildGenericDB,
    db_data::{DBShared, DBSharedMutable},
};


pub(crate) struct InnerGenericDB<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>>(
    #[expect(clippy::type_complexity, reason = "a bunch of type aliases are used to simplify it")]
    LdbContainer<LDBG, (
        DBShared<LDBG, WriteImpl>,
        LdbRwCell<LDBG, DBSharedMutable<LDBG, WriteImpl>>,
    )>
);

// #[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    // open
    // close - halt compaction and prevent all future reads and writes from succeeding.
    // Existing iterators may start to return `None`, but are _not_ necessarily invalidated.
    // In order to ensure that the ground is not ripped out from under the iterators' feet,
    // the database lockfile is not unlocked until all outstanding iterators are dropped.
    // In other words, you must ensure that existing iterators are dropped in a timely manner.
    // If there are not outstanding iterators, this method will wait for compaction to stop,
    // then close the database and release its lockfile.
    // Ok(CloseStatus)
    // Err(_)
    // CloseStatus: EntirelyClosed, OpenDueToIterators(DB) (or OutstandingIterators)

    // Similar to close, but does not kill the current compaction, and instead waits for it
    // to finish. *other* reads and writes are blocked right away, though.
    // close_after_compaction
    // NOTE: try to have `close` and `close_after_compaction` affect iterators whenever they're
    // about to read from the filesystem, with the option to NOT affect compaction-related
    // processes.

    // irreversibly_delete_db
    // later: repair_db
    // later: clone_db
}

// #[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    // put
    // put_with
    // delete
    // delete_with
    // write
    // write_with
    // flush
    // get
    // get_with
    // iter
    // iter_with
    // snapshot
    // compact_range
    // compact_full
    // has_outstanding_snapshots
    // has_outstanding_iters
}

// #[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    // check_corruption
    // approximate_sizes
    // later: approximate_ram_usage
    // later: compaction_statistics
    // num_files_at_level
    // file_summary_with_text_keys(&self, f) -> FmtResult
    // file_summary_with_numeric_keys(&self, f) -> FmtResult
    // file_summary_with<K>(&self, f, display_key: K) -> FmtResult
    // info_log
}

impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    #[inline]
    #[must_use]
    pub fn shared(&self) -> &DBShared<LDBG, WriteImpl> {
        &self.0.0
    }

    #[inline]
    #[must_use]
    pub fn cmp(&self) -> &InternalComparator<LDBG::Cmp> {
        &self.0.0.table_options.comparator
    }

    #[inline]
    #[must_use]
    pub fn shared_access(&self) -> &DBSharedAccess<LDBG, WriteImpl> {
        DBSharedAccess::from_ref(self)
    }

    /// If the current version needs a seek compaction, attempts to start a compaction.
    ///
    /// This function returns `true` if and only if the provided version is the current version.
    ///
    /// This function acquires a database-wide lock.
    #[must_use]
    pub fn maybe_start_seek_compaction(
        &self,
        maybe_current_version: &LdbContainer<LDBG, Version<LDBG::Refcounted>>,
        start_seek_compaction: StartSeekCompaction<LDBG::Refcounted>,
    ) -> bool {
        let mut locked_full_shared = self.ldb_locked_shared();
        let needs_compaction = locked_full_shared
            .1
            .version_set
            .needs_seek_compaction(maybe_current_version, start_seek_compaction);

        if needs_compaction.needs_seek_compaction {
            WriteImpl::maybe_start_compaction(locked_full_shared);
        }

        needs_compaction.version_is_current
    }
}

// Temporary implementations without corruption handlers
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    #[must_use]
    pub(super) fn new(build_version: BuildGenericDB<LDBG, WriteImpl>) -> Self {
        // Ensure that no fields are forgotten
        let BuildGenericDB {
            db_directory,
            filesystem,
            table_cache,
            table_options,
            db_options,
            corruption_handler,
            version_set,
            current_memtable,
            current_log,
            info_logger,
            write_status,
            write_impl,
        } = build_version;

        let (write_data, mutable_write_data) = write_impl.split();

        let shared = DBShared {
            db_directory,
            filesystem,
            table_cache,
            table_options,
            db_options,
            corruption_handler,
            write_data,
        };

        let shared_mutable = DBSharedMutable {
            version_set,
            snapshot_list: SnapshotList::<LDBG::Refcounted, LDBG::RwCell>::new(),
            current_memtable,
            current_log,
            memtable_under_compaction: None,
            iter_read_sample_seed:     0,
            info_logger,
            write_status,
            mutable_write_data,
        };

        Self(LdbContainer::<LDBG, _>::new_container((
            shared,
            LdbRwCell::<LDBG, _>::new_rw_cell(shared_mutable),
        )))
    }

    #[inline]
    #[must_use]
    pub(super) fn ldb_shared(&self) -> LdbFullShared<'_, LDBG, WriteImpl> {
        (self.shared(), self.shared_mutable())
    }

    #[inline]
    #[must_use]
    pub(super) fn ldb_locked_shared(&self) -> LdbLockedFullShared<'_, LDBG, WriteImpl> {
        (self.shared(), self.shared_mutable().write())
    }

    #[inline]
    #[must_use]
    pub(super) fn shared_mutable(&self) -> &LdbRwCell<LDBG, DBSharedMutable<LDBG, WriteImpl>> {
        &self.0.1
    }

    /// Calling this method requires a lock on the database, in addition to a reference-counted
    /// clone of the database. Methods called on the returned iterator may acquire locks on the
    /// database.
    #[must_use]
    pub(super) fn iter_with_sampler(
        this:       &mut LdbLockedFullShared<'_, LDBG, WriteImpl>,
        this_clone: Self,
    ) -> InnerGenericDBIter<LDBG, WriteImpl> {
        let iters = Self::internal_iters(this, &this_clone);

        let cmp = this.0.table_options.comparator.fast_mirrored_clone();
        let sequence_number = this.1.version_set.last_sequence();
        let version = this.1.version_set.cloned_current_version();

        let seed = this.1.iter_read_sample_seed;
        this.1.iter_read_sample_seed = seed.wrapping_add(1);
        let sampler = IterReadSampler::new(this_clone, seed);

        InnerGenericDBIter::new(cmp, Some(sampler), sequence_number, version, iters)
    }

    /// Calling this method requires a lock on the database, in addition to a reference-counted
    /// clone of the database. Methods called on the returned iterator will never acquire
    /// database-wide locks.
    #[must_use]
    pub(super) fn iter_without_sampler(
        this: &mut LdbLockedFullShared<'_, LDBG, WriteImpl>,
        this_clone: Self,
    ) -> InnerGenericDBIter<LDBG, WriteImpl> {
        let iters = Self::internal_iters(this, &this_clone);

        let cmp = this.0.table_options.comparator.fast_mirrored_clone();
        let sequence_number = this.1.version_set.last_sequence();
        let version = this.1.version_set.cloned_current_version();

        InnerGenericDBIter::new(cmp, None, sequence_number, version, iters)
    }

    #[must_use]
    fn internal_iters(
        this:       &mut LdbLockedFullShared<'_, LDBG, WriteImpl>,
        this_clone: &Self,
    ) -> Vec<InternalIter<LDBG, WriteImpl>> {
        let mut iters = Vec::new();

        iters.push(InternalIter::Memtable(
            this.1.current_memtable
                .fast_mirrored_clone()
                .lending_iter(),
        ));

        if let Some(memtable_under_compaction) = &this.1.memtable_under_compaction {
            iters.push(InternalIter::Memtable(
                memtable_under_compaction.fast_mirrored_clone().lending_iter(),
            ));
        }

        this.1.version_set.current().add_iterators(this_clone.shared_access(), &mut iters);

        iters
    }
}

impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> Clone
for InnerGenericDB<LDBG, WriteImpl>
{
    #[inline]
    fn clone(&self) -> Self {
        self.fast_mirrored_clone()
    }
}

impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>, S: Speed> MirroredClone<S>
for InnerGenericDB<LDBG, WriteImpl>
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.fast_mirrored_clone())
    }
}

impl<LDBG, WriteImpl> Debug for InnerGenericDB<LDBG, WriteImpl>
where
    LDBG:                     LevelDBGenerics,
    LDBG::FS:                 Debug,
    LDBG::Skiplist:           Debug,
    LDBG::Policy:             Debug,
    LDBG::Cmp:                Debug,
    LDBG::Pool:               Debug,
    LdbPooledBuffer<LDBG>:    Debug,
    WriteImpl:                DBWriteImpl<LDBG>,
    WriteImpl::Shared:        Debug,
    WriteImpl::SharedMutable: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("DB")
            .field(&self.0.0)
            .field(LDBG::RwCell::debug(&self.0.1))
            .finish()
    }
}
