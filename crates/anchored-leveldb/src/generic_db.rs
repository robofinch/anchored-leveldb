use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::MirroredClone as _;
use new_clone_behavior::{FastMirroredClone as _, MirroredClone, Speed};

use crate::{
    db_shared_access::DBSharedAccess,
    file_tracking::StartSeekCompaction,
    memtable::MemtableLendingIter,
    read_sampling::IterReadSampler,
    table_traits::adapters::InternalComparator,
    write_impl::DBWriteImpl as _,
    version::version_struct::Version,
};
use crate::{
    containers::{FragileRwCell as _, RwCellFamily as _},
    db_data::{DBShared, DBSharedMutable},
    leveldb_generics::{
        LdbContainer, LdbFullShared, LdbLockedFullShared, LdbPooledBuffer, LdbRwCell,
        LdbSharedMutableWriteData, LdbSharedWriteData, LevelDBGenerics,
    },
    leveldb_iter::{InnerGenericDBIter, InternalIter},
};


pub(crate) struct InnerGenericDB<LDBG: LevelDBGenerics>(
    #[expect(clippy::type_complexity, reason = "a bunch of type aliases are used to simplify it")]
    LdbContainer<LDBG, (
        DBShared<LDBG>,
        LdbRwCell<LDBG, DBSharedMutable<LDBG>>,
    )>
);

// #[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics> InnerGenericDB<LDBG> {
    // open
    // close_writes
    // close_writes_after_compaction
    // irreversibly_delete_db
    // later: repair_db
    // later: clone_db
}

// #[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics> InnerGenericDB<LDBG> {
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
impl<LDBG: LevelDBGenerics> InnerGenericDB<LDBG> {
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

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics> InnerGenericDB<LDBG> {
    #[inline]
    #[must_use]
    pub fn ldb_shared(&self) -> LdbFullShared<'_, LDBG> {
        (self.shared(), self.shared_mutable())
    }

    #[inline]
    #[must_use]
    pub fn ldb_locked_shared(&self) -> LdbLockedFullShared<'_, LDBG> {
        (self.shared(), self.shared_mutable().write())
    }

    #[inline]
    #[must_use]
    pub fn shared(&self) -> &DBShared<LDBG> {
        &self.0.0
    }

    #[inline]
    #[must_use]
    pub fn shared_mutable(&self) -> &LdbRwCell<LDBG, DBSharedMutable<LDBG>> {
        &self.0.1
    }

    #[inline]
    #[must_use]
    pub fn cmp(&self) -> &InternalComparator<LDBG::Cmp> {
        &self.0.0.table_options.comparator
    }

    #[inline]
    #[must_use]
    pub fn shared_access(&self) -> &DBSharedAccess<LDBG> {
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
            LDBG::WriteImpl::maybe_start_compaction(locked_full_shared);
        }

        needs_compaction.version_is_current
    }
}

// Temporary implementations without corruption handlers
#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics> InnerGenericDB<LDBG> {
    /// Calling this method requires a lock on the database, in addition to a reference-counted
    /// clone of the database. Methods called on the returned iterator may acquire locks on the
    /// database.
    #[must_use]
    pub fn iter_with_sampler(
        this:       &mut LdbLockedFullShared<'_, LDBG>,
        this_clone: Self,
    ) -> InnerGenericDBIter<LDBG> {
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
    pub fn iter_without_sampler(
        this: &mut LdbLockedFullShared<'_, LDBG>,
        this_clone: Self,
    ) -> InnerGenericDBIter<LDBG> {
        let iters = Self::internal_iters(this, &this_clone);

        let cmp = this.0.table_options.comparator.fast_mirrored_clone();
        let sequence_number = this.1.version_set.last_sequence();
        let version = this.1.version_set.cloned_current_version();

        InnerGenericDBIter::new(cmp, None, sequence_number, version, iters)
    }

    #[must_use]
    fn internal_iters(
        this:       &mut LdbLockedFullShared<'_, LDBG>,
        this_clone: &Self,
    ) -> Vec<InternalIter<LDBG>> {
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

impl<LDBG: LevelDBGenerics> Clone for InnerGenericDB<LDBG> {
    #[inline]
    fn clone(&self) -> Self {
        self.fast_mirrored_clone()
    }
}

impl<LDBG: LevelDBGenerics, S: Speed> MirroredClone<S> for InnerGenericDB<LDBG> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.fast_mirrored_clone())
    }
}

impl<LDBG> Debug for InnerGenericDB<LDBG>
where
    LDBG:                            LevelDBGenerics,
    LDBG::FS:                        Debug,
    LDBG::Skiplist:                  Debug,
    LDBG::Policy:                    Debug,
    LDBG::Cmp:                       Debug,
    LDBG::Pool:                      Debug,
    LdbPooledBuffer<LDBG>:           Debug,
    LdbSharedMutableWriteData<LDBG>: Debug,
    LdbSharedWriteData<LDBG>:        Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("DB")
            .field(&self.0.0)
            .field(LDBG::RwCell::debug(&self.0.1))
            .finish()
    }
}
