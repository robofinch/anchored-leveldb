use clone_behavior::MirroredClone as _;

use crate::{
    file_tracking::StartSeekCompaction,
    read_sampling::IterReadSampler,
    version::Version
};
use crate::{
    leveldb_generics::{LdbContainer, LdbLockedFullShared, LevelDBGenerics},
    leveldb_iter::{InnerGenericDBIter, InternalIter},
};
use super::super::write_impl::DBWriteImpl;
use super::InnerGenericDB;


// Temporary
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    pub(crate) fn testing_iter(&self) -> InnerGenericDBIter<LDBG, WriteImpl> {
        Self::iter_without_sampler(&self.ldb_locked_shared(), self)
    }
}

// #[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    // iter
    // iter_with
    // snapshot
    // compact_range
    // compact_full
    // has_outstanding_snapshots
    // has_outstanding_iters
}

// Internal-ish utils
#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    /// Calling this method requires a lock on the database, in addition to a reference-counted
    /// clone of the database. Methods called on the returned iterator may acquire locks on the
    /// database.
    #[must_use]
    fn iter_with_sampler(
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
    fn iter_without_sampler(
        this:       &LdbLockedFullShared<'_, LDBG, WriteImpl>,
        this_clone: &Self,
    ) -> InnerGenericDBIter<LDBG, WriteImpl> {
        let iters = Self::internal_iters(this, this_clone);

        let cmp = this.0.table_options.comparator.fast_mirrored_clone();
        let sequence_number = this.1.version_set.last_sequence();
        let version = this.1.version_set.cloned_current_version();

        InnerGenericDBIter::new(cmp, None, sequence_number, version, iters)
    }

    #[must_use]
    fn internal_iters(
        this:       &LdbLockedFullShared<'_, LDBG, WriteImpl>,
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
