#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to perform Polonius-style early returns of borrows"),
)]

use std::mem;
use std::{slice::Iter as SliceIter, sync::Arc};

use clone_behavior::FastMirroredClone;

use anchored_skiplist::Comparator as _;
use anchored_vfs::{LevelDBFilesystem, RandomAccess};

use crate::{
    all_errors::aliases::RwErrorKindAlias,
    file_tracking::FileMetadata,
    internal_leveldb::InternalDBState,
    pub_typed_bytes::FileNumber,
    table_file::read_sstable,
    typed_bytes::EncodedInternalEntry,
};
use crate::{
    options::{InternalReadOptions, pub_options::CacheUsage},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    sstable::{TableIter, TableReader},
    version::{CompactionInputsCow, StartCompaction},
};


/// A peekable fused iterator over compaction inputs.
///
/// This iterator never acquires database-wide locks. (Though, it does use the buffer pool,
/// caches, and so on.)
///
/// If an error is returned, further results are meaningless. (In other words, this struct
/// internally uses a loser tree, and since we know that any error will result in the iterator
/// never being used again, we don't bother to uphold the tree's invariants if an error occurs.)
pub(crate) struct CompactionInputs<'a, FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    db_state: &'a InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
    iters:    Vec<CompactionIterToMerge<'a, FS::RandomAccessFile, Policy, Pool>>,
    tree:     Box<[usize]>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a, FS, Cmp, Policy, Codecs, Pool> CompactionInputs<'a, FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    pub fn new(
        db_state:         &'a InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders:         &mut Codecs::Decoders,
        start_compaction: &StartCompaction<'a>,
        manifest_number:  FileNumber,
    ) -> Result<Self, RwErrorKindAlias<FS, Cmp, Codecs>> {
        let mut iters = Vec::new();

        match &start_compaction.base_inputs {
            CompactionInputsCow::Owned(files) => {
                for file in files {
                    let sstable = read_sstable(
                        &db_state.opts,
                        &db_state.mut_opts,
                        Self::read_opts(db_state),
                        decoders,
                        manifest_number,
                        file.file_number(),
                        file.file_size(),
                    )?;

                    let sstable_iter = TableIter::new(&sstable);
                    iters.push(CompactionIterToMerge::Single(sstable_iter, sstable));
                }
            }
            CompactionInputsCow::Borrowed(disjoint_files) => {
                let iter = DisjointCompactionInputIter::new(disjoint_files, manifest_number);
                iters.push(CompactionIterToMerge::Multiple(iter));
            }
        }

        let parent_iter = DisjointCompactionInputIter::new(
            start_compaction.parent_inputs,
            manifest_number,
        );
        iters.push(CompactionIterToMerge::Multiple(parent_iter));

        let num_internal_nodes = iters.len().next_power_of_two();
        let tree = vec![usize::MAX; num_internal_nodes].into_boxed_slice();

        let mut this = Self {
            db_state,
            iters,
            tree,
        };

        for i in 0..this.iters.len() {
            this.advance(decoders, i)?;
        }

        Ok(this)
    }

    #[inline]
    #[must_use]
    const fn read_opts(
        db_state: &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
    ) -> InternalReadOptions {
        InternalReadOptions {
            verify_data_checksums:  db_state.opts.verify_data_checksums,
            verify_index_checksums: db_state.opts.verify_index_checksums,
            block_cache_usage:      CacheUsage::Read,
            table_cache_usage:      CacheUsage::Read,
        }
    }

    fn loser(&self) -> usize {
        // The number of nodes in `self.tree` is a power of two, and is therefore at least `0`.
        #[expect(clippy::indexing_slicing, reason = "guaranteed to have at least one element")]
        self.tree[0]
    }

    pub fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        self.iters.get(self.loser())?.current()
    }

    pub fn next(
        &mut self,
        decoders: &mut Codecs::Decoders,
    ) -> Result<Option<EncodedInternalEntry<'_>>, RwErrorKindAlias<FS, Cmp, Codecs>> {
        self.advance(decoders, self.loser())
    }

    fn advance(
        &mut self,
        decoders:   &mut Codecs::Decoders,
        iter_index: usize,
    ) -> Result<Option<EncodedInternalEntry<'_>>, RwErrorKindAlias<FS, Cmp, Codecs>> {
        let Some(iter_mut) = self.iters.get_mut(iter_index) else {
            return Ok(None);
        };

        iter_mut.next(self.db_state, decoders)?;
        #[expect(
            clippy::indexing_slicing,
            reason = "we confirmed above that `iter_index` is in-bounds",
        )]
        let iter = &self.iters[iter_index];

        // Semantically, the iterators come after the internal nodes in `self.tree`.
        let mut cur_loser_index = iter_index + self.tree.len();
        let mut cur_loser = iter.current();
        // We just played the "game" located at `cur_loser_index`, with only a single player, who
        // lost. Start advancing up the tree.
        let mut cur_game_index = cur_loser_index;
        loop {
            let parent_game_index = cur_game_index / 2;
            let parent_iter_index = &mut self.tree[parent_game_index];
            let parent_entry = self.iters
                .get(*parent_iter_index)
                .and_then(CompactionIterToMerge::current);

            // Grant ties to the reigning winner (the parent).
            let parent_wins = match (cur_loser, parent_entry) {
                (Some(some_current), Some(some_parent)) => {
                    self.db_state.opts.cmp.cmp(
                        some_current.0.as_internal_key(),
                        some_parent.0.as_internal_key(),
                    ).is_le()
                }
                (None, Some(_)) => false,
                (_, None) => true,
            };

            if parent_wins {
                // The winner's index is already written to `parent_index`.
            } else {
                // Write the winner, and advance the loser (the former parent).
                mem::swap(parent_iter_index, &mut cur_loser_index);
                cur_loser = parent_entry;
            }

            if parent_game_index == 0 {
                break Ok(cur_loser);
            } else {
                cur_game_index = parent_game_index;
            }
        }
    }
}

enum CompactionIterToMerge<'a, File, Policy, Pool: BufferPool> {
    Single(TableIter<Pool>, Arc<TableReader<File, Policy, Pool>>),
    Multiple(DisjointCompactionInputIter<'a, File, Policy, Pool>),
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File, Policy, Pool: BufferPool> CompactionIterToMerge<'_, File, Policy, Pool> {
    #[must_use]
    pub fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        match self {
            Self::Single(iter, _) => iter.current(),
            Self::Multiple(iter)  => iter.current(),
        }
    }

    pub fn next<FS, Cmp, Codecs>(
        &mut self,
        db_state: &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders: &mut Codecs::Decoders,
    ) -> Result<Option<EncodedInternalEntry<'_>>, RwErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<RandomAccessFile = File>,
        File:   RandomAccess,
        Cmp:    LevelDBComparator,
        Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
        Codecs: CompressionCodecs,
    {
        match self {
            Self::Single(iter, table) => iter.next(
                table,
                &db_state.opts,
                &db_state.mut_opts,
                CompactionInputs::read_opts(db_state),
                decoders,
            ),
            Self::Multiple(iter) => iter.next(db_state, decoders),
        }
    }
}

/// A peekable fused iterator over the portion of compaction inputs in a certain nonzero level.
///
/// This iterator never acquires database-wide locks. (Though, it does use the buffer pool,
/// caches, and so on.)
struct DisjointCompactionInputIter<'a, File, Policy, Pool: BufferPool> {
    /// # Invariants
    /// - If `sstable` is `Some(_)`, then `sstable_iter` should be set to that table and be
    ///   `valid()`.
    ///
    /// If `sstable_iter` becomes `!valid()`, then a new table file should be retrieved from
    /// `file_iter`, if possible. Note that `sstable_iter` should be cleared before attempting
    /// to open a new table file, for performance reasons.
    sstable_iter:    TableIter<Pool>,
    sstable:         Option<Arc<TableReader<File, Policy, Pool>>>,
    /// # Invariants
    /// `file_iter.next()` should be the file metadata of the next SSTable. That is, if it
    /// were peekable, its `current` position should be `self.sstable`.
    file_iter:       SliceIter<'a, Arc<FileMetadata>>,
    /// The file number of the `MANIFEST` which contains the `Version` used in `self.files`.
    manifest_number: FileNumber,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a, File, Policy, Pool: BufferPool> DisjointCompactionInputIter<'a, File, Policy, Pool> {
    #[inline]
    #[must_use]
    pub fn new(files: &'a [Arc<FileMetadata>], manifest_number: FileNumber) -> Self {
        Self {
            sstable_iter: TableIter::new_empty(),
            sstable:      None,
            file_iter:    files.iter(),
            manifest_number,
        }
    }

    #[must_use]
    pub fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        if self.sstable.is_some() {
            self.sstable_iter.current()
        } else {
            None
        }
    }

    pub fn next<FS, Cmp, Codecs>(
        &mut self,
        db_state: &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders: &mut Codecs::Decoders,
    ) -> Result<Option<EncodedInternalEntry<'_>>, RwErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<RandomAccessFile = File>,
        File:   RandomAccess,
        Cmp:    LevelDBComparator,
        Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
        Codecs: CompressionCodecs,
    {
        if let Some(current_table) = &self.sstable {
            let entry = self.sstable_iter.next(
                current_table,
                &db_state.opts,
                &db_state.mut_opts,
                CompactionInputs::read_opts(db_state),
                decoders,
            )?;

            if let Some(entry) = entry {
                #[cfg(not(feature = "polonius"))]
                // SAFETY: We are just transmuting a lifetime, so we need only worry about
                // borrowck and the aliasing rules. The code compiles under Polonius, so
                // Rust's aliasing and ownership rules are satisfied.
                let entry = unsafe {
                    mem::transmute::<
                        EncodedInternalEntry<'_>,
                        EncodedInternalEntry<'_>,
                    >(entry)
                };

                return Ok(Some(entry));
            } else {
                self.sstable = None;
            }
        }

        self.next_fallback(db_state, decoders)
    }

    fn next_fallback<FS, Cmp, Codecs>(
        &mut self,
        db_state: &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders: &mut Codecs::Decoders,
    ) -> Result<Option<EncodedInternalEntry<'_>>, RwErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<RandomAccessFile = File>,
        File:   RandomAccess,
        Cmp:    LevelDBComparator,
        Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
        Codecs: CompressionCodecs,
    {
        loop {
            // If we get here, `self.sstable` is `None`. Also, note that slice iterators are fused.
            let Some(next_file) = self.file_iter.next() else {
                return Ok(None);
            };

            let sstable = read_sstable(
                &db_state.opts,
                &db_state.mut_opts,
                CompactionInputs::read_opts(db_state),
                decoders,
                self.manifest_number,
                next_file.file_number(),
                next_file.file_size(),
            )?;

            self.sstable_iter.set(&sstable);

            let entry = self.sstable_iter.next(
                &sstable,
                &db_state.opts,
                &db_state.mut_opts,
                CompactionInputs::read_opts(db_state),
                decoders,
            )?;

            if let Some(entry) = entry {
                self.sstable = Some(sstable);

                #[cfg(not(feature = "polonius"))]
                // SAFETY: We are just transmuting a lifetime, so we need only worry about
                // borrowck and the aliasing rules. The code compiles under Polonius, so
                // Rust's aliasing and ownership rules are satisfied.
                let entry = unsafe {
                    mem::transmute::<
                        EncodedInternalEntry<'_>,
                        EncodedInternalEntry<'_>,
                    >(entry)
                };

                return Ok(Some(entry));
            }
        }
    }
}
