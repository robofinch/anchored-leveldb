use std::num::NonZero;

use clone_behavior::FastMirroredClone;

use anchored_skiplist::Comparator as _;
use anchored_vfs::{LevelDBFilesystem, RandomAccess};

use crate::{
    all_errors::aliases::RwErrorKindAlias,
    internal_leveldb::InternalDBState,
    options::InternalReadOptions,
    version::Version,
};
use crate::{
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    typed_bytes::{EncodedInternalEntry, InternalKey},
};
use super::iter_to_merge::{IterResult, IterToMerge};


#[derive(Debug, Clone, Copy)]
enum Direction {
    Forwards,
    Backwards,
}

/// A [`MergingIter`] takes several [`IterToMerge`]s as input, and iterates over the
/// sorted union of their entries.
///
/// The given iterators may have overlap in their keys, and can be provided in any order.
///
/// Conceptually, each [`IterToMerge`] is a circular iterator over the entries of some
/// sorted collection; this also holds of [`MergingIter`]. The collection corresponding to a
/// [`MergingIter`] is the sorted union (without de-duplication) of its given iterators'
/// collections. However, thanks to sequence numbers, there should be no duplicate keys. (If there
/// were, at the very least, behavior is not *awful*.)
///
/// # Note on backwards iteration
/// Some [`IterToMerge`] variants have better performance for forwards iteration than backwards
/// iteration. `MergingIter` itself otherwise has roughly equal performance in either direction,
/// but has overhead for switching the direction of iteration (see below for more information).
/// Moreover, switching direction does not play well with duplicate keys. Therefore,
/// [`MergingIterWithOpts::prev`], [`MergingIterWithOpts::seek_before`], and
/// [`MergingIterWithOpts::seek_to_last`] (the three methods that use backwards iteration) should
/// be avoided if possible.
///
/// The following methods need to switch direction if necessary, and iterate in a certain direction:
/// - Forwards:
///   - [`MergingIterWithOpts::next`]
/// - Backwards:
///   - [`MergingIterWithOpts::prev`]
///
/// The following methods are not impacted by the direction, but set the direction:
/// - Set direction to forwards, with no cost to a following backwards-iterating method:
///   - [`MergingIter::new`]
///   - [`MergingIterWithOpts::reset`]
/// - Set direction to forwards:
///   - [`MergingIterWithOpts::seek`]
///   - [`MergingIterWithOpts::seek_to_first`]
/// - Set direction to backwards:
///   - [`MergingIterWithOpts::seek_before`]
///   - [`MergingIterWithOpts::seek_to_last`]
///
/// The following methods do not impact and are not impacted by the direction:
/// - [`MergingIter::valid`]
/// - [`MergingIter::current`]
///
/// # Time Complexity
/// [`MergingIter::new`] takes O(1) time and O(1) space, where `n` is `iterators.len()`.
/// Switching direction, seeking, or resetting takes O(n) time. [`MergingIter::valid`] and
/// [`MergingIter::current`] are O(1). Lastly, [`MergingIterWithOpts::next`] and
/// [`MergingIterWithOpts::prev`] take O(n) time even if they do not switch direction.
// TODO: Debug impl
pub(super) struct MergingIter<File, Cmp, Policy, Pool>
where
    File:   RandomAccess,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Pool:   BufferPool,
{
    iterators:    Vec<IterToMerge<File, Cmp, Policy, Pool>>,
    /// If `Some`, the value should be 1 more than the index of the current iterator.
    ///
    /// Additionally, an invariant is: after calling any public method of `Self`, either
    /// `self.current_iter` is `None`, or the iterator it refers to is `valid()`.
    ///
    /// In the former case, no iterator in `self.iterators` should be `valid()`.
    current_iter: Option<NonZero<usize>>,
    /// If `current_iter` is `Some` and `direction` is `Forwards`, then the non-`current_iter`
    /// iterators are non-strictly in front of `current_iter`. If `Backwards`, the
    /// non-`current_iter` iterators are non-strictly behind `current_iter`.
    ///
    /// (Non-strictly is specified to clarify behavior for duplicate keys.)
    direction:    Direction,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File, Cmp, Policy, Pool> MergingIter<File, Cmp, Policy, Pool>
where
    File:   RandomAccess,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Pool:   BufferPool,
{
    /// Create a new [`MergingIter`]. See the type-level documentation for details on behavior.
    ///
    /// # Panics
    /// Panics if the length of `iterators` is `usize::MAX`. Any other number of iterators
    /// can, theoretically, be merged.
    #[inline]
    #[must_use]
    pub fn new(iterators: Vec<IterToMerge<File, Cmp, Policy, Pool>>) -> Self {
        assert_ne!(
            iterators.len(),
            usize::MAX,
            "Cannot create a MergingIter over `usize::MAX`-many iterators",
        );

        Self {
            iterators,
            current_iter: None,
            direction:    Direction::Forwards,
        }
    }

    #[inline]
    #[must_use]
    pub const fn with_opts<'a, FS, Codecs>(
        &'a mut self,
        version:   &'a Version,
        db_state:  &'a InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        read_opts: InternalReadOptions
    ) -> MergingIterWithOpts<'a, FS, Cmp, Policy, Codecs, Pool>
    where
        FS:     LevelDBFilesystem<RandomAccessFile = File>,
        Codecs: CompressionCodecs,
    {
        MergingIterWithOpts {
            iter: self,
            version,
            db_state,
            read_opts,
        }
    }

    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.current_iter.is_some()
    }

    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        self.get_current_iter_ref()?.current()
    }

    #[inline]
    #[must_use]
    fn get_current_iter_ref(
        &self,
    ) -> Option<&IterToMerge<File, Cmp, Policy, Pool>> {
        let current_idx = self.current_iter?.get() - 1;

        #[expect(
            clippy::indexing_slicing,
            reason = "`self.iterators` is never truncated, \
                      and `self.current_idx` is always a valid idx if `Some`",
        )]
        Some(&self.iterators[current_idx])
    }
}

pub(super) struct MergingIterWithOpts<'a, FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    iter:      &'a mut MergingIter<FS::RandomAccessFile, Cmp, Policy, Pool>,
    version:   &'a Version,
    db_state:  &'a InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
    read_opts: InternalReadOptions,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> MergingIterWithOpts<'_, FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    pub fn reset(&mut self) {
        for iter in &mut self.iter.iterators {
            iter.reset(self.version);
        }
        self.iter.current_iter = None;
        // Note that the direction doesn't actually matter when `self.current_iter` is `None`,
        // but forwards is the default.
        self.iter.direction = Direction::Forwards;
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> MergingIterWithOpts<'_, FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// Set `self.current_iter` to the iterator with the smallest `current` key, among the
    /// iterators in `self.iterators` which are valid.
    fn find_smallest_iter(&mut self) {
        let cmp = &self.db_state.opts.cmp;
        let mut smallest = None;

        for (idx, iter) in self.iter.iterators.iter().enumerate() {
            if let Some(curr_entry) = iter.current() {
                let curr_key = curr_entry.0.as_internal_key();
                if let Some((_, smallest_key)) = smallest {
                    if cmp.cmp(curr_key, smallest_key).is_lt() {
                        // `curr_key` is smaller than the previous `smallest`'s key
                        smallest = Some((idx, curr_key));
                    }
                } else {
                    // de-facto `smallest`, nothing was previously found
                    smallest = Some((idx, curr_key));
                }
            } else {
                // The iterator was `!valid()`, so continue.
            }
        }

        #[expect(clippy::unwrap_used, reason = "MergingIter cannot have `usize::MAX` iterators")]
        {
            self.iter.current_iter = smallest.map(|(idx, _)| NonZero::new(idx + 1).unwrap());
        };
    }

    /// Set `self.iter.current_iter` to the iterator with the largest `current` key, among the
    /// iterators in `self.iterators` which are valid.
    fn find_largest_iter(&mut self) {
        let cmp = &self.db_state.opts.cmp;
        let mut largest = None;

        for (idx, iter) in self.iter.iterators.iter().enumerate().rev() {
            if let Some(curr_entry) = iter.current() {
                let curr_key = curr_entry.0.as_internal_key();
                if let Some((_, largest_key)) = largest {
                    if cmp.cmp(curr_key, largest_key).is_gt() {
                        // `curr_key` is larger than the previous `largest`'s key
                        largest = Some((idx, curr_key));
                    }
                } else {
                    // de-facto `largest`, nothing was previously found
                    largest = Some((idx, curr_key));
                }
            } else {
                // The iterator was `!valid()`, so continue.
            }
        }

        #[expect(clippy::unwrap_used, reason = "MergingIter cannot have `usize::MAX` iterators")]
        {
            self.iter.current_iter = largest.map(|(idx, _)| NonZero::new(idx + 1).unwrap());
        };
    }

    /// For use only in `next`.
    ///
    /// Move all non-`current_iter` iterators one entry strictly in front of `current_iter`.
    fn switch_to_forwards(
        &mut self,
        decoders:    &mut Codecs::Decoders,
        current_idx: NonZero<usize>,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>> {
        let cmp = &self.db_state.opts.cmp;
        let current_idx = current_idx.get() - 1;

        // Do a little game to satisfy borrowck and aliasing rules
        let (iters, current_and_later) = self.iter.iterators.split_at_mut(current_idx);
        let (current_iter, other_iters) = current_and_later.split_at_mut(1);
        #[expect(clippy::indexing_slicing, reason = "`current_idx` is a valid index")]
        let current_iter = &mut current_iter[0];

        #[expect(
            clippy::unwrap_used,
            reason = "the current iterator is `valid()` as an invariant",
        )]
        let current_key = current_iter.current().unwrap().0.as_internal_key();

        for iter in iters {
            iter.seek(self.version, self.db_state, decoders, self.read_opts, current_key)?;

            // `seek` provides a `geq` order, we want a strict greater-than order.
            if iter.current().is_some_and(|entry| {
                cmp.cmp(current_key, entry.0.as_internal_key()).is_eq()
            }) {
                iter.next(self.version, self.db_state, decoders, self.read_opts)?;
            }
        }

        for iter in other_iters {
            iter.seek(self.version, self.db_state, decoders, self.read_opts, current_key)?;

            if iter.current().is_some_and(|entry| {
                cmp.cmp(current_key, entry.0.as_internal_key()).is_eq()
            }) {
                iter.next(self.version, self.db_state, decoders, self.read_opts)?;
            }
        }

        self.iter.direction = Direction::Forwards;

        Ok(())
    }

    /// For use only in `prev`.
    ///
    /// Move all non-`current_iter` iterators one entry strictly behind `current_iter`.
    fn switch_to_backwards(
        &mut self,
        decoders:    &mut Codecs::Decoders,
        current_idx: NonZero<usize>,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>> {
        let current_idx = current_idx.get() - 1;

        // Do a little game to satisfy borrowck and aliasing rules
        let (iters, current_and_later) = self.iter.iterators.split_at_mut(current_idx);
        let (current_iter, other_iters) = current_and_later.split_at_mut(1);
        #[expect(clippy::indexing_slicing, reason = "`current_idx` is a valid index")]
        let current_iter = &mut current_iter[0];

        #[expect(
            clippy::unwrap_used,
            reason = "the current iterator is `valid()` as an invariant",
        )]
        let current_key = current_iter.current().unwrap().0.as_internal_key();

        for iter in iters {
            iter.seek_before(self.version, self.db_state, decoders, self.read_opts, current_key)?;
        }
        for iter in other_iters {
            iter.seek_before(self.version, self.db_state, decoders, self.read_opts, current_key)?;
        }

        self.iter.direction = Direction::Backwards;

        Ok(())
    }

    pub fn next(&mut self, decoders: &mut Codecs::Decoders) -> IterResult<'_, FS, Cmp, Codecs> {
        if let Some(current_idx) = self.iter.current_iter {
            if matches!(self.iter.direction, Direction::Backwards) {
                self.switch_to_forwards(decoders, current_idx)?;
            }

            #[expect(clippy::indexing_slicing, reason = "we know that it's a valid index")]
            let current_iter = &mut self.iter.iterators[current_idx.get() - 1];

            // Before this call, `current_iter` is the (non-strictly) smallest iter.
            // Move it forwards...
            current_iter.next(self.version, self.db_state, decoders, self.read_opts)?;
            // And find the new smallest iter.
            self.find_smallest_iter();

        } else {
            // In this branch, we're `!valid()`. This means that _every_ iterator is currently
            // `!valid()`.
            // Move every iterator forwards one, and find the smallest.
            for iter in &mut self.iter.iterators {
                iter.next(self.version, self.db_state, decoders, self.read_opts)?;
            }

            self.find_smallest_iter();
            self.iter.direction = Direction::Forwards;
        }

        Ok(self.iter.current())
    }

    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        self.iter.current()
    }

    /// Move the iterator one position back, and return the entry at that position.
    /// Returns `None` if the iterator was at the first entry.
    ///
    /// The inner `Iter` iterators may have worse performance for backwards iteration than forwards
    /// iteration, so prefer to not use `prev`. Additionally, [`MergingIter`] has overhead
    /// for switching between backwards and forwards iteration; check the type-level documentation
    /// if you wish to use `prev`.
    pub fn prev(&mut self, decoders: &mut Codecs::Decoders) -> IterResult<'_, FS, Cmp, Codecs> {
        if let Some(current_idx) = self.iter.current_iter {
            if matches!(self.iter.direction, Direction::Forwards) {
                self.switch_to_backwards(decoders, current_idx)?;
            }

            #[expect(clippy::indexing_slicing, reason = "we know that it's a valid index")]
            let current_iter = &mut self.iter.iterators[current_idx.get() - 1];

            // Before this call, `current_iter` is the largest iter. Move it backwards...
            current_iter.prev(self.version, self.db_state, decoders, self.read_opts)?;
            // And find the new largest iter.
            self.find_largest_iter();

        } else {
            // In this branch, we're `!valid()`. This means that _every_ iterator is currently
            // `!valid()`.
            // Move every iterator backwards one, and find the largest.
            for iter in &mut self.iter.iterators {
                iter.prev(self.version, self.db_state, decoders, self.read_opts)?;
            }

            self.find_largest_iter();
            self.iter.direction = Direction::Backwards;
        }

        Ok(self.iter.current())
    }

    pub fn seek(
        &mut self,
        decoders:    &mut Codecs::Decoders,
        lower_bound: InternalKey<'_>,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>> {
        for iter in &mut self.iter.iterators {
            iter.seek(self.version, self.db_state, decoders, self.read_opts, lower_bound)?;
        }

        self.find_smallest_iter();
        self.iter.direction = Direction::Forwards;
        Ok(())
    }

    /// Move the iterator to the greatest key which is strictly less than the provided
    /// `strict_upper_bound`.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the collection).
    ///
    /// The inner `Iter` iterators may have worse performance for `seek_before` than [`seek`].
    /// Additionally, [`MergingIter`] has overhead for switching between backwards and forwards
    /// iteration; check the type-level documentation if you wish to use `seek_before`.
    ///
    /// [`seek`]: MergingIter::seek
    pub fn seek_before(
        &mut self,
        decoders:           &mut Codecs::Decoders,
        strict_upper_bound: InternalKey<'_>,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>> {
        for iter in &mut self.iter.iterators {
            iter.seek_before(
                self.version,
                self.db_state,
                decoders,
                self.read_opts,
                strict_upper_bound,
            )?;
        }

        self.find_largest_iter();
        self.iter.direction = Direction::Backwards;
        Ok(())
    }

    pub fn seek_to_first(
        &mut self,
        decoders: &mut Codecs::Decoders,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>> {
        for iter in &mut self.iter.iterators {
            iter.seek_to_first(self.version, self.db_state, decoders, self.read_opts)?;
        }

        self.find_smallest_iter();
        self.iter.direction = Direction::Forwards;
        Ok(())
    }

    /// Move the iterator to the greatest key in the collection.
    ///
    /// If the collection is empty, the iterator is `!valid()`.
    ///
    /// [`MergingIter`] has overhead for switching between backwards and forwards
    /// iteration; check the type-level documentation if you wish to use `seek_before`.
    pub fn seek_to_last(
        &mut self,
        decoders: &mut Codecs::Decoders,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>> {
        for iter in &mut self.iter.iterators {
            iter.seek_to_last(self.version, self.db_state, decoders, self.read_opts)?;
        }

        self.find_largest_iter();
        self.iter.direction = Direction::Backwards;
        Ok(())
    }
}
