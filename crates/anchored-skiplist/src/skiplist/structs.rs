#![expect(unsafe_code, reason = "assert that `insert_with` (and `debug_full`) is synchronized")]
// The external synchronization is not strictly necessary, but the `unsafe` code for lifetime
// erasure and the node format is already complicated (and unavoidable). Might as well put in
// marginally more effort in, and avoid the overhead of a mutex.

use core::error::Error;
use core::fmt::{Display, Formatter, Result as FmtResult};

use clone_behavior::{MirroredClone, Speed};
use variance_family::UpperBound;

use crate::maybe_loom::Arc;
use crate::{
    raw_skiplist::{AllocErr, RawSkiplist},
    interface::{EncodeWith, Entry, Key, SkiplistFormat},
};
use super::iter::{SkiplistIter, SkiplistLendingIter};


#[expect(missing_debug_implementations, reason = "not a priority. TODO: debug impls")]
pub struct UniqueSkiplist<F, U, Cmp>(RawSkiplist<F, U>, Cmp);

impl<F: SkiplistFormat<U>, U: UpperBound> UniqueSkiplist<F, U, F::Cmp> {
    #[inline]
    #[must_use]
    pub fn new(capacity: usize, seed: u64) -> Self
    where
        F::Cmp: Default,
    {
        Self::new_with_cmp(capacity, seed, F::Cmp::default())
    }

    #[inline]
    #[must_use]
    pub fn new_with_cmp(capacity: usize, seed: u64, cmp: F::Cmp) -> Self {
        Self(RawSkiplist::new(capacity, seed), cmp)
    }

    /// Insert an entry into the skiplist.
    ///
    /// If the keys of multiple entries in the skiplist compared equal, it is unspecified what
    /// order they have amongst themselves. (The skiplist will still be sorted in ascending order.)
    pub fn insert_with<E>(&mut self, encoder: E) -> Result<Entry<'_, F, U>, AllocErr>
    where
        F: EncodeWith<E, U>,
    {
        // SAFETY: We hold exclusive access permissions over `self` and thus also `self.0`,
        // so it's impossible for this call to `insert_into` to be concurrent with *any* other
        // access to `self.0` (except for those made within `insert_into`).
        unsafe { self.0.insert_with(&self.1, encoder) }
    }

    #[inline]
    #[must_use]
    pub fn into_shareable(self) -> Skiplist<F, U, F::Cmp> {
        Skiplist(Arc::new((self.0, self.1)))
    }

    #[inline]
    #[must_use]
    pub const fn iter(&self) -> SkiplistIter<'_, F, U, F::Cmp> {
        SkiplistIter::new(&self.0, &self.1)
    }

    #[must_use]
    pub fn get_entry(&self, key: Key<'_, F, U>) -> Option<Entry<'_, F, U>> {
        self.0.get_entry(&self.1, key)
    }

    #[inline]
    #[must_use]
    pub const fn cmp(&self) -> &F::Cmp {
        &self.1
    }

    pub fn reset(&mut self) {
        self.0.reset();
    }

    /// The remaining capacity of the current chunk of the skiplist's bump allocator.
    ///
    /// If the skiplist has just been reset and nothing has been inserted yet, the returned
    /// value is the capacity of the skiplist.
    pub fn chunk_capacity(&mut self) -> usize {
        // SAFETY: We hold exclusive access permissions over `self` and thus also `self.0`,
        // so it's impossible for this call to `chunk_capacity` to be concurrent with *any* other
        // access to `self.0` (except for those made within `chunk_capacity`).
        unsafe { self.0.chunk_capacity() }
    }

    /// The total number of bytes allocated in this skiplist (excluding allocator metadata, but
    /// including padding).
    pub fn allocated_bytes(&mut self) -> usize {
        // SAFETY: We hold exclusive access permissions over `self` and thus also `self.0`,
        // so it's impossible for this call to `allocated_bytes` to be concurrent with *any* other
        // access to `self.0` (except for those made within `allocated_bytes`).
        unsafe { self.0.allocated_bytes() }
    }
}

impl<'a, F: SkiplistFormat<U>, U: UpperBound> IntoIterator for &'a UniqueSkiplist<F, U, F::Cmp> {
    type IntoIter = SkiplistIter<'a, F, U, F::Cmp>;
    type Item = Entry<'a, F, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[expect(missing_debug_implementations, reason = "not a priority. TODO: debug impls")]
pub struct Skiplist<F, U, Cmp>(
    /// # Safety invariant
    /// At most one instance of this `Arc` may be the `Skiplist` writer; any others must be
    /// `SkiplistReader`s.
    Arc<(RawSkiplist<F, U>, Cmp)>,
);

impl<F: SkiplistFormat<U>, U: UpperBound> Skiplist<F, U, F::Cmp> {
    #[inline]
    #[must_use]
    pub fn new(capacity: usize, seed: u64) -> Self
    where
        F::Cmp: Default,
    {
        Self::new_with_cmp(capacity, seed, F::Cmp::default())
    }

    #[inline]
    #[must_use]
    pub fn new_with_cmp(capacity: usize, seed: u64, cmp: F::Cmp) -> Self {
        // Safety invariant: trivially met, the new `Arc` is uniquely owned here.
        Self(Arc::new((RawSkiplist::new(capacity, seed), cmp)))
    }

    /// Insert an entry into the skiplist.
    ///
    /// If the keys of multiple entries in the skiplist compared equal, it is unspecified what
    /// order they have amongst themselves. (The skiplist will still be sorted in ascending order.)
    pub fn insert_with<E>(&mut self, encoder: E) -> Result<Entry<'_, F, U>, AllocErr>
    where
        F: EncodeWith<E, U>,
    {
        // SAFETY: Only the `Skiplist`, not any of its readers, call this method of `self.0.0`.
        // Note that `Skiplist: !Clone`, that we do not publicly expose the inner `Arc`, and that
        // we take a unique/exclusive/mutable reference to this `Skiplist`. Basically, this function
        // is the only thing accessing `self.0.0`. The same reasoning *will* apply to usage
        // of `self.0.0.debug_full` and similar functions.
        unsafe { self.0.0.insert_with(&self.0.1, encoder) }
    }

    #[inline]
    #[must_use]
    pub fn reader(&self) -> SkiplistReader<F, U, F::Cmp> {
        SkiplistReader(Arc::clone(&self.0))
    }

    #[inline]
    #[must_use]
    pub fn into_reader(self) -> SkiplistReader<F, U, F::Cmp> {
        SkiplistReader(self.0)
    }

    #[inline]
    #[must_use]
    pub fn iter(&self) -> SkiplistIter<'_, F, U, F::Cmp> {
        SkiplistIter::new(&self.0.0, &self.0.1)
    }

    #[must_use]
    pub fn get_entry(&self, key: Key<'_, F, U>) -> Option<Entry<'_, F, U>> {
        self.0.0.get_entry(&self.0.1, key)
    }

    #[inline]
    #[must_use]
    pub fn cmp(&self) -> &F::Cmp {
        &self.0.1
    }

    pub fn try_reset(&mut self) -> Result<(), TryResetError> {
        if let Some((skiplist_mut, _)) = Arc::get_mut(&mut self.0) {
            skiplist_mut.reset();
            Ok(())
        } else {
            Err(TryResetError)
        }
    }

    #[must_use]
    pub fn into_reset(mut self) -> Option<Self> {
        // Try to optimistically reuse the allocation first, and fall back to
        // `Arc::into_inner`.
        if let Some((skiplist_mut, _)) = Arc::get_mut(&mut self.0) {
            skiplist_mut.reset();
            Some(self)
        } else {
            Arc::into_inner(self.0).map(|(mut skiplist, cmp)| {
                skiplist.reset();
                // Safety invariant: trivially met, the new `Arc` is uniquely owned here.
                Self(Arc::new((skiplist, cmp)))
            })
        }
    }

    /// The remaining capacity of the current chunk of the skiplist's bump allocator.
    ///
    /// If the skiplist has just been reset and nothing has been inserted yet, the returned
    /// value is the capacity of the skiplist.
    pub fn chunk_capacity(&mut self) -> usize {
        // SAFETY: See the safety comment of `Self::insert_with`. TLDR, this `Skiplist`
        // is the unique thing with write privileges / exclusive access privileges over the
        // raw skiplist, and none of the readers will access the skiplist's bump allocator.
        // We take a `&mut self` argument, so this call doesn't race with other access to the bump.
        unsafe { self.0.0.chunk_capacity() }
    }

    /// The total number of bytes allocated in this skiplist (excluding allocator metadata, but
    /// including padding).
    pub fn allocated_bytes(&mut self) -> usize {
        // SAFETY: See the safety comment of `Self::insert_with`. TLDR, this `Skiplist`
        // is the unique thing with write privileges / exclusive access privileges over the
        // raw skiplist, and none of the readers will access the skiplist's bump allocator.
        // We take a `&mut self` argument, so this call doesn't race with other access to the bump.
        unsafe { self.0.0.allocated_bytes() }
    }
}

impl<'a, F: SkiplistFormat<U>, U: UpperBound> IntoIterator for &'a Skiplist<F, U, F::Cmp> {
    type IntoIter = SkiplistIter<'a, F, U, F::Cmp>;
    type Item = Entry<'a, F, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[expect(missing_debug_implementations, reason = "not a priority. TODO: debug impls")]
pub struct SkiplistReader<F, U, Cmp>(Arc<(RawSkiplist<F, U>, Cmp)>);

impl<F: SkiplistFormat<U>, U: UpperBound> SkiplistReader<F, U, F::Cmp> {
    #[inline]
    #[must_use]
    pub fn iter(&self) -> SkiplistIter<'_, F, U, F::Cmp> {
        SkiplistIter::new(&self.0.0, &self.0.1)
    }

    #[inline]
    #[must_use]
    pub const fn lending_iter(self) -> SkiplistLendingIter<F, U, F::Cmp> {
        SkiplistLendingIter::new(self)
    }

    #[must_use]
    pub fn get_entry(&self, key: Key<'_, F, U>) -> Option<Entry<'_, F, U>> {
        self.0.0.get_entry(&self.0.1, key)
    }

    pub fn writer(&mut self) -> Option<Skiplist<F, U, F::Cmp>> {
        if Arc::get_mut(&mut self.0).is_some() {
            // Safety invariant: If `get_mut` succeeds, then this reader held the only reference
            // count, which implies there is no existing writer. Therefore, we can create one
            // new writer.
            Some(Skiplist(Arc::clone(&self.0)))
        } else {
            None
        }
    }

    pub fn into_writer(mut self) -> Result<Skiplist<F, U, F::Cmp>, Self> {
        if Arc::get_mut(&mut self.0).is_some() {
            // Safety invariant: If `get_mut` succeeds, then this reader held the only reference
            // count, which implies there is no existing writer. Therefore, we can create one
            // new writer.
            Ok(Skiplist(self.0))
        } else {
            Err(self)
        }
    }

    #[inline]
    #[must_use]
    pub fn cmp(&self) -> &F::Cmp {
        &self.0.1
    }

    #[must_use]
    pub fn into_reset(mut self) -> Option<Skiplist<F, U, F::Cmp>> {
        // Try to optimistically reuse the allocation first, and fall back to
        // `Arc::into_inner`.
        if let Some((skiplist_mut, _)) = Arc::get_mut(&mut self.0) {
            skiplist_mut.reset();
            // Safety invariant: If `get_mut` succeeds, then this reader held the only reference
            // count, which implies there is no existing writer. Therefore, we can create one
            // new writer.
            Some(Skiplist(self.0))
        } else {
            Arc::into_inner(self.0).map(|(mut skiplist, cmp)| {
                skiplist.reset();
                // Safety invariant: Same as above. If `into_inner` succeeds, there was no writer.
                Skiplist(Arc::new((skiplist, cmp)))
            })
        }
    }

    #[inline]
    #[must_use]
    pub(super) fn raw_parts(&self) -> (&RawSkiplist<F, U>, &F::Cmp) {
        (&self.0.0, &self.0.1)
    }
}

impl<F, U, Cmp> Clone for SkiplistReader<F, U, Cmp> {
    #[inline]
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<F, U, Cmp, S: Speed> MirroredClone<S> for SkiplistReader<F, U, Cmp> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        self.clone()
    }
}

impl<'a, F: SkiplistFormat<U>, U: UpperBound> IntoIterator for &'a SkiplistReader<F, U, F::Cmp> {
    type IntoIter = SkiplistIter<'a, F, U, F::Cmp>;
    type Item = Entry<'a, F, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TryResetError;

impl Display for TryResetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "could not reset a `Skiplist`, because at least one `SkiplistReader` was active")
    }
}

impl Error for TryResetError {}
