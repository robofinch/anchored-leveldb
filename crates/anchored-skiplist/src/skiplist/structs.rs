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
        // SAFETY: only the `Skiplist`, not any of its readers, call this method of `self.0.0`.
        // Note that `Skiplist: !Clone`, that we do not publicly expose the inner `Arc`, and that
        // we take a unique/exclusive/mutable reference to this `Skiplist`. Basically, this function
        // is the only thing accessing `self.0.0`. The same reasoning *will* apply to usage
        // of `self.0.0.debug_full`.
        unsafe { self.0.0.insert_with(&self.0.1, encoder) }
    }

    #[inline]
    #[must_use]
    pub fn reader(&self) -> SkiplistReader<F, U, F::Cmp> {
        SkiplistReader(Arc::clone(&self.0))
    }

    // pub fn iter(&self)

    #[must_use]
    pub fn get_entry(&self, key: Key<'_, F, U>) -> Option<Entry<'_, F, U>> {
        self.0.0.get_entry(&self.0.1, key)
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
}

#[expect(missing_debug_implementations, reason = "not a priority. TODO: debug impls")]
pub struct SkiplistReader<F, U, Cmp>(Arc<(RawSkiplist<F, U>, Cmp)>);

impl<F: SkiplistFormat<U>, U: UpperBound> SkiplistReader<F, U, F::Cmp> {
    // pub fn iter(&self)

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

#[derive(Debug, Default, Clone, Copy)]
pub struct TryResetError;

impl Display for TryResetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "could not reset a `Skiplist`, because at least one `SkiplistReader` was active")
    }
}

impl Error for TryResetError {}
