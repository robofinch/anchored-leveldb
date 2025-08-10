#![expect(
    unsafe_code,
    reason = "preserve invariance of inputs in a nominally covariant struct, \
              and assert that `Bump`s live longer than the lifetimes of provided references",
)]

use std::{marker::PhantomPinned, pin::Pin};

use aliasable::boxed::{AliasableBox, UniqueBox};
use bumpalo::Bump;
use oorandom::Rand32;

use crate::{skiplistiter_wrapper, skiplistlendingiter_wrapper};
use crate::{
    interface::{Comparator, Skiplist, SkiplistIterator, SkiplistLendingIterator},
    iter_defaults::{SkiplistIter, SkiplistLendingIter},
    node_heights::{Prng32, MAX_HEIGHT},
};
// `iter_defaults` needs to run tests on a list.
#[cfg(test)]
use crate::iter_defaults::SkiplistSeek;
use super::{
    list_inner::{SingleThreadedSkiplist, SkiplistState},
    node::{ErasedLink, Link},
};


// ================================
//  State
// ================================

#[derive(Debug)]
struct SimpleState {
    /// Invariant: all inserted nodes must have been allocated with `self.arena`.
    /// (If solely [`SimpleState::set_head_skip`] is used, this invariant is upheld.)
    /// This field should be the first for drop order, just in case.
    head:           [ErasedLink; MAX_HEIGHT],
    /// Vital invariant: after construction, the `Bump` must never be dropped, moved, or otherwise
    /// invalidate, up until this `SimpleState` is dropped or otherwise invalidated aside from
    /// by being moved.
    ///
    /// Note that `AliasableBox`s have stable deref addresses, even without `Pin`.
    ///
    /// This struct is self-referential via the below `ErasedLink`s.
    arena:          Pin<AliasableBox<(Bump, PhantomPinned)>>,
    prng:           Rand32,
    current_height: usize,
}

impl Prng32 for SimpleState {
    #[inline]
    fn rand_u32(&mut self) -> u32 {
        self.prng.rand_u32()
    }
}

// SAFETY:
// We don't do something dumb like internal mutability for which `Bump` allocator is returned.
// The same `Bump` allocator is returned every time, and we don't drop it early. And the arena in
// the pinned `AliasableBox` does not move the address of its contained `Bump`, even when the
// `SimpleState` and `arena` are moved.
//
// The links stored in `self` which `head_skip` can return were initially constructed as `None`
// and are only mutated by `set_head_skip`. Since the unsafe contract of `set_head_skip` is the
// exact same, the second condition is met.
unsafe impl SkiplistState for SimpleState {
    #[inline]
    fn new_seeded(seed: u64) -> Self {
        let arena = (Bump::new(), PhantomPinned);
        let pinned_arena = AliasableBox::from_unique_pin(UniqueBox::pin(arena));

        Self {
            head:           [ErasedLink::new_null(); MAX_HEIGHT],
            arena:          pinned_arena,
            prng:           Rand32::new(seed),
            current_height: 0,
        }
    }

    #[inline]
    fn bump(&self) -> &Bump {
        &self.arena.0
    }

    #[inline]
    fn current_height(&self) -> usize {
        self.current_height
    }

    /// # Panics
    /// May or may not panic if `current_height` is greater than [`MAX_HEIGHT`].
    #[inline]
    fn set_current_height(&mut self, current_height: usize) {
        debug_assert!(
            current_height <= MAX_HEIGHT,
            "crate should not attempt to generate a height more than `MAX_HEIGHT`",
        );

        self.current_height = current_height;
    }

    /// If the returned [`Link`] references a [`Node`], then that node was allocated in
    /// the [`self.bump()`] bump allocator.
    ///
    /// # Panics
    /// Panics if `level` is greater than or equal to [`MAX_HEIGHT`].
    ///
    /// [`self.bump()`]: SkiplistState::bump
    #[inline]
    fn head_skip(&self, level: usize) -> Link<'_> {
        #[expect(clippy::indexing_slicing, reason = "Max index is statically known")]
        let erased = self.head[level];

        // SAFETY:
        // The only source of non-None head links is `set_head_skip`, so if `erased` contains
        // a node, it was allocated in `self.bump()`. Since the address of the `Bump` in
        // `self.arena` has not changed since then, and since `self` remains borrowed and thus
        // valid for at least `'_` longer, it follows that the `Bump` has not been dropped, moved,
        // or otherwise invalidated since the node was obtained, and will not be for `'_`.
        unsafe { erased.into_link() }
    }

    /// # Safety
    /// If the provided `link` is a `Some` value, referencing a `Node`, then that node must have
    /// been allocated in the [`Bump`] allocator which can be obtained from [`self.bump()`].
    ///
    /// # Panics
    /// Panics if `level` is greater than or equal to [`MAX_HEIGHT`].
    ///
    /// [`self.bump()`]: SkiplistState::bump
    unsafe fn set_head_skip(&mut self, level: usize, link: Link<'_>) {
        #![expect(clippy::indexing_slicing, reason = "Max index is statically known")]
        self.head[level] = ErasedLink::from_link(link);
    }
}

// ================================
//  List
// ================================

/// A single-threaded skiplist which can only be accessed through a single handle.
///
/// The [`Skiplist`] trait must be imported to use the list effectively.
#[derive(Debug)]
pub struct SimpleSkiplist<Cmp>(SingleThreadedSkiplist<Cmp, SimpleState>);

impl<Cmp: Comparator + Default> Default for SimpleSkiplist<Cmp> {
    #[inline]
    fn default() -> Self {
        Self::new(Cmp::default())
    }
}

// `iter_defaults` needs to run tests on a list.
#[cfg(test)]
impl<Cmp: Comparator> SimpleSkiplist<Cmp> {
    #[inline]
    pub(crate) fn get_list_seek(self) -> impl SkiplistSeek {
        self.0
    }
}

#[expect(clippy::into_iter_without_iter, reason = ".iter() is provided by Skiplist trait")]
impl<'a, Cmp: Comparator> IntoIterator for &'a SimpleSkiplist<Cmp> {
    type IntoIter = Iter<'a, Cmp>;
    type Item     = &'a [u8];

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<Cmp: Comparator> Skiplist<Cmp> for SimpleSkiplist<Cmp> {
    /// Since this skiplist is single-threaded, there are no write locks that `Self::WriteLocked`
    /// would need to hold.
    type WriteLocked = Self;
    type Iter<'a>    = Iter<'a, Cmp> where Self: 'a;
    type LendingIter = LendingIter<Cmp>;

    #[inline]
    fn new_seeded(cmp: Cmp, seed: u64) -> Self {
        Self(SingleThreadedSkiplist::new_seeded(cmp, seed))
    }

    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `init_entry`.
    ///
    /// If the resulting entry compares equal to an entry already in the skiplist, the entry
    /// is discarded, and `false` is returned. Otherwise, `true` is returned. Attempting to add
    /// duplicate entries should be avoided, as the spent memory will not be reclaimed until the
    /// skiplist is dropped.
    // Note: `SimpleSkiplist` is not cloneable, so sound Rust is incapable of mutably accessing
    // this `SimpleSkiplist` inside `init_entry`. Therefore, there's no risk of panicking, aside
    // from allocation failures, or `init_entry` panicking.
    fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F) -> bool {
        self.0.insert_with(entry_len, init_entry)
    }

    /// Since this skiplist is single-threaded, `write_locked` is a no-op.
    #[inline]
    fn write_locked(self) -> Self::WriteLocked {
        self
    }

    /// Since this skiplist is single-threaded, `write_unlocked` is a no-op.
    #[inline]
    fn write_unlocked(list: Self::WriteLocked) -> Self {
        list
    }

    fn contains(&self, entry: &[u8]) -> bool {
        self.0.contains(entry)
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        Iter::new(self)
    }

    #[inline]
    fn lending_iter(self) -> Self::LendingIter {
        LendingIter::new(self)
    }

    #[inline]
    fn from_lending_iter(lending_iter: Self::LendingIter) -> Self {
        lending_iter.into_list()
    }
}

// ================================
//  Iter and LendingIter
// ================================

skiplistiter_wrapper! {
    /// # Safety of lifetime extension
    /// The returned entry references remain valid until the [`SimpleSkiplist`] containing the entry
    /// is dropped or otherwise invalidated, aside from by being moved. (Neither
    /// [`SimpleSkiplist::lending_iter`] nor [`SimpleSkiplist::from_lending_iter`] invalidate the
    /// backing storage; they move the skiplist, but the backing storage remains at a stable
    /// address.)
    ///
    /// The returned entry references may be lifetime-extended, provided that the backing
    /// [`SimpleSkiplist`] or [`LendingIter`] is not invalidated in the ways described above for at
    /// least the length of the modified lifetime.
    ///
    /// In particular, these assurances apply to [`Iterator`] methods, [`Iter::current`], and
    /// [`Iter::prev`].
    ///
    /// Extending the lifetime of the `Iter` itself is *not* covered by the above guarantees, and
    /// may be unsound.
    #[derive(Debug, Clone)]
    pub struct Iter<'_, Cmp: _>(#[List = SingleThreadedSkiplist<Cmp, SimpleState>] _);
}

impl<'a, Cmp: Comparator> Iter<'a, Cmp> {
    #[inline]
    #[must_use]
    const fn new(list: &'a SimpleSkiplist<Cmp>) -> Self {
        Self(SkiplistIter::new(&list.0))
    }
}

skiplistlendingiter_wrapper! {
    /// # Safety of lifetime extension
    /// The returned entry references remain valid until the [`SimpleSkiplist`] containing the entry
    /// is dropped or otherwise invalidated, aside from by being moved. (Neither
    /// [`SimpleSkiplist::lending_iter`] nor [`SimpleSkiplist::from_lending_iter`] invalidate the
    /// backing storage; they move the skiplist, but the backing storage remains at a stable
    /// address.)
    ///
    /// The returned entry references may be lifetime-extended, provided that the backing
    /// [`SimpleSkiplist`] or [`LendingIter`] is not invalidated in the ways described above for at
    /// least the length of the modified lifetime.
    ///
    /// In particular, these assurances apply to [`LendingIter::next`], [`LendingIter::current`],
    /// and [`LendingIter::prev`].
    #[derive(Debug)]
    pub struct LendingIter<Cmp: _>(#[List = SingleThreadedSkiplist<Cmp, SimpleState>] _);
}

impl<Cmp: Comparator> LendingIter<Cmp> {
    #[inline]
    #[must_use]
    fn new(list: SimpleSkiplist<Cmp>) -> Self {
        Self(SkiplistLendingIter::new(list.0))
    }

    #[inline]
    #[must_use]
    fn into_list(self) -> SimpleSkiplist<Cmp> {
        SimpleSkiplist(self.0.into_list())
    }
}
