#![expect(
    unsafe_code,
    reason = "preserve invariance of inputs in a nominally covariant struct, \
              and assert that `Bump`s live longer than the lifetimes of provided references",
)]

use std::{marker::PhantomPinned, pin::Pin, rc::Rc};
use std::cell::{Cell, RefCell};

use bumpalo::Bump;
use clone_behavior::{AnySpeed, IndependentClone, MirroredClone, MixedClone, Speed};
use oorandom::Rand32;

use crate::{skiplistiter_wrapper, skiplistlendingiter_wrapper};
use crate::{
    interface::{Comparator, Skiplist, SkiplistIterator, SkiplistLendingIterator},
    iter_defaults::{SkiplistIter, SkiplistLendingIter},
    node_heights::{MAX_HEIGHT, Prng32},
};
use super::{
    list_inner::{SingleThreadedSkiplist, SkiplistState},
    node::{ErasedLink, Link},
};


// ================================
//  State
// ================================

/// Vital invariants:
///
/// Before allocating *anything* with this struct's arena `Bump`, this struct must be moved into
/// the pinned `Rc` of `ConcurrentState`.
///
/// After the construction of `ConcurrentState`, the contents of `InnerConcurrentState` must
/// never be dropped, moved, or otherwise invalidated by `ConcurrentState`, up until the
/// `ConcurrentState` is dropped or otherwise invalidated, aside from by being moved.
///
/// Note that `Rc`s (pinned or not) have stable deref addresses.
///
/// This struct is self-referential, via `ErasedLink`s.
#[derive(Debug)]
struct InnerConcurrentState {
    /// Invariant: all inserted nodes must have been allocated with `self.arena`.
    /// (If solely [`ConcurrentState::set_head_skip`] is used, this invariant is upheld.)
    /// This field should be the first for drop order, just in case.
    head:               [Cell<ErasedLink>; MAX_HEIGHT],
    /// Vital invariant: nothing may be allocated in this bump until the `InnerConcurrentState`
    /// is moved into a `ConcurrentState`.
    arena:              Bump,
    prng:               RefCell<Rand32>,
    current_height:     Cell<usize>,
    _address_sensitive: PhantomPinned,
}

impl InnerConcurrentState {
    #[inline]
    fn from_prng(prng: Rand32) -> Self {
        Self {
            head:               Default::default(),
            arena:              Bump::new(),
            prng:               RefCell::new(prng),
            current_height:     Cell::new(0),
            _address_sensitive: PhantomPinned,
        }
    }
}

#[derive(Debug, Clone)]
struct ConcurrentState(Pin<Rc<InnerConcurrentState>>);

impl<S: Speed> MirroredClone<S> for ConcurrentState {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        self.clone()
    }
}

impl Prng32 for ConcurrentState {
    #[inline]
    fn rand_u32(&mut self) -> u32 {
        // This and `current_seed` are the only places where we borrow the prng, and the borrow
        // doesn't persist beyond either function, so `borrow_mut()` cannot panic.
        self.0.prng.borrow_mut().rand_u32()
    }
}

// SAFETY:
// We don't do something dumb like internal mutability for which `Bump` allocator is returned.
// The same `Bump` allocator is returned every time, and we don't drop it early. The pinned `Rc`
// does not move its inner contents, even when the `Rc` is cloned or moved. Therefore, all the
// reference-counted clones refer to the same `Bump` address.
//
// The links stored in `self` which `head_skip` can return were initially constructed as `None`
// and are only mutated by `set_head_skip`. Since the unsafe contract of `set_head_skip` is the
// exact same, the second condition is met.
unsafe impl SkiplistState for ConcurrentState {
    #[inline]
    fn new_seeded(seed: u64) -> Self {
        Self(Rc::pin(InnerConcurrentState::from_prng(Rand32::new(seed))))
    }

    #[inline]
    fn new_from_state(prng_state: (u64, u64)) -> Self {
        Self(Rc::pin(InnerConcurrentState::from_prng(Rand32::from_state(prng_state))))
    }

    fn current_prng_state(&self) -> (u64, u64) {
        // This and `Prng32::rand_u32` are the only places where we borrow the prng, and the borrow
        // doesn't persist beyond either function, so `borrow()` cannot panic.
        self.0.prng.borrow().state()
    }

    #[inline]
    fn bump(&self) -> &Bump {
        &self.0.arena
    }

    #[inline]
    fn current_height(&self) -> usize {
        self.0.current_height.get()
    }

    /// # Panics
    /// May or may not panic if `current_height` is greater than [`MAX_HEIGHT`].
    #[inline]
    fn set_current_height(&mut self, current_height: usize) {
        debug_assert!(
            current_height <= MAX_HEIGHT,
            "crate should not attempt to generate a height more than `MAX_HEIGHT`",
        );

        self.0.current_height.set(current_height);
    }

    /// If the returned [`Link`] references a [`Node`], then that node was allocated in
    /// the [`self.bump()`] bump allocator.
    ///
    /// # Panics
    /// Panics if `level` is greater than or equal to [`MAX_HEIGHT`].
    ///
    /// [`self.bump()`]: SkiplistState::bump
    #[inline]
    fn head_skip<'bump>(&'bump self, level: usize) -> Link<'bump> {
        let links = &self.0.head;

        #[expect(clippy::indexing_slicing, reason = "Max index is statically known")]
        let erased = links[level].get();

        // SAFETY:
        // `self` (and therefore also `self.0` and the `arena` `Bump` within)
        // live for `'bump`. Any node referenced by `erased` was put into this struct with
        // `set_head_skip`, and should thus have been allocated in `self.bump()`.
        // `self.bump()` was not dropped or otherwise invalidated (including not being moved,
        // thanks to the stability of `Rc`s), since `self` still exists, and that will continue to
        // be true for `'bump`. Thus, setting the lifetime to `'bump` doesn't break the safety
        // contract.
        unsafe { erased.into_link::<'bump>() }
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
        self.0.head[level].set(ErasedLink::from_link(link));
    }
}

// ================================
//  List
// ================================

/// A single-threaded skiplist which supports concurrency (though not parallelism) through
/// reference-counted cloning.
///
/// The [`Skiplist`] trait must be imported to use the list effectively.
#[derive(Debug)]
pub struct ConcurrentSkiplist<Cmp>(SingleThreadedSkiplist<Cmp, ConcurrentState>);

impl<Cmp: MirroredClone<AnySpeed>> ConcurrentSkiplist<Cmp> {
    /// Get another reference-counted handle to the same skiplist.
    #[inline]
    #[must_use]
    pub fn refcounted_clone(&self) -> Self {
        self.mirrored_clone()
    }
}

impl<Cmp: Comparator + IndependentClone<AnySpeed>> ConcurrentSkiplist<Cmp> {
    /// Copy the contents of this skiplist into a new, independent skiplist.
    #[inline]
    #[must_use]
    pub fn deep_clone(&self) -> Self {
        self.independent_clone()
    }
}

impl<S: Speed, Cmp: MirroredClone<S>> MirroredClone<S> for ConcurrentSkiplist<Cmp> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}

impl<Cmp: Comparator + IndependentClone<AnySpeed>> IndependentClone<AnySpeed>
for ConcurrentSkiplist<Cmp>
{
    #[inline]
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}

impl<Cmp: Comparator + Default> Default for ConcurrentSkiplist<Cmp> {
    #[inline]
    fn default() -> Self {
        Self::new(Cmp::default())
    }
}

#[expect(clippy::into_iter_without_iter, reason = ".iter() is provided by Skiplist trait")]
impl<'a, Cmp: Comparator> IntoIterator for &'a ConcurrentSkiplist<Cmp> {
    type IntoIter = Iter<'a, Cmp>;
    type Item     = &'a [u8];

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<Cmp: Comparator> Skiplist<Cmp> for ConcurrentSkiplist<Cmp> {
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
    ///
    /// Additionally, `init_entry` could insert something into the skiplist (and, if so,
    /// that insertion would complete before this call to `insert_with` would insert the entry),
    /// though doing so is not a good idea.
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
    /// The returned entry references remain valid until every [`ConcurrentSkiplist`] containing the
    /// entry is dropped or otherwise invalidated, aside from by being moved. (Neither
    /// [`ConcurrentSkiplist::lending_iter`] nor [`ConcurrentSkiplist::from_lending_iter`]
    /// invalidate the backing storage; they move the skiplist, but the backing storage remains at
    /// a stable address.)
    ///
    /// The returned entry references may be lifetime-extended, provided that at, for at least the
    /// length of the modified lifetime, at least one of the reference-counted clones of the backing
    /// [`ConcurrentSkiplist`] (possibly inside a [`LendingIter`], and possibly trading off which
    /// clone is valid without any one clone being valid the whole time) is not invalidated in the
    /// ways described above.
    ///
    /// In particular, these assurances apply to [`Iterator`] methods, [`Iter::current`], and
    /// [`Iter::prev`].
    ///
    /// Extending the lifetime of the `Iter` itself is *not* covered by the above guarantees, and
    /// may be unsound.
    #[derive(Debug)]
    pub struct Iter<'_, Cmp: _>(#[List = SingleThreadedSkiplist<Cmp, ConcurrentState>] _);
}

impl<'a, Cmp: Comparator> Iter<'a, Cmp> {
    #[inline]
    #[must_use]
    const fn new(list: &'a ConcurrentSkiplist<Cmp>) -> Self {
        Self(SkiplistIter::new(&list.0))
    }
}

impl<Cmp: Comparator> Clone for Iter<'_, Cmp> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S: Speed, Cmp: Comparator> MixedClone<S> for Iter<'_, Cmp> {
    #[inline]
    fn mixed_clone(&self) -> Self {
        self.clone()
    }
}

skiplistlendingiter_wrapper! {
    /// # Safety of lifetime extension
    /// The returned entry references remain valid until every [`ConcurrentSkiplist`] containing the
    /// entry is dropped or otherwise invalidated, aside from by being moved. (Neither
    /// [`ConcurrentSkiplist::lending_iter`] nor [`ConcurrentSkiplist::from_lending_iter`]
    /// invalidate the backing storage; they move the skiplist, but the backing storage remains at
    /// a stable address.)
    ///
    /// The returned entry references may be lifetime-extended, provided that at, for at least the
    /// length of the modified lifetime, at least one of the reference-counted clones of the backing
    /// [`ConcurrentSkiplist`] (possibly inside a [`LendingIter`], and possibly trading off which
    /// clone is valid without any one clone being valid the whole time) is not invalidated in the
    /// ways described above.
    ///
    /// In particular, these assurances apply to [`LendingIter::next`], [`LendingIter::current`],
    /// and [`LendingIter::prev`].
    #[derive(Debug, Clone)]
    pub struct LendingIter<Cmp: _>(
        #[List = SingleThreadedSkiplist<Cmp, ConcurrentState>] _,
    );
}

impl<Cmp: Comparator> LendingIter<Cmp> {
    #[inline]
    #[must_use]
    fn new(list: ConcurrentSkiplist<Cmp>) -> Self {
        Self(SkiplistLendingIter::new(list.0))
    }

    #[inline]
    #[must_use]
    fn into_list(self) -> ConcurrentSkiplist<Cmp> {
        ConcurrentSkiplist(self.0.into_list())
    }
}

impl<S: Speed, Cmp: Comparator + MirroredClone<S>> MixedClone<S> for LendingIter<Cmp> {
    #[inline]
    fn mixed_clone(&self) -> Self {
        Self(self.0.mixed_clone())
    }
}

impl<Cmp: Comparator + IndependentClone<AnySpeed>> IndependentClone<AnySpeed> for LendingIter<Cmp> {
    #[inline]
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}
