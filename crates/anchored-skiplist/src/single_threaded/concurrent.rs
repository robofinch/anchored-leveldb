#![expect(
    unsafe_code,
    reason = "preserve invariance of inputs in a nominally covariant struct, \
              and assert that `Bump`s live longer than the lifetimes of provided references",
)]

use std::marker::PhantomData;
use std::rc::Rc;
use std::cell::{Cell, RefCell};

use bumpalo::Bump;
use oorandom::Rand32;

use crate::{
    interface::{Comparator, Skiplist, SkiplistIterator, SkiplistLendingIterator},
    iter_defaults::{SkiplistIter, SkiplistLendingIter},
    node_heights::{MAX_HEIGHT, Prng32},
};
use super::{
    list_inner::{SingleThreadedSkiplist, SkiplistState},
    node::{ErasedLink, Link, Node},
};
// See below
use self::_lint_scope::ConcurrentArenaAndHead;


// ================================
//  Head
// ================================

#[derive(Default, Debug)]
struct ConcurrentHead<'bump>(pub [Cell<ErasedLink>; MAX_HEIGHT], pub PhantomData<&'bump ()>);

impl ConcurrentHead<'_> {
    #[inline]
    #[must_use]
    fn new() -> Self {
        Self::default()
    }
}

// ================================
//  Self-referential Struct
// ================================

mod _lint_scope {
    #![expect(clippy::mem_forget, reason = "not my code, it's the macro triggering the lint")]

    use bumpalo::Bump;
    use self_cell::self_cell;

    use super::ConcurrentHead;


    self_cell! {
        pub(super) struct ConcurrentArenaAndHead {
            owner: Bump,

            #[covariant]
            dependent: ConcurrentHead,
        }

        impl {Debug}
    }
}

// ================================
//  State
// ================================

#[derive(Debug)]
struct InnerConcurrentState {
    arena_and_head: ConcurrentArenaAndHead,
    prng:           RefCell<Rand32>,
    current_height: Cell<usize>,
}

impl InnerConcurrentState {
    #[inline]
    fn new_seeded(seed: u64) -> Self {
        Self {
            arena_and_head: ConcurrentArenaAndHead::new(Bump::new(), |_| ConcurrentHead::new()),
            prng:           RefCell::new(Rand32::new(seed)),
            current_height: Cell::new(0),
        }
    }
}

#[derive(Debug, Clone)]
struct ConcurrentState(Rc<InnerConcurrentState>);

impl Default for ConcurrentState {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Prng32 for ConcurrentState {
    #[inline]
    fn rand_u32(&mut self) -> u32 {
        self.0.prng.borrow_mut().rand_u32()
    }
}

/// SAFETY:
/// We don't do something dumb like internal mutability for which `Bump` allocator is returned.
/// The same `Bump` allocator is returned every time, and we don't drop it early. And `self_cell`
/// ensures that it is not accidentally moved.
///
/// The links stored in `self` which `head_skip` can return were initially constructed as `None`
/// and are only mutated by `set_head_skip`. Since the unsafe contract of `set_head_skip` is the
/// exact same, the second condition is met.
unsafe impl SkiplistState for ConcurrentState {
    #[inline]
    fn new_seeded(seed: u64) -> Self {
        Self(Rc::new(InnerConcurrentState::new_seeded(seed)))
    }

    #[inline]
    fn bump(&self) -> &Bump {
        self.0.arena_and_head.borrow_owner()
    }

    #[inline]
    fn current_height(&self) -> usize {
        self.0.current_height.get()
    }

    /// # Panics
    /// May or may not panic if `level` is greater than [`MAX_HEIGHT`].
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
    /// [`MAX_HEIGHT`]: super::MAX_HEIGHT
    #[inline]
    fn head_skip<'bump>(&'bump self, level: usize) -> Link<'bump> {
        let links = &self.0.arena_and_head.borrow_dependent().0;

        #[expect(clippy::indexing_slicing, reason = "Max index is statically known")]
        let erased = links[level].get();

        // SAFETY:
        // `self` (and therefore also `self.arena_and_head` and the `Bump` within)
        // live for `'bump`. Any node referenced by `erased` was put into this struct with
        // `set_head_skip`, and should thus have been allocated in `self.bump()`.
        // `self.bump()` was not dropped or otherwise invalidated (including not being moved,
        // thanks to `self_cell`), since `self` still exists, and that will continue to be true for
        // `'bump`. Thus, setting the lifetime to `'bump` doesn't break the safety contract.
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
    /// [`MAX_HEIGHT`]: super::MAX_HEIGHT
    unsafe fn set_head_skip(&mut self, level: usize, link: Link<'_>) {
        /// This inner function is used to clearly define the `'q` lifetime, to more explicitly
        /// reason about it, since if I have to be overly-cautious anywhere, it might as well be
        /// while doing unsafe lifetime extension.
        ///
        /// # Safety
        /// Must be called inside `with_dependent_mut`'s callback with the provided `head` and the
        /// captured `link` (which was provided to `set_head_skip`), so that we get the safety
        /// guarantees required by `set_head_skip`.
        unsafe fn set_head<'q>(head: &ConcurrentHead<'q>, level: usize, link: Link<'_>) {
            #![expect(clippy::indexing_slicing, reason = "Max index is statically known")]

            // SAFETY:
            // - If `link` refers to a node, it was allocated in the `self.bump()` bump allocator,
            //   which is the one stored in `self.arena_and_head`. That bump allocator has not
            //   previously been invalidated, since moving `self` doesn't cause `self.bump()` to
            //   be moved, and `self` hasn't been dropped or otherwise invalidated aside from being
            //   moved (since we still have access to `self` right now).
            //   Since the `head` obtained from `with_dependent_mut` can borrow from that `Bump`,
            //   and those borrows in `head` last for at least `'q`, we thus know that the `Bump`
            //   lives for at least `'q`, and had been valid from when `link`'s node was allocated
            //   up to now.
            let link = unsafe { Node::extend_link_lifetime::<'q>(link) };

            head.0[level].set(ErasedLink::from_link(link));
        }

        self.0.arena_and_head.with_dependent(move |_, head| {
            // SAFETY:
            // This is being called inside `with_dependent_mut` in the described way.
            unsafe { set_head(head, level, link); }
        });
    }
}

// ================================
//  List
// ================================

/// A single-threaded skiplist which supports concurrency (though not parallelism) through
/// reference-counted cloning.
#[derive(Default, Debug, Clone)]
pub struct ConcurrentSkiplist<Cmp> {
    list:      SingleThreadedSkiplist<Cmp, ConcurrentState>,
    /// Track whether anything is currently being inserted into the skiplist.
    /// There's no need to coordinate across multiple threads, but reentrancy would still be
    /// a problem, and could compromise the correctness (though not the memory-safety)
    /// of the skiplist implementation.
    inserting: Rc<Cell<bool>>,
}

impl<Cmp> ConcurrentSkiplist<Cmp> {
    #[inline]
    pub fn new(cmp: Cmp) -> Self {
        Self {
            list:      SingleThreadedSkiplist::new(cmp),
            inserting: Rc::new(Cell::new(false)),
        }
    }

    #[inline]
    pub fn new_seeded(cmp: Cmp, seed: u64) -> Self {
        Self {
            list:      SingleThreadedSkiplist::new_seeded(cmp, seed),
            inserting: Rc::new(Cell::new(false)),
        }
    }
}

impl<Cmp: Comparator> Skiplist<Cmp> for ConcurrentSkiplist<Cmp> {
    type Iter<'a> = Iter<'a, Cmp> where Self: 'a;
    type LendingIter = LendingIter<Cmp>;

    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `init_entry`.
    ///
    /// Even if the entry compares equal to something already in the skiplist, it is added.
    ///
    /// # Panics or Deadlocks
    /// Implementatations may panic or deadlock if the `init_entry` callback attempts to
    /// insert anything into this skiplist.
    fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F) {
        assert!(
            !self.inserting.get(),
            "The `init_entry` callback of `ConcurrentSkiplist::insert_with` called `insert_with` \
             on the same skiplist",
        );
        self.inserting.set(true);
        // If this line panics, then this skiplist would never again be able to have an element
        // inserted. And, in `list.insert_with`, we'd waste some of the memory of the bump
        // allocator. But there'd be no critically broken invariants, and no memory unsafety.
        self.list.insert_with(entry_len, init_entry);
        self.inserting.set(false);
    }

    fn contains(&self, entry: &[u8]) -> bool {
        self.list.contains(entry)
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
//  Iter
// ================================

#[derive(Debug, Clone)]
pub struct Iter<'a, Cmp: Comparator>(
    SkiplistIter<'a, SingleThreadedSkiplist<Cmp, ConcurrentState>>,
);

impl<'a, Cmp: Comparator> Iter<'a, Cmp> {
    #[inline]
    #[must_use]
    const fn new(list: &'a ConcurrentSkiplist<Cmp>) -> Self {
        Self(SkiplistIter::new(&list.list))
    }
}

impl<'a, Cmp: Comparator> Iterator for Iter<'a, Cmp> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<'a, Cmp: Comparator> SkiplistIterator<'a> for Iter<'a, Cmp> {
    #[inline]
    fn is_valid(&self) -> bool {
        self.0.is_valid()
    }

    #[inline]
    fn reset(&mut self) {
        self.0.reset();
    }

    #[inline]
    fn current(&self) -> Option<&'a [u8]> {
        self.0.current()
    }

    fn prev(&mut self) -> Option<&'a [u8]> {
        self.0.prev()
    }

    fn seek(&mut self, min_bound: &[u8]) {
        self.0.seek(min_bound);
    }

    #[inline]
    fn seek_to_first(&mut self) {
        self.0.seek_to_first();
    }

    fn seek_to_end(&mut self) {
        self.0.seek_to_end();
    }
}

#[derive(Debug)]
pub struct LendingIter<Cmp: Comparator> {
    iter:      SkiplistLendingIter<SingleThreadedSkiplist<Cmp, ConcurrentState>>,
    inserting: Rc<Cell<bool>>,
}

impl<Cmp: Comparator> LendingIter<Cmp> {
    #[inline]
    #[must_use]
    fn new(list: ConcurrentSkiplist<Cmp>) -> Self {
        Self {
            iter:      SkiplistLendingIter::new(list.list),
            inserting: list.inserting,
        }
    }

    #[inline]
    #[must_use]
    fn into_list(self) -> ConcurrentSkiplist<Cmp> {
        ConcurrentSkiplist {
            list:      self.iter.into_list(),
            inserting: self.inserting,
        }
    }
}

impl<Cmp: Comparator> SkiplistLendingIterator for LendingIter<Cmp> {
    #[inline]
    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    #[inline]
    fn reset(&mut self) {
        self.iter.reset();
    }

    #[inline]
    fn next(&mut self) -> Option<&[u8]> {
        self.iter.next()
    }

    #[inline]
    fn current(&self) -> Option<&[u8]> {
        self.iter.current()
    }

    fn prev(&mut self) -> Option<&[u8]> {
        self.iter.prev()
    }

    fn seek(&mut self, min_bound: &[u8]) {
        self.iter.seek(min_bound);
    }

    #[inline]
    fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    fn seek_to_end(&mut self) {
        self.iter.seek_to_end();
    }
}
