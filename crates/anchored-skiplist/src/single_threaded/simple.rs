#![expect(
    unsafe_code,
    reason = "preserve invariance of inputs in a nominally covariant struct, \
              and assert that `Bump`s live longer than the lifetimes of provided references",
)]

use bumpalo::Bump;
use oorandom::Rand32;

use crate::{
    interface::{Comparator, Skiplist, SkiplistIterator, SkiplistLendingIterator},
    iter_defaults::{SkiplistIter, SkiplistLendingIter},
    node_heights::{MAX_HEIGHT, Prng32},
};
// `iter_defaults` needs to run tests on a list.
#[cfg(test)]
use crate::iter_defaults::SkiplistSeek;
use super::{
    list_inner::{SingleThreadedSkiplist, SkiplistState},
    node::{Link, Node},
};
// See below
use self::_lint_scope::SimpleArenaAndHead;


// ================================
//  Head
// ================================

#[derive(Default, Debug)]
struct SimpleHead<'bump>(pub [Link<'bump>; MAX_HEIGHT]);

impl SimpleHead<'_> {
    #[inline]
    #[must_use]
    const fn new() -> Self {
        Self([None; MAX_HEIGHT])
    }
}

// ================================
//  Self-referential Struct
// ================================

mod _lint_scope {
    #![expect(clippy::mem_forget, reason = "not my code, it's the macro triggering the lint")]

    use bumpalo::Bump;
    use self_cell::self_cell;

    use super::SimpleHead;


    self_cell! {
        pub(super) struct SimpleArenaAndHead {
            owner: Bump,

            #[covariant]
            dependent: SimpleHead,
        }

        impl {Debug}
    }
}

// ================================
//  State
// ================================

#[derive(Debug)]
struct SimpleState {
    arena_and_head: SimpleArenaAndHead,
    prng:           Rand32,
    current_height: usize,
}

impl Default for SimpleState {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Prng32 for SimpleState {
    #[inline]
    fn rand_u32(&mut self) -> u32 {
        self.prng.rand_u32()
    }
}

/// SAFETY:
/// We don't do something dumb like internal mutability for which `Bump` allocator is returned.
/// The same `Bump` allocator is returned every time, and we don't drop it early. And `self_cell`
/// ensures that the address of the `Bump` does not change, even when the `SimpleState` value
/// is moved.
///
/// The links stored in `self` which `head_skip` can return were initially constructed as `None`
/// and are only mutated by `set_head_skip`. Since the unsafe contract of `set_head_skip` is the
/// exact same, the second condition is met.
unsafe impl SkiplistState for SimpleState {
    #[inline]
    fn new_seeded(seed: u64) -> Self {
        Self {
            arena_and_head: SimpleArenaAndHead::new(Bump::new(), |_| SimpleHead::new()),
            prng:           Rand32::new(seed),
            current_height: 0,
        }
    }

    #[inline]
    fn bump(&self) -> &Bump {
        self.arena_and_head.borrow_owner()
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
        self.arena_and_head.borrow_dependent().0[level]
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
        /// This inner function is used to clearly define the `'q` lifetime, to more explicitly
        /// reason about it, since if I have to be overly-cautious anywhere, it might as well be
        /// while doing unsafe lifetime extension.
        ///
        /// # Safety
        /// Must be called inside `with_dependent_mut`'s callback with the provided `head` and the
        /// captured `link` (which was provided to `set_head_skip`), so that we get the safety
        /// guarantees required by `set_head_skip`.
        const unsafe fn set_head<'q>(head: &mut SimpleHead<'q>, level: usize, link: Link<'_>) {
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

            head.0[level] = link;
        }

        self.arena_and_head.with_dependent_mut(move |_, head| {
            // SAFETY:
            // This is being called inside `with_dependent_mut` in the described way.
            unsafe { set_head(head, level, link); }
        });
    }
}

// ================================
//  List
// ================================

/// A single-threaded skiplist which can only be accessed through a single handle.
#[derive(Default, Debug)]
pub struct SimpleSkiplist<Cmp>(SingleThreadedSkiplist<Cmp, SimpleState>);

impl<Cmp> SimpleSkiplist<Cmp> {
    #[inline]
    pub fn new(cmp: Cmp) -> Self {
        Self(SingleThreadedSkiplist::new(cmp))
    }

    #[inline]
    pub fn new_seeded(cmp: Cmp, seed: u64) -> Self {
        Self(SingleThreadedSkiplist::new_seeded(cmp, seed))
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

impl<Cmp: Comparator> Skiplist<Cmp> for SimpleSkiplist<Cmp> {
    /// Since this skiplist is single-threaded, there are no write locks that `Self::WriteLocked`
    /// would need to hold.
    type WriteLocked = Self;
    type Iter<'a>    = Iter<'a, Cmp> where Self: 'a;
    type LendingIter = LendingIter<Cmp>;

    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `init_entry`.
    ///
    /// Even if the entry compares equal to something already in the skiplist, it is added.
    // Note: `SimpleSkiplist` is not cloneable, so sound Rust is incapable of mutably accessing
    // this `SimpleSkiplist` inside `init_entry`. Therefore, there's no risk of panicking, aside
    // from allocation failures, or `init_entry` panicking.
    fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F) {
        self.0.insert_with(entry_len, init_entry);
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
//  Iter
// ================================

#[derive(Debug, Clone)]
pub struct Iter<'a, Cmp: Comparator>(
    SkiplistIter<'a, SingleThreadedSkiplist<Cmp, SimpleState>>,
);

impl<'a, Cmp: Comparator> Iter<'a, Cmp> {
    #[inline]
    #[must_use]
    const fn new(list: &'a SimpleSkiplist<Cmp>) -> Self {
        Self(SkiplistIter::new(&list.0))
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
pub struct LendingIter<Cmp: Comparator>(
    SkiplistLendingIter<SingleThreadedSkiplist<Cmp, SimpleState>>,
);

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

impl<Cmp: Comparator> SkiplistLendingIterator for LendingIter<Cmp> {
    #[inline]
    fn is_valid(&self) -> bool {
        self.0.is_valid()
    }

    #[inline]
    fn reset(&mut self) {
        self.0.reset();
    }

    #[inline]
    fn next(&mut self) -> Option<&[u8]> {
        self.0.next()
    }

    #[inline]
    fn current(&self) -> Option<&[u8]> {
        self.0.current()
    }

    fn prev(&mut self) -> Option<&[u8]> {
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
