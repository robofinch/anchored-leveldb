#![expect(
    unsafe_code,
    reason = "preserve invariance of inputs in a nominally covariant struct, \
              and assert that `Herd`s live longer than the lifetimes of provided references",
)]

use std::sync::{atomic::Ordering, PoisonError};

use bumpalo_herd::{Herd, Member};
use clone_behavior::{MirroredClone, Speed};
use oorandom::Rand32;
use seekable_iterator::Comparator;
use yoke::{Yoke, Yokeable};

use crate::{
    maybe_loom::{Arc, AtomicUsize, Mutex, MutexGuard},
    node_heights::{random_node_height, MAX_HEIGHT},
};
use super::list_inner;
use super::list_inner::ThreadedSkiplistState;
use super::atomic_node::{AtomicErasedLink, Link, Node};


// ================================
//  Inner state
// ================================

#[derive(Debug)]
struct InnerThreadsafeState {
    /// Invariant: any node in `head` should be allocated in the below `arena` field.
    head:              [AtomicErasedLink; MAX_HEIGHT],
    /// *Nothing* should be allocated in this `Herd` before this struct is moved into an `Arc`
    /// in `UnlockedThreadsafeState`.
    arena:             Herd,
    /// This lock does double-duty: we already needed to briefly access the prng during insertions,
    /// so this might as well be the "hold this lock throughout an insertion" lock.
    ///
    /// Additionally: the lock should not be acquired before this struct is moved into an `Arc`
    /// in `LockedThreadsafeState`.
    prng_write_lock:   Mutex<Rand32>,
    current_height:    AtomicUsize,
}

impl InnerThreadsafeState {
    #[inline]
    #[must_use]
    fn from_prng(prng: Rand32) -> Self {
        Self {
            head:              Default::default(),
            arena:             Herd::new(),
            prng_write_lock:   Mutex::new(prng),
            current_height:    AtomicUsize::new(0),
        }
    }

    #[inline]
    fn acquire_prng_write_lock(&self) -> MutexGuard<'_, Rand32> {
        let maybe_poison: Result<_, PoisonError<_>> = self.prng_write_lock.lock();
        #[expect(clippy::unwrap_used, reason = "poison errors can only occur after/during a panic")]
        maybe_poison.unwrap()
    }
}

#[derive(Yokeable, Debug)]
struct UnlockedYokeable<'cart> {
    pub member: Member<'cart>,
}

impl InnerThreadsafeState {
    #[inline]
    #[must_use]
    fn yoke_unlocked(&self) -> UnlockedYokeable<'_> {
        UnlockedYokeable {
            member: self.arena.get(),
        }
    }
}

#[derive(Yokeable, Debug)]
struct LockedYokeable<'cart> {
    pub member: Member<'cart>,
    pub guard:  MutexGuard<'cart, Rand32>,
}

impl InnerThreadsafeState {
    #[inline]
    #[must_use]
    fn yoke_locked(&self) -> LockedYokeable<'_> {
        LockedYokeable {
            member: self.arena.get(),
            guard:  self.acquire_prng_write_lock(),
        }
    }
}


// ================================
//  Unlocked state
// ================================

#[derive(Debug)]
pub(super) struct UnlockedThreadsafeState {
    inner: Yoke<UnlockedYokeable<'static>, Arc<InnerThreadsafeState>>,
}

impl UnlockedThreadsafeState {
    #[inline]
    #[must_use]
    fn from_prng(prng: Rand32) -> Self {
        let cart = Arc::new(InnerThreadsafeState::from_prng(prng));
        Self {
            inner: Yoke::attach_to_cart(cart, |state| state.yoke_unlocked())
        }
    }
}

impl Clone for UnlockedThreadsafeState {
    #[inline]
    fn clone(&self) -> Self {
        let cart = Arc::clone(self.inner.backing_cart());
        Self {
            inner: Yoke::attach_to_cart(cart, |state| state.yoke_unlocked())
        }
    }
}

impl<S: Speed> MirroredClone<S> for UnlockedThreadsafeState {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        self.clone()
    }
}

// SAFETY:
// Each `UnlockedThreadsafeState` creates a `Herd` only through `InnerThreadsafeState::new_seeded`,
// which is called only on construction (and not when creating a `Clone`). The
// `LockedThreadsafeState` does not create a new `Herd`, either. Therefore, every reference-counted
// clone of `self` returns a `Member` of the same source `Herd` when the `member` method is called.
// Moreover, that `Herd` is stored in an `Arc` (which is never overwritten, up until `self` is
// dropped or invalidated other than by being moved), so it is not moved.
//
// For the second condition, because the safety contract of `store_head_skip` is not weakened
// in this module and it's the only source of nodes into the struct, the implementation of
// `load_head_skip` is valid.
//
// We implement `insert_with` with `random_node_height` and `inner_insert`.
unsafe impl ThreadedSkiplistState for UnlockedThreadsafeState {
    type WriteLockedState   = LockedThreadsafeState;

    #[inline]
    fn new_seeded(seed: u64) -> Self {
        Self::from_prng(Rand32::new(seed))
    }

    fn new_from_state(prng_state: (u64, u64)) -> Self {
        Self::from_prng(Rand32::from_state(prng_state))
    }

    fn current_prng_state(&self) -> (u64, u64) {
        self.inner.backing_cart().acquire_prng_write_lock().state()
    }

    #[inline]
    fn member(&self) -> &Member<'_> {
        &self.inner.get().member
    }

    fn insert_with<F, Cmp>(&mut self, cmp: &Cmp, entry_len: usize, init_entry: F) -> bool
    where
        F:   FnOnce(&mut [u8]),
        Cmp: Comparator<[u8]>,
    {
        let mut prng_write_guard = self.inner.backing_cart().acquire_prng_write_lock();
        let node_height = random_node_height(&mut *prng_write_guard);

        // This call could panic, due to the `init_entry` callback (or allocation failure). If it
        // were to panic, the mutex is unfortunately poisoned. Other than that, we've mutated the
        // prng and wasted some memory in the arena allocator. The poisoned mutex will cause any
        // other insertions into the skiplist to panic.
        // At least the skiplist isn't corrupted.
        let node = Node::new_node_with(self.member(), node_height, entry_len, init_entry);
        // SAFETY:
        // `self` and `self.inner` live for at least `'_`, so the data inside the `Arc` cart,
        // including the `Herd`, isn't dropped or otherwise invalidated (even by being moved) for
        // at least `'_` as well. Thus, extending `node`'s lifetime to `'_` is sound, since we just
        // allocated it into a `Herd` valid from its creation up to at least `'_`.
        let node = unsafe { node.extend_lifetime() };

        // Correctness: this is protected by the write lock.
        // SAFETY:
        // We just allocated `node` in `self`'s `Herd`.
        let successful = unsafe { list_inner::inner_insert(cmp, self, node, node_height) };

        // Explicitly ensure that the mutex guard isn't dropped until after the insertion ends.
        drop(prng_write_guard);

        successful
    }

    /// # Panics or Deadlocks
    /// May panic or deadlock if this thread has already called
    /// `get_random_node_height_and_write_lock()` or `write_locked()` on `self` or one of its
    /// reference-counted clones and has not yet dropped the returned `WriteLockGuard` or
    /// dropped or write-unlocked the returned `WriteLockedState`, respectively.
    fn write_locked(self) -> Self::WriteLockedState {
        let inner = self.inner.map_with_cart(|unlocked, cart| {
            LockedYokeable {
                member: unlocked.member,
                guard:  cart.acquire_prng_write_lock(),
            }
        });
        LockedThreadsafeState { inner }
    }

    fn write_unlocked(locked: Self::WriteLockedState) -> Self {
        let inner = locked.inner.map_project(|locked, _| {
            UnlockedYokeable {
                member: locked.member,
            }
        });
        Self { inner }
    }

    #[inline]
    fn load_current_height(&self, order: Ordering) -> usize {
        self.inner
            .backing_cart()
            .current_height
            .load(order)
    }

    /// # Panics
    /// Panics if `order` is [`Acquire`] or [`AcqRel`].
    ///
    /// May or may not panic if `current_height` is greater than [`MAX_HEIGHT`].
    ///
    /// [`Acquire`]: Ordering::Acquire
    /// [`AcqRel`]:  Ordering::AcqRel
    #[inline]
    fn store_current_height(&self, current_height: usize, order: Ordering) {
        self.inner
            .backing_cart()
            .current_height
            .store(current_height, order);
    }

    /// If the returned [`Link`] references a [`Node`], then that node was allocated in the same
    /// [`Herd`] which the [`Member`] returned by [`self.member()`] is a part of.
    ///
    /// # Panics
    /// Panics if `level` is greater than or equal to [`MAX_HEIGHT`].
    ///
    /// [`Herd`]: bumpalo_herd::Herd
    /// [`self.member()`]: ThreadedSkiplistState::member
    #[inline]
    fn load_head_skip(&self, level: usize, order: Ordering) -> Link<'_> {
        #[expect(clippy::indexing_slicing, reason = "max index is statically known")]
        let erased_link: &AtomicErasedLink = &self.inner
            .backing_cart()
            .head[level];

        // SAFETY:
        // `self` (and therefore also the `Herd` arena of `self.inner`) live for `'_`. Any node
        // referenced by `erased_link` was put into this struct by `store_head_skip`, and should
        // thus have been allocated in that same `Herd` arena. Since `self` does not drop, move,
        // or otherwise invalidate the `Herd` while it exists (up to when it is dropped or
        // invalidated other than by being moved), the `Herd` remains/remained valid for `'_`
        // starting from at least when the node was allocated; its lifetime can be set to `'_`.
        unsafe { erased_link.load_link(order) }
    }

    /// # Safety
    /// If the provided `link` is a `Some` value, referencing a `Node`, then that node must have
    /// been allocated in the same [`Herd`] allocator which the [`Member`] returned by
    /// [`self.member()`] is a part of.
    ///
    /// # Panics
    /// Panics if `level` is greater than or equal to [`MAX_HEIGHT`].
    ///
    /// [`Herd`]: bumpalo_herd::Herd
    /// [`self.member()`]: ThreadedSkiplistState::member
    #[inline]
    unsafe fn store_head_skip(&self, level: usize, link: Link<'_>, order: Ordering) {
        #[expect(clippy::indexing_slicing, reason = "max index is statically known")]
        let erased_link = &self.inner
            .backing_cart()
            .head[level];

        erased_link.store_link(link, order);
    }
}


// ================================
//  Locked state
// ================================

#[derive(Debug)]
pub(super) struct LockedThreadsafeState {
    inner: Yoke<LockedYokeable<'static>, Arc<InnerThreadsafeState>>,
}

impl LockedThreadsafeState {
    #[inline]
    #[must_use]
    fn from_prng(prng: Rand32) -> Self {
        let cart = Arc::new(InnerThreadsafeState::from_prng(prng));
        Self {
            inner: Yoke::attach_to_cart(cart, |state| state.yoke_locked())
        }
    }
}

// SAFETY:
// Each `UnlockedThreadsafeState` creates a `Herd` only through `InnerThreadsafeState::new_seeded`,
// which is called only on construction (and not when creating a `Clone`). The
// `LockedThreadsafeState` does not create a new `Herd`, either. Therefore, every reference-counted
// clone (including write-unlocked or write-locked versions) of `self` returns a `Member` of the
// same source `Herd` when the `member` method is called. Moreover, that `Herd` is stored in an
// `Arc` (which is never overwritten, up until `self` is dropped or invalidated other than by being
// moved), so it is not moved.
//
// For the second condition, because the safety contract of `store_head_skip` is not weakened
// in this module and it's the only source of nodes into the struct, the implementation of
// `load_head_skip` is valid.
//
// We implement `insert_with` with `random_node_height` and `inner_insert`.
unsafe impl ThreadedSkiplistState for LockedThreadsafeState {
    type WriteLockedState = Self;

    #[inline]
    fn new_seeded(seed: u64) -> Self {
        Self::from_prng(Rand32::new(seed))
    }

    fn new_from_state(prng_state: (u64, u64)) -> Self {
        Self::from_prng(Rand32::from_state(prng_state))
    }

    fn current_prng_state(&self) -> (u64, u64) {
        self.inner.get().guard.state()
    }

    #[inline]
    fn member(&self) -> &Member<'_> {
        &self.inner.get().member
    }

    fn insert_with<F, Cmp>(&mut self, cmp: &Cmp, entry_len: usize, init_entry: F) -> bool
    where
        F:   FnOnce(&mut [u8]),
        Cmp: Comparator<[u8]>,
    {
        let node_height = self.inner.with_mut_return(|inner| {
            random_node_height(&mut *inner.guard)
        });

        // This call could panic, due to the `init_entry` callback (or allocation failure). If it
        // were to panic, the mutex is unfortunately poisoned. Other than that, we've mutated the
        // prng and wasted some memory in the arena allocator. The poisoned mutex will cause any
        // other insertions into the skiplist to panic.
        // At least the skiplist isn't corrupted.
        let node = Node::new_node_with(self.member(), node_height, entry_len, init_entry);
        // SAFETY:
        // `self` lives for at least `'b`, so the invariant of `self.state` implies that
        // `self.state` isn't dropped or otherwise invalidated (in this case, even by being moved)
        // for at least `'b`, either, nor had it been since `self` was created. The unsafe contract
        // of `ThreadedSkiplistState` then implies that the `Herd` of `self.state` remains valid for
        // at least `'b` (and has been since the line above), which is the same `Herd` we just
        // allocated `node` into. Thus, extending `node`'s lifetime to `'b` is sound.
        let node = unsafe { node.extend_lifetime() };

        // Correctness:
        // `self` holds the write lock.
        // SAFETY:
        // We just allocated `node` in `self`'s `Herd`.
        unsafe { list_inner::inner_insert(cmp, self, node, node_height) }
    }

    /// As `Self` already holds the write lock, this is a no-op.
    #[inline]
    fn write_locked(self) -> Self::WriteLockedState {
        self
    }

    /// As `Self` also holds the write lock, this is a no-op.
    #[inline]
    fn write_unlocked(locked: Self::WriteLockedState) -> Self {
        locked
    }

    /// # Panics
    /// Panics if `order` is [`Release`] or [`AcqRel`].
    ///
    /// [`Release`]: Ordering::Release
    /// [`AcqRel`]:  Ordering::AcqRel
    #[inline]
    fn load_current_height(&self, order: Ordering) -> usize {
        self.inner
            .backing_cart()
            .current_height
            .load(order)
    }

    /// # Panics
    /// Panics if `order` is [`Acquire`] or [`AcqRel`].
    ///
    /// May or may not panic if `current_height` is greater than [`MAX_HEIGHT`].
    ///
    /// [`Acquire`]: Ordering::Acquire
    /// [`AcqRel`]:  Ordering::AcqRel
    #[inline]
    fn store_current_height(&self, current_height: usize, order: Ordering) {
        self.inner
            .backing_cart()
            .current_height
            .store(current_height, order);
    }

    /// If the returned [`Link`] references a [`Node`], then that node was allocated in the same
    /// [`Herd`] which the [`Member`] returned by [`self.member()`] is a part of.
    ///
    /// # Panics
    /// Panics if `level` is greater than or equal to [`MAX_HEIGHT`], or if `order` is
    /// [`Release`] or [`AcqRel`].
    ///
    /// [`Herd`]: bumpalo_herd::Herd
    /// [`self.member()`]: ThreadedSkiplistState::member
    /// [`Release`]: Ordering::Release
    /// [`AcqRel`]:  Ordering::AcqRel
    #[inline]
    fn load_head_skip(&self, level: usize, order: Ordering) -> Link<'_> {
        #[expect(clippy::indexing_slicing, reason = "max index is statically known")]
        let erased_link: &AtomicErasedLink = &self.inner
            .backing_cart()
            .head[level];

        // SAFETY:
        // `self` (and therefore also the `Herd` arena of `self.inner`) live for `'_`. Any node
        // referenced by `erased_link` was put into this struct by `store_head_skip`, and should
        // thus have been allocated in that same `Herd` arena. Since `self` does not drop, move,
        // or otherwise invalidate the `Herd` while it exists (up to when it is dropped or
        // invalidated other than by being moved), the `Herd` remains/remained valid for `'_`
        // starting from at least when the node was allocated; its lifetime can be set to `'_`.
        unsafe { erased_link.load_link(order) }
    }

    /// # Safety
    /// If the provided `link` is a `Some` value, referencing a `Node`, then that node must have
    /// been allocated in the same [`Herd`] allocator which the [`Member`] returned by
    /// [`self.member()`] is a part of.
    ///
    /// # Panics
    /// Panics if `level` is greater than or equal to [`MAX_HEIGHT`], or if `order` is
    /// [`Acquire`] or [`AcqRel`].
    ///
    /// [`Herd`]: bumpalo_herd::Herd
    /// [`self.member()`]: ThreadedSkiplistState::member
    /// [`Acquire`]: Ordering::Acquire
    /// [`AcqRel`]:  Ordering::AcqRel
    #[inline]
    unsafe fn store_head_skip(&self, level: usize, link: Link<'_>, order: Ordering) {
        #[expect(clippy::indexing_slicing, reason = "max index is statically known")]
        let erased_link = &self.inner
            .backing_cart()
            .head[level];

        erased_link.store_link(link, order);
    }
}
