#![expect(
    unsafe_code,
    reason = "preserve invariance of inputs in a nominally covariant struct, \
              and assert that `Herd`s live longer than the lifetimes of provided references",
)]

use std::{cmp::Ordering as CmpOrdering, sync::atomic::Ordering};

use bumpalo_herd::Member;

use crate::{interface::Comparator, iter_defaults::SkiplistSeek};
use crate::node_heights::MAX_HEIGHT;
use super::atomic_node::{Link, Node};


// TODO/FIXME: replace Ordering::SeqCst with whatever it's *supposed* to be at each point.


/// # Safety
/// The [`member`] method must return a reference to a [`Member`] of the same, unmoved [`Herd`]
/// every time it is called, and reference-counted clones of `self` (including write-locked
/// versions) must also return [`Member`]s of the same source [`Herd`].
///
/// (For performance reasons, a given refcounted clone should probably return the same [`Member`]
/// each time as well, but that is not critical.)
///
/// This requirement implies that [`Herd`] arena allocator must not be dropped, moved, or otherwise
/// invalidated until `self` and all associated reference-counted clones are dropped or otherwise
/// invalidated, aside from by being moved, as otherwise the [`member`] method could be called again
/// and could not return a [`Member`] of the same [`Herd`].
///
/// If the [`Link`] returned by [`load_head_skip`] refers to a node, then that node must have been
/// allocated in the `Herd` allocator of `self`, and thus be valid until the `Herd` allocator
/// is dropped, moved, or otherwise invalidated. If the unsafe contract of [`store_head_skip`] is
/// not weakened for `Self` or its `WriteLocked` or write-unlocked versions, and [`head_skip`]'s
/// only source of non-`None` links is [`store_head_skip`], then this condition is satisfied.
///
/// The implementation of `insert_with` should use [`crate::node_heights::random_node_height`]
/// and call [`inner_insert`] (and meet its constraints).
///
/// [`Herd`]: bumpalo_herd::Herd
/// [`member`]: ThreadedSkiplistState::member
/// [`load_head_skip`]: ThreadedSkiplistState::load_head_skip
/// [`store_head_skip`]: ThreadedSkiplistState::store_head_skip
pub(super) unsafe trait ThreadedSkiplistState: Sized {
    /// A version of `Self` which permanently holds the state's unique write lock (accessible
    /// by a `self` value and all of its reference-counted clones), up until it is dropped or
    /// converted back into a `Self` value.
    ///
    /// If `Self` is already a `WriteLockedState`, this type should be `Self`.
    type WriteLockedState: ThreadedSkiplistState;

    #[must_use]
    fn new_seeded(seed: u64) -> Self;

    #[must_use]
    fn member(&self) -> &Member<'_>;

    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `create_entry`.
    ///
    /// If the resulting entry compares equal to an entry already in the skiplist, the entry
    /// is discarded, and `false` is returned. Otherwise, `true` is returned. Attempting to add
    /// duplicate entries should be avoided, as the spent memory will not be reclaimed until the
    /// skiplist is dropped.
    ///
    /// # Panics or Deadlocks
    /// If `init_entry` attempts to call `insert_with` on a reference-counted clone of
    /// associated with `self` (including write-unlocked verions), a panic or deadlock will occur.
    ///
    /// Will also panic or deadlock if the current thread already holds a `WriteLockedState`
    /// associated with `self`, excluding `self`.
    fn insert_with<F, Cmp>(&mut self, cmp: &Cmp, entry_len: usize, init_entry: F) -> bool
    where
        F:   FnOnce(&mut [u8]),
        Cmp: Comparator;

    /// If `Self` is already a `WriteLockedState`, this should be a no-op.
    ///
    /// # Panics or Deadlocks
    /// Unless `Self` is a `WriteLockedState`, may panic or deadlock if the current thread has
    /// already called `write_locked()` on `self` or one of its reference-counted clones and has not
    /// yet dropped or write-unlocked the returned `WriteLockedState`.
    #[must_use]
    fn write_locked(self) -> Self::WriteLockedState;

    /// If `Self` is already a `WriteLockedState`, this should be a no-op.
    #[must_use]
    fn write_unlocked(locked: Self::WriteLockedState) -> Self;

    /// The returned value will always be less than or equal to [`MAX_HEIGHT`].
    #[must_use]
    fn load_current_height(&self, order: Ordering) -> usize;

    /// # Panics
    /// Panics if `order` is [`Acquire`] or [`AcqRel`].
    ///
    /// May or may not panic if `current_height` is greater than [`MAX_HEIGHT`].
    ///
    /// [`Acquire`]: Ordering::Acquire
    /// [`AcqRel`]:  Ordering::AcqRel
    fn store_current_height(&self, current_height: usize, order: Ordering);

    /// If the returned [`Link`] references a [`Node`], then that node was allocated in the same
    /// [`Herd`] which the [`Member`] returned by [`self.member()`] is a part of.
    ///
    /// # Panics
    /// Panics if `level` is greater than or equal to [`MAX_HEIGHT`].
    ///
    /// [`Herd`]: bumpalo_herd::Herd
    /// [`self.member()`]: ThreadedSkiplistState::member
    #[must_use]
    fn load_head_skip(&self, level: usize, order: Ordering) -> Link<'_>;

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
    unsafe fn store_head_skip(&self, level: usize, link: Link<'_>, order: Ordering);
}

// ================================================================================================
//  Functions that I couldn't manage to fit into `MultithreadedSkiplist`
//  (`UnlockedSkiplistState` and `LockedSkiplistState` are slightly too different.)
// ================================================================================================

// Short utility functions

/// Return `Some(node)` if the provided `link` sorts strictly less than the provided `entry`.
/// Since `None` links are considered to sort after every entry, in such a scenario, the link
/// is guaranteed to have a `node` in it.
fn node_before_entry<'b, Cmp: Comparator>(
    cmp:   &Cmp,
    link:  Link<'b>,
    entry: &[u8],
) -> Option<&'b Node<'b>> {
    let node = link?;

    if cmp.cmp(node.entry(), entry) == CmpOrdering::Less {
        Some(node)
    } else {
        None
    }
}

/// Determines whether the entries of the two provided nodes compare as equal.
#[inline]
fn nodes_equal<Cmp: Comparator>(cmp: &Cmp, lhs: &Node<'_>, rhs: &Node<'_>) -> bool {
    cmp.cmp(lhs.entry(), rhs.entry()) == CmpOrdering::Equal
}

/// Any nodes referenced by the returned `Link`s were allocated in the `Herd` of `state`.
fn find_preceding_neighbors<'s, Cmp: Comparator, State: ThreadedSkiplistState>(
    cmp:   &Cmp,
    state: &'s State,
    entry: &[u8],
) -> [Link<'s>; MAX_HEIGHT]
{
    let mut prev = [None; MAX_HEIGHT];

    // Return if the current height is `0` (since nothing's in the list in that case).
    let current_height = state.load_current_height(Ordering::SeqCst);
    let Some(mut level) = current_height.checked_sub(1) else {
        return prev;
    };

    let link_from_head = loop {
        // If `Some`, this was allocated in the `Herd` of `state` by the contract of
        // `ThreadedSkiplistState`.
        let next = state.load_head_skip(level, Ordering::SeqCst);

        if let Some(node) = node_before_entry(cmp, next, entry) {
            // We should search further ahead since `next` was too small.
            // (So, don't decrement `level`. But break out and stop searching from the head.)
            break node;
        } else if level == 0 {
            // We would set `prev[level]` to `None`, but it already is.
            return prev;
        } else {
            // This level might have looked too far ahead. Drop down to a lower level.
            // We would set `prev[level]` to `None`, but it already is.
            level -= 1;
        }
    };

    // This was allocated in the `Herd` of `state` above, and may be set to a node
    // in `next` below which was also allocated in the `Herd` of `state`
    let mut current = link_from_head;

    loop {
        // By the invariants of `Node`, this was also allocated in the `Herd` of `state`,
        // since `current` was.
        let next = current.load_skip(level, Ordering::SeqCst);

        if let Some(node) = node_before_entry(cmp, next, entry) {
            // We should search further ahead since `next` was too small.
            // (So, don't decrement `level`.)
            current = node;
        } else {
            #[expect(clippy::indexing_slicing, reason = "0 <= level < MAX_HEIGHT")]
            #[expect(clippy::semicolon_outside_block, reason = "block needed for lint scope")]
            {
                // This is the only part of the function where we write to `prev`.
                // We know that `current` was allocated in the `Herd` of `state`
                // so this function's assertion holds.
                prev[level] = Some(current);
            }

            if level == 0 {
                return prev;
            } else {
                // This level might have looked too far ahead. Drop down to a lower level.
                level -= 1;
            }
        }
    }
}

/// This inner function for implementing `insert_with` is not generic over the callback `F`, so
/// that monomorphization won't necessarily make a bunch of duplicate copies of it.
///
/// # Logical correctness
/// This function must be protected by a write lock which ensures that no reference-counted clone
/// of `state`, including write-locked or write-unlocked versions, can concurrently call
/// `insert_with`.
///
/// # Safety
/// `node` must have been allocated in the `Herd` of `state`. The contract of
/// `ThreadedSkiplistState` applies to `state`, so it follows that the `Herd` of `state` has
/// remained valid since `node` was allocated, and that the `Herd` will remain valid for at least
/// `'herd` (as the lifetime of `State`).
pub(super) unsafe fn inner_insert<'herd, Cmp: Comparator, State: ThreadedSkiplistState>(
    cmp:         &'herd Cmp,
    state:       &'herd State,
    node:        &'herd Node<'herd>,
    node_height: usize,
) -> bool {
    let prev = find_preceding_neighbors(cmp, state, node.entry());

    // Check whether `node` is already in the skiplist.
    // `prev[0]` should be the greatest node which is strictly less than `node`,
    // so if the next node after `prev[0]` is not equal to `node`, then `node` is
    // unique in the skiplist.
    let next_node = if let Some(prev_node) = prev[0] {
        prev_node.load_skip(0, Ordering::SeqCst)
    } else {
        state.load_head_skip(0, Ordering::SeqCst)
    };

    if next_node.is_some_and(|next_node| nodes_equal(cmp, next_node, node)) {
        // Ah, well. It was a duplicate. We lose access to the memory allocated to `node`
        // until this skiplist is dropped.
        return false;
    }

    let prev = prev.map(|link| {
        // SAFETY:
        // `state` lives for at least `'herd`, so it isn't dropped or otherwise invalidated
        // (though being moved would be fine) for `'herd`. By the safety contract of
        // `ThreadedSkiplistState`, the `Herd` of `state` is valid for `'herd` as well.
        // Since any nodes returned by `this.find_preceding_neighbors` were allocated in
        // that `Herd`, including any node referenced by `link`, the precondition
        // of `Node::extend_link_lifetime` is met.
        unsafe { Node::extend_link_lifetime::<'herd>(link) }
    });

    // Only increase the current height after we're sure that we're inserting something.
    // NOTE: Ordinarily, something like the CAS-based `fetch_max` should be used.
    // However, since the sole call to `store_current_height` is here, and it's protected
    // by the write lock, separate loads and stores are fine.
    if node_height > state.load_current_height(Ordering::SeqCst) {
        state.store_current_height(node_height, Ordering::SeqCst);
    }

    for (level, prev_link) in prev.into_iter().take(node_height).enumerate() {
        if let Some(preceding_neighbor) = prev_link {
            // On level `level`, put `node` between `preceding_neighbor` and
            // `preceding_neighbor`'s skip on the level.

            let next = preceding_neighbor.load_skip(level, Ordering::SeqCst);
            // SAFETY:
            // As discussed above, any node in `prev`, like `preceding_neighbor`, was
            // allocated in the `Herd` of `state`. By the safety contract, `node` was also
            // allocated in that `Herd`, and by the invariants of `Node`, if `next` refers to a
            // node, that noed was allocated in the same `Herd` as `preceding_neighbor`, too.
            // Thus, `next` and `node` were both allocated in the same `Herd`.
            unsafe { node.store_skip(level, next, Ordering::SeqCst) }
            // SAFETY:
            // As stated above, `node` and `preceding_neighbor` were allocated in the
            // same `Herd` allocator.
            unsafe { preceding_neighbor.store_skip(level, Some(node), Ordering::SeqCst) }
        } else {
            // `node` is sorted as the first node on this level;
            // put `node` before the previously-first node on this level (if there was one).

            let next = state.load_head_skip(level, Ordering::SeqCst);
            // SAFETY:
            // By the safety contract of `inner_insert`, `node` was allocated in the `Herd` of
            // `state`. By the contract of `ThreadedSkiplistState`, we have that any node referenced
            // by the link returned from `this.state.load_head_skip(_)` was allocated in that
            // `Herd`, too. Therefore, `next` and `node` were allocated in the same `Herd`.
            unsafe { node.store_skip(level, next, Ordering::SeqCst) }
            // SAFETY:
            // `node` was allocated in the `Herd` of state.
            unsafe { state.store_head_skip(level, Some(node), Ordering::SeqCst); }
        }
    }

    true
}

/// Struct that implements most of the logic of the two multithreading-capable skiplists provided by
/// this crate.
#[derive(Debug, Clone)]
pub(super) struct MultithreadedSkiplist<Cmp, State> {
    cmp:   Cmp,
    /// Invariants:
    ///
    /// - `state` must not be dropped or otherwise invalidated, except by being moved,
    ///   until `self` is being dropped or otherwise invalidated, except by being moved.
    ///   (Basically, just do not assign anything to this field after `self`'s construction, only
    ///   call methods on it.)
    state: State,
}

// Initialization
#[expect(unreachable_pub, reason = "control visibility from one site: the type definition")]
impl<Cmp, State: ThreadedSkiplistState> MultithreadedSkiplist<Cmp, State> {
    #[inline]
    #[must_use]
    pub fn new_seeded(cmp: Cmp, seed: u64) -> Self {
        Self {
            cmp,
            state: State::new_seeded(seed),
        }
    }
}

// Longer utility functions, related to searching through the skiplist.
impl<Cmp: Comparator, State: ThreadedSkiplistState> MultithreadedSkiplist<Cmp, State> {
    /// Any node referenced by the returned `Link` was allocated in the `Herd` of `self.state`.
    fn find_le_or_geq<const GEQ: bool>(&self, entry: &[u8]) -> Link<'_> {
        // Justification for the assertion made above: `self.state.load_head_skip(_)` and
        // `Node::load_skip` are our only sources of node references or links in the function. Any
        // node from the former was allocated in `Herd` of `self.state`. Any node obtained from the
        // latter was allocated in the same arena allocator as the input node. Or, see
        // `find_preceding_neighbors` for a more methodical justification.

        // Return `None` if the current height is `0` (since nothing's in the list in that case).
        let current_height = self.state.load_current_height(Ordering::SeqCst);
        let mut level = current_height.checked_sub(1)?;

        let link_from_head = loop {
            let next = self.state.load_head_skip(level, Ordering::SeqCst);

            if let Some(node) = node_before_entry(&self.cmp, next, entry) {
                // We should search further ahead since `next` was too small.
                // (So, don't decrement `level`. But break out and stop searching from the head.)
                break node;
            } else if level == 0 {
                // The lowest-level link from the `head`, which is the first element in the list,
                // is greater than or equal to the `entry`; we're done. `next` is `geq`, and nothing
                // is `le`.
                return if GEQ { next } else { None };
            } else {
                // This level might have looked too far ahead. Drop down to a lower level.
                level -= 1;
            }
        };

        let mut current = link_from_head;

        loop {
            let next = current.load_skip(level, Ordering::SeqCst);

            if let Some(node) = node_before_entry(&self.cmp, next, entry) {
                // We should search further ahead since `next` was too small.
                // (So, don't decrement `level`.)
                current = node;
            } else if level == 0 {
                // We've narrowed it down to here, we're done. `next` is `geq`, and the one
                // before it (`current`) is `le`.
                return if GEQ { next } else { Some(current) };
            } else {
                // This level might have looked too far ahead. Drop down to a lower level.
                level -= 1;
            }
        }
    }
}

// The rest is practically a `Skiplist` implementation, aside from lacking iterators.
impl<Cmp: Comparator, State: ThreadedSkiplistState> MultithreadedSkiplist<Cmp, State> {
    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `create_entry`.
    ///
    /// If the resulting entry compares equal to an entry already in the skiplist, the entry
    /// is discarded, and `false` is returned. Otherwise, `true` is returned. Attempting to add
    /// duplicate entries should be avoided, as the spent memory will not be reclaimed until the
    /// skiplist is dropped.
    ///
    /// # Panics or Deadlocks
    /// If `init_entry` attempts to call `insert_with` on a reference-counted clone of
    /// associated with `self` (including write-unlocked verions), a panic or deadlock will occur.
    ///
    /// Will also panic or deadlock if the current thread already holds a `WriteLockedState`
    /// associated with `self.state`, excluding `self.state`.
    pub fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F) -> bool {
        self.state.insert_with(&self.cmp, entry_len, init_entry)
    }

    /// Check whether the entry, or something which compares as equal to the entry, is in
    /// the skiplist.
    pub fn contains(&self, entry: &[u8]) -> bool {
        self.find_greater_or_equal(entry)
            .is_some_and(|node| self.cmp.cmp(node.entry(), entry) == CmpOrdering::Equal)
    }

    /// If `State` is already a `WriteLockedState`, this should be a no-op.
    ///
    /// # Panics or Deadlocks
    /// Unless `State` is a `WriteLockedState`, may panic or deadlock if the current thread has
    /// already called `write_locked()` on `self` or one of its reference-counted clones and has not
    /// yet dropped or write-unlocked the returned `WriteLockedState`.
    #[inline]
    #[must_use]
    pub fn write_locked(self) -> MultithreadedSkiplist<Cmp, State::WriteLockedState> {
        MultithreadedSkiplist {
            cmp:   self.cmp,
            state: self.state.write_locked(),
        }
    }

    /// If `State` is already a `WriteLockedState`, this should be a no-op.
    #[inline]
    #[must_use]
    pub fn write_unlocked(locked: MultithreadedSkiplist<Cmp, State::WriteLockedState>) -> Self {
        Self {
            cmp:   locked.cmp,
            state: State::write_unlocked(locked.state),
        }
    }
}

// SAFETY:
// Each of the below four functions justifies why the returned reference may be soundly
// lifetime-extended, provided that, for at least the length of the new lifetime, the source
// `self: MultithreadedSkiplist<_,_>` value (or, at least one of the reference-counted clones
// associated with it) is not dropped or invalidated in some way other than by moving that `Self`
// value (or its clones).
// As discussed by `SkiplistSeek`, a sound implementation of `SkiplistNode` for `Node`
// implies the last requirement for this implementation to uphold the unsafe contract.
unsafe impl<Cmp: Comparator, State: ThreadedSkiplistState> SkiplistSeek
for MultithreadedSkiplist<Cmp, State>
{
    type Node<'a> = Node<'a> where Self: 'a;

    /// Return the first node in the skiplist, if the skiplist is nonempty.
    ///
    /// This operation is fast.
    #[inline]
    fn get_first(&self) -> Link<'_> {
        // SAFETY of implementation:
        // Any node referenced by `self.state.load_head_skip(_)` was allocated in the `Herd`
        // of `self.state`, which is not invalidated until the `self: Self` and any associated
        // reference-counted clones are dropped or invalidated in some way other than moving the
        // `self: Self` or clones.

        // The very first link on the lowest level leads to the first node.
        self.state.load_head_skip(0, Ordering::SeqCst)
    }

    /// Return the last node in the skiplist, if the skiplist is nonempty.
    fn find_last(&self) -> Link<'_> {
        // SAFETY of implementation:
        // This is basically the same as `find_strictly_less`, except the entry is
        // the `None` link, and thus any non-`None` node comes before that phantom entry.
        // So the assertion about lifetime extension holds for the same reason as `find_le_or_geq`.

        // Return `None` if the current height is `0` (since nothing's in the list in that case).
        let current_height = self.state.load_current_height(Ordering::SeqCst);
        let mut level = current_height.checked_sub(1)?;

        let link = self.state.load_head_skip(level, Ordering::SeqCst);
        // We never set head skips to `None`, and shortly after increasing `current_height`,
        // without fail we set all the head skips up to that level to `Some`.
        // Because that section of the code is guarded by a mutex, this is `Some`.
        #[expect(clippy::unwrap_used, reason = "easy to verify that this is `Some`")]
        let mut current = link.unwrap();

        loop {
            let next = current.load_skip(level, Ordering::SeqCst);

            if let Some(node) = next {
                // We should search further ahead since `next` was too small.
                // (So, don't decrement `level`.)
                current = node;
            } else if level == 0 {
                // We've narrowed it down to here, we're done. `next` is `geq`, and the one
                // before it (`current`) is `le`.
                return Some(current);
            } else {
                // This level might have looked too far ahead. Drop down to a lower level.
                level -= 1;
            }
        }
    }

    /// Return the first node whose entry compares greater than or equal to the provided `entry`,
    /// if there is such a node.
    fn find_greater_or_equal(&self, entry: &[u8]) -> Link<'_> {
        // SAFETY of implementation:
        // Any node referenced by `self.find_le_or_geq(_)` was allocated in the `Herd` of
        // `self.state`, which is not invalidated until the `self: Self` and any associated
        // reference-counted clones are dropped or invalidated in some way other than moving the
        // `self: Self` or clones.
        self.find_le_or_geq::<true>(entry)
    }

    /// Return the last node whose entry compares strictly less than the provided `entry`,
    /// if there is such a node.
    fn find_strictly_less(&self, entry: &[u8]) -> Link<'_> {
        // SAFETY of implementation:
        // Identical to the reason of `find_greater_or_equal`.
        self.find_le_or_geq::<false>(entry)
    }
}


// As with the iterator adapters in `crate::iter_defaults`, testing the logic here requires
// a `State` to be provided.... so it suffices to just test `ThreadsafeSkiplist` and
// `LockedThreadsafeSkiplist`.
