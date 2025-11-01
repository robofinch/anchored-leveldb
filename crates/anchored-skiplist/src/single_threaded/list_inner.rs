#![expect(
    unsafe_code,
    reason = "preserve invariance of inputs in a nominally covariant struct, \
              and assert that `Bump`s live longer than the lifetimes of provided references",
)]

use bumpalo::Bump;
use clone_behavior::{AnySpeed, IndependentClone, MirroredClone, MixedClone, Speed};
use seekable_iterator::Comparator;

use crate::{
    iter_defaults::{SkiplistIter, SkiplistSeek},
    node_heights::{MAX_HEIGHT, Prng32, random_node_height},
};
use super::node::{Link, Node};


/// # Safety
/// The `bump` method must return a reference to the same, unmoved [`Bump`] allocator every time it
/// is called. Therefore, that bump allocator must not be dropped or invalidated until `self` is
/// dropped or otherwise invalidated, aside from `self` being moved, as otherwise the `bump` method
/// could be called again. For emphasis: moving `self` must not move the underlying [`Bump`]
/// allocator.
///
/// If `self` can have reference-counted clones, then the condition of `self.bump()` always
/// returning the same [`Bump`] implies that all the reference-counted clones would have to be
/// invalidated as described before the [`Bump`] is invalidated in any way.
///
/// If the [`Link`] returned by [`head_skip`] refers to a node, then that node must have been
/// allocated in the bump allocator of `self`, and thus be valid until `self` (and its bump
/// allocator in particular) is dropped or otherwise invalidated. If the unsafe contract of
/// [`set_head_skip`] is not weakened, and [`head_skip`]'s only source of non-`None` links is
/// [`set_head_skip`], then this condition is satisfied.
///
/// [`head_skip`]: SkiplistState::head_skip
/// [`set_head_skip`]: SkiplistState::set_head_skip
pub(super) unsafe trait SkiplistState: Prng32 + Sized {
    #[must_use]
    fn new_seeded(seed: u64) -> Self;

    #[must_use]
    fn new_from_state(prng_state: (u64, u64)) -> Self;

    #[must_use]
    fn current_prng_state(&self) -> (u64, u64);

    #[must_use]
    fn bump(&self) -> &Bump;

    /// The returned value will always be less than or equal to [`MAX_HEIGHT`].
    #[must_use]
    fn current_height(&self) -> usize;

    /// # Panics
    /// May or may not panic if `current_height` is greater than [`MAX_HEIGHT`].
    fn set_current_height(&mut self, current_height: usize);

    /// If the returned [`Link`] references a [`Node`], then that node was allocated in
    /// the [`self.bump()`] bump allocator.
    ///
    /// # Panics
    /// Panics if `level` is greater than or equal to [`MAX_HEIGHT`].
    ///
    /// [`self.bump()`]: SkiplistState::bump
    #[must_use]
    fn head_skip(&self, level: usize) -> Link<'_>;

    /// # Safety
    /// If the provided `link` is a `Some` value, referencing a `Node`, then that node must have
    /// been allocated in the [`Bump`] allocator which can be obtained from [`self.bump()`].
    ///
    /// # Panics
    /// Panics if `level` is greater than or equal to [`MAX_HEIGHT`].
    ///
    /// [`self.bump()`]: SkiplistState::bump
    unsafe fn set_head_skip(&mut self, level: usize, link: Link<'_>);
}

/// Struct that implements most of the logic of the two single-threaded skiplists provided by
/// this crate.
#[derive(Debug, Clone)]
pub(super) struct SingleThreadedSkiplist<Cmp, State> {
    cmp:   Cmp,
    /// Invariant: `state` must not be dropped or otherwise invalidated, except by being moved,
    /// until `self` is being dropped or otherwise invalidated, except by being moved.
    ///
    /// (Basically, just do not assign anything to this field after `self`'s construction, only
    /// call methods on it.)
    state: State,
}

impl<Cmp, State> IndependentClone<AnySpeed> for SingleThreadedSkiplist<Cmp, State>
where
    Cmp:   Comparator<[u8]> + IndependentClone<AnySpeed>,
    State: SkiplistState,
{
    #[inline]
    fn independent_clone(&self) -> Self {
        let mut new_list = Self {
            cmp:   self.cmp.independent_clone(),
            state: State::new_from_state(self.state.current_prng_state()),
        };

        let iter = SkiplistIter::new(self);

        for entry in iter {
            new_list.insert_with(entry.len(), |data| data.copy_from_slice(entry));
        }

        new_list
    }
}

impl<S: Speed, Cmp: MirroredClone<S>, State: MirroredClone<S>> MirroredClone<S>
for SingleThreadedSkiplist<Cmp, State>
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self {
            cmp:   self.cmp.mirrored_clone(),
            state: self.state.mirrored_clone(),
        }
    }
}

impl<S: Speed, Cmp: MirroredClone<S>, State: MirroredClone<S>> MixedClone<S>
for SingleThreadedSkiplist<Cmp, State>
{
    #[inline]
    fn mixed_clone(&self) -> Self {
        self.mirrored_clone()
    }
}

// Short utility functions
impl<Cmp: Comparator<[u8]>, State: SkiplistState> SingleThreadedSkiplist<Cmp, State> {
    /// Return `Some(node)` if the provided `link` sorts strictly less than the provided `entry`.
    /// Since `None` links are considered to sort after every entry, in such a scenario, the link
    /// is guaranteed to have a `node` in it.
    fn node_before_entry<'b>(&self, link: Link<'b>, entry: &[u8]) -> Option<&'b Node<'b>> {
        let node = link?;

        if self.cmp.cmp(node.entry(), entry).is_lt() {
            Some(node)
        } else {
            None
        }
    }

    /// Determines whether the entries of the two provided nodes compare as equal.
    #[inline]
    fn nodes_equal(&self, lhs: &Node<'_>, rhs: &Node<'_>) -> bool {
        self.cmp.cmp(lhs.entry(), rhs.entry()).is_eq()
    }
}

// Longer utility functions, related to searching through the skiplist.
impl<Cmp: Comparator<[u8]>, State: SkiplistState> SingleThreadedSkiplist<Cmp, State> {
    /// Any nodes referenced by the returned `Link`s were allocated in `self.state.bump()`.
    fn find_preceding_neighbors(&self, entry: &[u8]) -> [Link<'_>; MAX_HEIGHT] {
        let mut prev = [None; MAX_HEIGHT];

        // Return if the current height is `0` (since nothing's in the list in that case).
        let Some(mut level) = self.state.current_height().checked_sub(1) else {
            return prev;
        };

        let link_from_head = loop {
            // If `Some`, this was allocated in `self.state.bump()` by the contract of
            // `SkiplistState`.
            let next = self.state.head_skip(level);

            if let Some(node) = self.node_before_entry(next, entry) {
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

        // This was allocated in `self.state.bump()` above, and may be set to a node
        // in `next` below which was also allocated in `self.state.bump().
        let mut current = link_from_head;

        loop {
            // By the invariants of `Node`, this was also allocated in `self.state.bump()`
            // since `current` was.
            let next = current.skip(level);

            if let Some(node) = self.node_before_entry(next, entry) {
                // We should search further ahead since `next` was too small.
                // (So, don't decrement `level`.)
                current = node;
            } else {
                #[expect(clippy::indexing_slicing, reason = "0 <= level < MAX_HEIGHT")]
                #[expect(clippy::semicolon_outside_block, reason = "block needed for lint scope")]
                {
                    // This is the only part of the function where we write to `prev`.
                    // We know that `current` was allocated in `self.state.bump()`,
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

    /// Any node referenced by the returned `Link` was allocated in `self.state.bump()`.
    fn find_le_or_geq<const GEQ: bool>(&self, entry: &[u8]) -> Link<'_> {
        // Justification for the assertion made above: `self.state.head_skip(_)` and `Node::skip`
        // are our only sources of node references or links in the function. Any node from the
        // former was allocated in `self.state.bump()`. Any node obtained from the latter was
        // allocated in the same bump allocator as the input node. Or, see
        // `find_preceding_neighbors` for a more methodical justification.

        // Return `None` if the current height is `0` (since nothing's in the list in that case).
        let mut level = self.state.current_height().checked_sub(1)?;

        let link_from_head = loop {
            let next = self.state.head_skip(level);

            if let Some(node) = self.node_before_entry(next, entry) {
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
            let next = current.skip(level);

            if let Some(node) = self.node_before_entry(next, entry) {
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

// Practically a `Skiplist` implementation, aside from lacking iterators.
#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Cmp: Comparator<[u8]>, State: SkiplistState> SingleThreadedSkiplist<Cmp, State> {
    #[inline]
    #[must_use]
    pub fn new_seeded(cmp: Cmp, seed: u64) -> Self {
        Self {
            cmp,
            state: State::new_seeded(seed),
        }
    }

    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `create_entry`.
    ///
    /// If the resulting entry compares equal to an entry already in the skiplist, the entry
    /// is discarded, and `false` is returned. Otherwise, `true` is returned. Attempting to add
    /// duplicate entries should be avoided, as the spent memory will not be reclaimed until the
    /// skiplist is dropped.
    ///
    /// Additionally, `init_entry` could insert something into the skiplist (and, if so,
    /// that insertion would complete before this call to `insert_with` would insert the entry),
    /// though doing so is not a good idea.
    pub fn insert_with<'b, F>(&'b mut self, entry_len: usize, init_entry: F) -> bool
    where
        F: FnOnce(&mut [u8]),
    {
        /// This inner function is not generic over the callback `F`, so monomorphization won't
        /// necessarily make a bunch of duplicate copies of it.
        ///
        /// # Safety
        /// Must be called inside `SingleThreadedSkiplist::insert_with`, as unsafe code in here
        /// relies on knowledge of `insert_with`'s body.
        /// (This safety condition trivially holds in the present state of the codebase.)
        unsafe fn inner_insert<'bump, Cmp: Comparator<[u8]>, State: SkiplistState>(
            this:        &mut SingleThreadedSkiplist<Cmp, State>,
            node:        &'bump Node<'bump>,
            node_height: usize,
        ) -> bool {
            let prev = this.find_preceding_neighbors(node.entry());

            // Check whether `node` is already in the skiplist.
            // `prev[0]` should be the greatest node which is strictly less than `node`,
            // so if the next node after `prev[0]` is not equal to `node`, then `node` is
            // unique in the skiplist.
            let next_node = if let Some(prev_node) = prev[0] {
                prev_node.skip(0)
            } else {
                this.state.head_skip(0)
            };

            if next_node.is_some_and(|next_node| this.nodes_equal(next_node, node)) {
                // Ah, well. It was a duplicate. We lose access to the memory allocated to `node`
                // until this skiplist is dropped.
                return false;
            }

            let prev = prev.map(|link| {
                // SAFETY:
                // `this` lives for at least `'bump`, so by the invariant of `this.state`,
                // `this.state` isn't dropped or otherwise invalidated for `'bump`, except by being
                // moved. By the unsafe contract of `SkiplistState`, we then have that
                // `this.state.bump()` is valid for `'bump`.
                // Since any nodes returned by `this.find_preceding_neighbors` were allocated in
                // `this.state.bump()`, including any node referenced by `link`, the precondition
                // of `Node::extend_link_lifetime` is met.
                unsafe { Node::extend_link_lifetime::<'bump>(link) }
            });

            // Only increase the current height after we're sure that we're inserting something.
            if node_height > this.state.current_height() {
                this.state.set_current_height(node_height);
            }

            for (level, prev_link) in prev.into_iter().take(node_height).enumerate() {
                if let Some(preceding_neighbor) = prev_link {
                    // On level `level`, put `node` between `preceding_neighbor` and
                    // `preceding_neighbor`'s skip on the level.

                    let next = preceding_neighbor.skip(level);
                    // SAFETY:
                    // As discussed above, any node in `prev`, like `preceding_neighbor`, was
                    // allocated in `this.state.bump()`. Looking at the body of `insert_with`,
                    // `node` was allocated in the same bump allocator.
                    // And by the invariants of `Node`, if `next` refers to a node, that node was
                    // allocated in the same bump allocator as `preceding_neighbor`, too.
                    // Thus, `next` and `node` were both allocated in `this.state.bump()`
                    unsafe { node.set_skip(level, next) }
                    // SAFETY:
                    // As stated above, `node` and `preceding_neighbor` were allocated in the
                    // same bump allocator.
                    unsafe { preceding_neighbor.set_skip(level, Some(node)) }
                } else {
                    // `node` is sorted as the first node on this level;
                    // put `node` before the previously-first node on this level (if there was one).

                    let next = this.state.head_skip(level);
                    // SAFETY:
                    // Looking at the body of `insert_with`, `node` was allocated in
                    // `this.state.bump()`. By the contract of `SkiplistState`, we have that
                    // any node referenced by the link returned from `this.state.head_skip(_)`
                    // was allocated in `this.state.bump()`, too. `next` and `node` were allocated
                    // in the same bump allocator.
                    unsafe { node.set_skip(level, next) }
                    // SAFETY:
                    // Looking at the body of `insert_with`, `node` was allocated in
                    // `this.state.bump()`.
                    unsafe { this.state.set_head_skip(level, Some(node)); }
                }
            }

            true
        }

        let node_height = random_node_height(&mut self.state);

        // This call could panic, due to the `init_entry` callback (or allocation failure). If it
        // were to panic, the worst that happens here is that we've mutated the PRNG (no problem),
        // and wasted some memory in the bump allocator (that's not good, but not awful).
        // Additionally, `init_entry` could insert a node. All we've done so far is mutate the
        // prng, so that's fine.
        let node = Node::new_node_with(self.state.bump(), node_height, entry_len, init_entry);
        // SAFETY:
        // `self` lives for at least `'b`, so the invariant of `self.state` implies that
        // `self.state` isn't dropped or otherwise invalidated (in this case, even by being moved)
        // for at least `'b`, either.
        // The unsafe contract of `SkiplistState` then implies that `self.state.bump()` remains
        // valid for at least `'b`. Since `node` was thus allocated in a `Bump` allocator which
        // remains valid for at least `'b`, extending the lifetime of the node to `'b` is sound.
        let node = unsafe { node.extend_lifetime::<'b>() };

        // SAFETY:
        // `inner_insert` is being called from `SingleThreadedSkiplist::insert_with`, so we're good.
        unsafe { inner_insert(self, node, node_height) }
    }

    /// Check whether the entry, or something which compares as equal to the entry, is in
    /// the skiplist.
    pub fn contains(&self, entry: &[u8]) -> bool {
        self.find_greater_or_equal(entry)
            .is_some_and(|node| self.cmp.cmp(node.entry(), entry).is_eq())
    }
}

// SAFETY:
// Each of the below four functions justifies why the returned reference may be soundly
// lifetime-extended, provided that, for at least the length of the new lifetime, the source
// `self: SingleThreadedSkiplist<_,_>` value (or, at least one of the reference-counted clones
// associated with it) is not dropped or invalidated in some way other than by moving that `Self`
// value (or its clones).
// As discussed by `SkiplistSeek`, a sound implementation of `SkiplistNode` for `Node`
// implies the last requirement for this implementation to uphold the unsafe contract.
unsafe impl<Cmp: Comparator<[u8]>, State: SkiplistState> SkiplistSeek
for SingleThreadedSkiplist<Cmp, State>
{
    type Node<'a> = Node<'a> where Self: 'a;
    type Cmp      = Cmp;

    /// Return the first node in the skiplist, if the skiplist is nonempty.
    ///
    /// This operation is fast.
    #[inline]
    fn get_first(&self) -> Link<'_> {
        // SAFETY of implementation:
        // Any node referenced by `self.state.head_skip(_)` was allocated in `self.state.bump()`,
        // which is not invalidated until the `self: Self` (and any associated reference-counted
        // clone) is dropped or invalidated in some way other than moving the `self: Self`
        // (or its clones).

        // The very first link on the lowest level leads to the first node.
        self.state.head_skip(0)
    }

    /// Return the last node in the skiplist, if the skiplist is nonempty.
    fn find_last(&self) -> Link<'_> {
        // SAFETY of implementation:
        // This is basically the same as `find_strictly_less`, except the entry is
        // the `None` link, and thus any non-`None` node comes before that phantom entry.
        // So the assertion about lifetime extension holds for the same reason as `find_le_or_geq`.

        // Return `None` if the current height is `0` (since nothing's in the list in that case).
        let mut level = self.state.current_height().checked_sub(1)?;
        // We never set head skips to `None`, and shortly after increasing `current_height`,
        // without fail we set all the head skips up to that level to `Some`.
        // Therefore, this is `Some`.
        #[expect(clippy::unwrap_used, reason = "easy to verify that this is `Some`")]
        let mut current = self.state.head_skip(level).unwrap();

        loop {
            let next = current.skip(level);

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
        // Any node referenced by `self.find_le_or_geq(_)` was allocated in `self.state.bump()`,
        // which is not invalidated until the `self: Self` (and any associated reference-counted
        // clone) is dropped or invalidated in some way other than moving the `self: Self`
        // (or its clones).
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
// a `State` to be provided.... so it suffices to just test `SimpleSkiplist` and
// `ConcurrentSkiplist`.
