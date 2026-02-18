#![expect(unsafe_code, reason = "Unsafe external synchronization for a `Bump` and a PRNG")]

use core::sync::atomic::Ordering;

use variance_family::UpperBound;

use crate::maybe_loom::AtomicU8;
use crate::interface::{Comparator as _, EncodeWith, Entry, Key, SkiplistFormat};
use super::bump::ExternallySynchronizedBump;
use super::{
    heights::{ExternallySynchronizedRand32, MAX_HEIGHT_USIZE},
    node::{AllocErr, Link, NodeBuilder, NodeRef},
};


#[expect(missing_debug_implementations, reason = "Simply not urgent. TODO: do this.")]
pub struct RawSkiplist<F, U> {
    /// # Safety invariant
    /// Any referenced nodes must have been allocated in `self.bump`.
    head:           [Link<F, U>; MAX_HEIGHT_USIZE],
    /// Should be in the range `0..=MAX_HEIGHT_USIZE`.
    current_height: AtomicU8,
    prng:           ExternallySynchronizedRand32,
    /// # Safety invariant
    /// Must not be invalidated except by dropping or resetting `self`. When resetting `self`,
    /// `self.head` must not reference any nodes.
    bump:           ExternallySynchronizedBump,
}

impl<F: SkiplistFormat<U>, U: UpperBound> RawSkiplist<F, U> {
    #[inline]
    #[must_use]
    pub fn new(capacity: usize, seed: u64) -> Self {
        Self {
            // Vacuously, all of the zero nodes referenced by these `None` links are allocated in
            // `self.bump`.
            head:           Default::default(),
            current_height: AtomicU8::new(0),
            prng:           ExternallySynchronizedRand32::new(seed),
            bump:           ExternallySynchronizedBump::with_capacity(capacity),
        }
    }

    /// Reset the skiplist, reusing some (and possibly all) of its allocations.
    ///
    /// This invalidates any previously returned entry and key references.
    pub fn reset(&mut self) {
        // Vacuously, all of the zero nodes referenced by these `None` links are allocated in
        // `self.bump`.
        self.head.fill_with(Link::new_none);
        *self.current_height.get_mut() = 0;
        // We are resetting `self`. Additionally, if we get here, then all nodes allocated in
        // `self.head` have already been overwritten with `None` links;
        // the safety invariant is upheld.
        self.bump.reset();
    }

    /// Insert an entry into the skiplist.
    ///
    /// If the keys of multiple entries in the skiplist compared equal, it is unspecified what
    /// order they have amongst themselves. (The skiplist will still be sorted in ascending order.)
    ///
    /// # Correctness
    /// The total order provided by `cmp` should be the same total order used to insert any previous
    /// elements into the skiplist. It is not unsound for this condition to fail, though the
    /// skiplist may end up in an unsorted order; attempts to search for entries may then
    /// unexpectedly fail.
    ///
    /// # Safety
    /// This method should not be called concurrently with other calls to this method
    /// or (in the future) `RawSkiplist::debug_full`. That is, calls to this method must *not* race
    /// with other calls to [`self.insert_with(_, _)`] or (in the future) `self.debug_full(_)`.
    ///
    /// [`self.insert_with(_, _)`]: RawSkiplist::insert_with
    #[expect(
        clippy::missing_panics_doc,
        clippy::panic_in_result_fn,
        reason = "panic could only occur if there's a bug",
    )]
    pub unsafe fn insert_with<E>(
        &self,
        cmp:     &F::Cmp,
        encoder: E,
    ) -> Result<Entry<'_, F, U>, AllocErr>
    where
        F: EncodeWith<E, U>,
    {
        // SAFETY: The only places where we call `self.prng.random_node_height()` or
        // `self.prng.debug(_)` are here and (in the future) `Self::debug_full`.
        // The caller guarantees that no other such calls can happen concurrently with this call,
        // so we meet the preconditions to access `self.prng`.
        let node_height = unsafe { self.prng.random_node_height() };
        // SAFETY: Same as above for accessing `self.prng`; we only call
        // `NodeBuilder::new_node_with` here, never directly call `self.bump.try_alloc_with`
        // except via this call, and only call `self.bump.debug(_)` (in the future) in
        // `Self::debug_full`. The caller guarantees that such calls do not race, so this access
        // to `self.bump` does not race with other threads.
        let mut node = unsafe {
            NodeBuilder::new_node_with(&self.bump, node_height, encoder)?
        };

        let (links, key) = node.parts();

        // Note that even though we discard entries in `prev_nodes` that are greater than
        // `node_height`, it doesn't particularly matter; we'd need to compute those extra nodes
        // anyway in the process of traversing the skiplist.
        let prev_nodes = self.find_prev_nodes(cmp, key);

        // Essentially, this sets `node->next` to `node->prev->next`.
        #[expect(clippy::non_zero_suggestions, reason = "not helpful here")]
        {
            assert!(
                usize::from(node_height.get()) <= MAX_HEIGHT_USIZE,
                "`prng.random_node_height()` should be at most `MAX_HEIGHT`",
            );
            assert_eq!(
                usize::from(node_height.get()),
                links.len(),
                "the `links` of `NodeBuilder::new_node_with(_, h, _).parts()` should have length `h`",
            );
        };
        #[expect(clippy::indexing_slicing, reason = "checked by above asserts, and loop bounds")]
        for level in 0..node_height.get() {
            // `Relaxed` is fine because this function does not race with anything
            // that mutates the skiplist.
            let prev_next = if let Some(prev) = prev_nodes[usize::from(level)] {
                prev.load_skip(level, Ordering::Relaxed)
            } else {
                // `node->prev` is basically `self.head`
                // Note that the lifetime of the return value is unified with `prev.load_skip`,
                // which traces back to `self.find_prev_nodes`, which has some lifetime
                // that ends within this function body.
                // SAFETY: since `self.bump` is not reset or dropped within this function body,
                // the referenced node allocation (if any) is valid for at least the lifetime `'_`
                // that only lasts within this function body.
                unsafe { self.head[usize::from(level)].load(Ordering::Relaxed) }
            };

            links[usize::from(level)].write(Link::new(prev_next));
        }

        // After we finish building the node, we never again assert exclusive/mutable access
        // to any part of it (except via internal mutability).
        // SAFETY: Since `node_height.get() == links.len()`, we initialized every skip link of the
        // builder. Additionally, the initialized links came either from
        // `self.find_prev_nodes` or from `self.head`, both of which guarantee that any
        // referenced nodes were allocated in `self.bump`.
        let node = unsafe { node.finish() };

        // Note that it's fine for readers to read an unexpectedly old or new value of
        // `self.current_height`, `self.head` links, and other nodes' links; it merely means that
        // the readers might
        // (a) miss out on skipping further ahead into the skiplist; does not harm correctness.
        // (b) skip past the newly-inserted node; again, does not harm correctness.
        // Therefore, we don't need to worry about readers seeing some but not all of the below
        // changes. The sole thing we need to ensure is that the readers see `node` itself
        // as initialized; therefore, publishing pointers to `node` requires `Release` orderings,
        // and reading links requires `Acquire` orderings to establish happens-before orderings
        // between the publish and the read. (Technically, `Consume` ordering might be sufficient.)

        // Now we need to set `node->prev->next` to `node` on each level. This finally publishes
        // pointers to `node`.
        assert!(
            usize::from(node_height.get()) <= MAX_HEIGHT_USIZE,
            "`prng.random_node_height()` should be at most `MAX_HEIGHT`",
        );
        #[expect(clippy::indexing_slicing, reason = "checked by above assert, and loop bounds")]
        for level in 0..node_height.get() {
            if let Some(prev) = prev_nodes[usize::from(level)] {
                // SAFETY: every node referenced by `prev_nodes` came from `self.find_prev_nodes`,
                // which guarantees that it was allocated in `self.bump`. Additionally,
                // we allocated `node` in `self.bump` above. Therefore, `prev` and `node`
                // reference nodes allocated in the same `ExternallySynchronizedBump`.
                unsafe {
                    prev.store_some_skip(level, node, Ordering::Release);
                };
            } else {
                // `node->prev` is basically `self.head`
                // Safety invariant: `node` was allocated in `self.bump` above.
                self.head[usize::from(level)].store_some(node, Ordering::Release);
            }
        }

        // If the node height is higher than the skiplist's current height, then the above loop
        // set at least one higher link of `self.head` to a `Some` link. Increase the height so
        // that readers check it.
        // NOTE: Ordinarily, something like the CAS-based `fetch_max` should be used.
        // However, since the sole store to `self.current_height` is here, and the caller ensures
        // that this function is protected against mutating races, separate loads and stores are
        // fine.
        if node_height.get() > self.current_height.load(Ordering::Relaxed) {
            self.current_height.store(node_height.get(), Ordering::Relaxed);
        }

        Ok(node.entry())
    }

    /// Return an entry in the skiplist which has the corresponding key.
    ///
    /// If multiple entries have keys which compare equal to the given key, it is unspecified
    /// which of them is returned.
    ///
    /// # Correctness
    /// The total order provided by `cmp` should be the same total order used to insert any
    /// elements into the skiplist. It is not unsound for this condition to fail, though attempts
    /// to search for entries may then unexpectedly fail.
    #[must_use]
    pub fn get_entry(
        &self,
        cmp: &F::Cmp,
        key: Key<'_, F, U>,
    ) -> Option<Entry<'_, F, U>> {
        let node = self.find_greater_or_equal(cmp, key.clone())?;
        if cmp.cmp(node.key(), key).is_eq() {
            // SAFETY: so long as the `'_` borrow on `self` is active, `self` cannot be dropped
            // or reset (since `Self::reset` requires an exclusive/mutable borrow).
            // By the robust guarantee of `self.find_greater_or_equal`, we thus have that
            // the referenced node remains valid for lifetime `'_`.
            let node = unsafe { node.extend_lifetime() };
            Some(node.entry())
        } else {
            None
        }
    }

    /// Return the first node in the skiplist, if the skiplist is nonempty.
    ///
    /// This operation is fast.
    ///
    /// # Robust guarantees
    /// Any node referenced by the returned link remains valid until `self` is dropped or
    /// [`self.reset()`] is called.
    ///
    /// [`self.reset()`]: RawSkiplist::reset
    #[must_use]
    pub(super) fn get_first(&self) -> Option<NodeRef<'_, F, U>> {
        // SAFETY: (and correctness of robust guarantee:)
        // Even if `self.bump` is moved, the memory allocated by it is not invalidated. Only when
        // the allocator is reset or dropped is its allocations invalidated. Since every node
        // referenced by `self.head` was allocated in `self.bump`, and the only way exposed to drop
        // or reset `self.bump` is by calling `self.reset()` or dropping `self`, the guarantee is
        // correct.
        // This justification extends to the below methods as well.
        // Also, see `insert_with` for discussion of atomic orderings.
        unsafe { self.head[0].load(Ordering::Acquire) }
    }

    /// Return the first node whose key compares greater than or equal to the provided `key`,
    /// if there is such a node.
    ///
    /// # Robust guarantees
    /// Any node referenced by the returned link remains valid until `self` is dropped or
    /// [`self.reset()`] is called.
    ///
    /// # Correctness
    /// The total order provided by `cmp` should be the same total order used to insert any
    /// elements into the skiplist. It is not unsound for this condition to fail, though attempts
    /// to search for entries may then unexpectedly fail.
    ///
    /// [`self.reset()`]: RawSkiplist::reset
    #[must_use]
    pub(super) fn find_greater_or_equal(
        &self,
        cmp: &F::Cmp,
        key: Key<'_, F, U>,
    ) -> Option<NodeRef<'_, F, U>> {
        // See `get_first` about the robust guarantee.
        self.find_le_or_geq::<true>(cmp, key)
    }

    /// Return the last node whose key strictly less than the provided `key`,
    /// if there is such a node.
    ///
    /// # Robust guarantees
    /// Any node referenced by the returned link remains valid until `self` is dropped or
    /// [`self.reset()`] is called.
    ///
    /// # Correctness
    /// The total order provided by `cmp` should be the same total order used to insert any
    /// elements into the skiplist. It is not unsound for this condition to fail, though attempts
    /// to search for entries may then unexpectedly fail.
    ///
    /// [`self.reset()`]: RawSkiplist::reset
    #[must_use]
    pub(super) fn find_strictly_less(
        &self,
        cmp: &F::Cmp,
        key: Key<'_, F, U>,
    ) -> Option<NodeRef<'_, F, U>> {
        // See `get_first` about the robust guarantee.
        self.find_le_or_geq::<false>(cmp, key)
    }

    /// Return the last node in the skiplist, if the skiplist is nonempty.
    ///
    /// # Robust guarantees
    /// Any node referenced by the returned link remains valid until `self` is dropped or
    /// [`self.reset()`] is called.
    ///
    /// [`self.reset()`]: RawSkiplist::reset
    #[must_use]
    pub(super) fn find_last(&self) -> Option<NodeRef<'_, F, U>> {
        // See `get_first` about the robust guarantee.
        self.find_last_impl()
    }
}

// Utility functions

/// Return `Some(node)` if the provided `link` sorts strictly less than the provided `key`.
///
/// Note that `None` links are considered to sort after every key.
#[inline]
#[must_use]
fn node_before_key<'a, F: SkiplistFormat<U>, U: UpperBound>(
    cmp:  &F::Cmp,
    link: Option<NodeRef<'a, F, U>>,
    key:  Key<'_, F, U>,
) -> Option<NodeRef<'a, F, U>> {
    let node = link?;

    if cmp.cmp(node.key(), key).is_lt() {
        Some(node)
    } else {
        None
    }
}

impl<F: SkiplistFormat<U>, U: UpperBound> RawSkiplist<F, U> {
    /// In each skiplist level, find the greatest node which compares strictly less than `key`.
    ///
    /// # Robust guarantees
    /// Any nodes referenced by the returned `Link`s were allocated in `self.bump`.
    ///
    /// # Logical correctness
    /// This function should not race with functions that mutate the skiplist.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Keys are expected to be cheap to clone and pass by value",
    )]
    #[must_use]
    fn find_prev_nodes<'a>(
        &'a self,
        cmp: &F::Cmp,
        key: Key<'_, F, U>,
    ) -> [Option<NodeRef<'a, F, U>>; MAX_HEIGHT_USIZE] {
        let mut prev_nodes = [None; MAX_HEIGHT_USIZE];

        // If the current height is `0`, nothing is in the skiplist, and the `prev_nodes` are all
        // `None` links.
        let current_height = self.current_height.load(Ordering::Relaxed);
        let Some(mut level) = current_height.checked_sub(1) else {
            return prev_nodes;
        };

        // Help the optimizer. Note that `level < self.current_height`.
        assert!(
            usize::from(level) < MAX_HEIGHT_USIZE,
            "`RawSkiplist.current_height` should be at most `MAX_HEIGHT_USIZE`",
        );

        // Find a node in `self.head` which compares strictly less than `key`
        // (or return a `None`-filled `prev_nodes`).
        let link_from_head = loop {
            // Specifically, `level` is only ever decremented from `self.current_height - 1`,
            // which is strictly less than `MAX_HEIGHT_USIZE == self.head.len()`.
            #[expect(clippy::indexing_slicing, reason = "checked by above assert")]
            // If `Some`, this was allocated in `self.bump` by the safety invariant of `self.head`.
            // `Relaxed` is fine because this function does not race with anything
            // that mutates the skiplist.
            // SAFETY: since `self.bump` is not reset or dropped for at least lifetime `'a`,
            // the referenced node allocation (if any) is valid for at least lifetime `'a`.
            let next: Option<NodeRef<'a, F, U>> = unsafe {
                self.head[usize::from(level)].load(Ordering::Relaxed)
            };

            if let Some(node) = node_before_key(cmp, next, key.clone()) {
                // We've found a node in `self.head` which compares strictly less than `key`.
                // (So, don't decrement `level`; continue searching it starting from `node`.)
                break node;
            } else if let Some(decremented) = level.checked_sub(1) {
                // This level might have looked too far ahead. Drop down to a lower level.
                level = decremented;
            } else {
                // If we get here, every node linked from the head compares greater than or equal
                // to the provided key.
                return prev_nodes;
            }
        };

        // `link_from_head` was allocated in `self.bump`, and this variable may be set to
        // to the `next` node below, which was also allocated in `self.bump`.
        // Note that `current` should always compare strictly less than `key`.
        let mut current = link_from_head;

        loop {
            // By the invariants of the skiplist nodes, this was also allocated in the same
            // `ExternallySynchronizedBump` as `current`, which is `self.bump`.
            // `Relaxed` is fine because this function does not race with anything
            // that mutates the skiplist.
            let next = current.load_skip(level, Ordering::Relaxed);

            if let Some(node) = node_before_key(cmp, next, key.clone()) {
                // We should search further ahead since `next` is before the key.
                // (So, don't decrement `level`.)
                current = node;
            } else {
                // Specifically, `level` is only ever decremented from `self.current_height - 1`,
                // which is strictly less than `MAX_HEIGHT_USIZE == prev_nodes.len()`.
                #[expect(clippy::indexing_slicing, reason = "checked by above assert")]
                {
                    // This is the only part of the function where we write to `prev`.
                    // We know that `current` was allocated in `self.bump`,
                    // so this function's robust guarantee holds.
                    prev_nodes[usize::from(level)] = Some(current);
                };

                if let Some(decremented) = level.checked_sub(1) {
                    // This level might have looked too far ahead. Drop down to a lower level.
                    level = decremented;
                } else {
                    break prev_nodes;
                }
            }
        }
    }

    /// # Robust guarantees
    /// Any node referenced by the returned link was allocated in `self.bump`.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Keys are expected to be cheap to clone and pass by value",
    )]
    #[must_use]
    fn find_le_or_geq<'a, const GEQ: bool>(
        &'a self,
        cmp: &F::Cmp,
        key: Key<'_, F, U>,
    ) -> Option<NodeRef<'a, F, U>> {
        // Justification for the assertion made above: `self.head.load_skip(_, _)` and
        // `NodeRef::load_skip` are our only sources of node references or links in the function.
        // Any node from the former was allocated in `self.bump` by the safety invariant of
        // `self.head`. Any node obtained from the latter was allocated in the same bump as the
        // input node, as per the invariants of the skiplist node format. Or, see
        // `find_prev_nodes` for a similar justification.

        // Return `None` if the current height is `0` (since nothing's in the list in that case).
        // See `insert_with` for discussion of atomic orderings.
        let current_height = self.current_height.load(Ordering::Relaxed);
        let mut level = current_height.checked_sub(1)?;

        // Help the optimizer. Note that `level < self.current_height`.
        assert!(
            usize::from(level) < MAX_HEIGHT_USIZE,
            "`RawSkiplist.current_height` should be at most `MAX_HEIGHT_USIZE`",
        );

        // Find a node in `self.head` which compares strictly less than `key` (if any).
        let link_from_head = loop {
            #[expect(clippy::indexing_slicing, reason = "checked by above assert")]
            // SAFETY: since `self.bump` is not reset or dropped for at least lifetime `'a`,
            // the referenced node allocation (if any) is valid for at least lifetime `'a`.
            let next: Option<NodeRef<'a, F, U>> = unsafe {
                self.head[usize::from(level)].load(Ordering::Acquire)
            };

            if let Some(node) = node_before_key(cmp, next, key.clone()) {
                break node;
            } else if let Some(decremented) = level.checked_sub(1) {
                level = decremented;
            } else {
                // The lowest-level link from the `head`, which is the first element in the list,
                // is greater than or equal to the `key`; we're done. `next` is `geq`, and nothing
                // is `le`.
                return if GEQ { next } else { None };
            }
        };

        // Note that `current` should always compare strictly less than `key`.
        let mut current = link_from_head;

        loop {
            let next = current.load_skip(level, Ordering::Acquire);

            if let Some(node) = node_before_key(cmp, next, key.clone()) {
                current = node;
            } else if let Some(decremented) = level.checked_sub(1) {
                level = decremented;
            } else {
                // We've narrowed it down to here, we're done. `next` is `geq`, and the one
                // before it (`current`) is `le`.
                break if GEQ { next } else { Some(current) };
            }
        }
    }

    /// # Robust guarantees
    /// Any node referenced by the returned link was allocated in `self.bump`.
    #[must_use]
    fn find_last_impl(&self) -> Option<NodeRef<'_, F, U>> {
        // This is basically the same as `find_strictly_less`, except the key is
        // the `None` link, and thus any non-`None` node comes before that phantom entry.
        // See `find_le_geq` for justifications of correctness.

        // Return `None` if the current height is `0` (since nothing's in the list in that case).
        // See `insert_with` for discussion of atomic orderings.
        let current_height = self.current_height.load(Ordering::Relaxed);
        let mut level = current_height.checked_sub(1)?;

        // Help the optimizer. Note that `level < self.current_height`.
        assert!(
            usize::from(level) < MAX_HEIGHT_USIZE,
            "`RawSkiplist.current_height` should be at most `MAX_HEIGHT_USIZE`",
        );

        // Find a node in `self.head`. Any such node compares less than the `None` pseudo-key.
        let link_from_head = loop {
            #[expect(clippy::indexing_slicing, reason = "checked by above assert")]
            // SAFETY: since `self.bump` is not reset or dropped for at least lifetime `'_`,
            // the referenced node allocation (if any) is valid for at least lifetime `'_`.
            // (Note that there's only one `'_` lifetime in the function signature.)
            let next = unsafe {
                self.head[usize::from(level)].load(Ordering::Acquire)
            };

            if let Some(node) = next {
                break node;
            } else if let Some(decremented) = level.checked_sub(1) {
                level = decremented;
            } else {
                // The skiplist is empty.
                return None;
            }
        };

        let mut current = link_from_head;

        loop {
            let next = current.load_skip(level, Ordering::Acquire);

            if let Some(node) = next {
                current = node;
            } else if let Some(decremented) = level.checked_sub(1) {
                level = decremented;
            } else {
                // `current.load_skip(0, _)` is `None`. This is the last node.
                break Some(current);
            }
        }
    }
}

// TODO: `Debug` functions for `RawSkiplist`.
