#![expect(
    unsafe_code,
    reason = "preserve invariance of inputs in a nominally covariant struct, \
              and assert that `Bump`s live longer than the lifetimes of provided references",
)]

mod erased;


use std::cell::Cell;

use bumpalo::Bump;

use crate::{iter_defaults::SkiplistNode, node_heights::MAX_HEIGHT};

pub(super) use self::erased::ErasedLink;


pub(super) type Link<'bump> = Option<&'bump Node<'bump>>;


/// # Invariants, which may be relied on by unsafe code:
/// Inside [`Node::new_node_with`], aside from its `init_entry` callback:
/// - [`Node::new_node_with`] is self-contained aside from calls to `std` and [`bumpalo`].
///   Unsafe code in this crate or its dependents do not need to worry about being called from
///   [`Node::new_node_with`].
///
/// Outside [`Node::new_node_with`], or in its `init_entry` callback:
/// - Every `Node` value was allocated in a [`Bump`] allocator.
/// - Every `Node` value is only accessible via immutable references (or sometimes `*const`), not
///   mutable aliases; i.e., `Node` values can never be mutated except via internal mutability,
///   so a `Node` value may always be accessed via shared aliasing (immutable references) up until
///   its bump allocator is dropped or otherwise invalidated, including by moving that bump.
/// - For any `self: &Node<'_>`, if `self.skip(level)` references another node, then that other
///   node was allocated in the same [`Bump`] allocator as `self`.
///
/// In either circumstance:
/// - The [`Bump`] allocator of a `Node<'bump>` or `&'bump Node<'bump>` is valid for at least
///   `'bump`. (The lifetime parameter is covariant; that is, shortening it is sound.)
#[derive(Debug)]
pub(super) struct Node<'bump> {
    /// The `Cell`s enable the pointers to be changed. Nothing else is allowed to be changed.
    ///
    /// Vital invariant: for any `ErasedLink` in `skips` which semantically refers to another node
    /// `node`, that `node` must have been allocated in the same [`Bump`] allocator as `self`.
    ///
    /// Callers in this crate also generally put `Some` skips at the start and `None` skips at the
    /// end, though that is not a crucial invariant.
    skips: &'bump [Cell<ErasedLink>],
    entry: &'bump [u8],
}

#[expect(unreachable_pub, reason = "control Node's visibility from one site, its definition")]
impl<'bump> Node<'bump> {
    /// Allocate a new node into the provided `Bump` allocator.
    ///
    /// The `init_entry` callback is passed a mutable slice of length `entry_len`, which is made
    /// the entry of the node.
    ///
    /// The passed `init_entry` callback may also allocate into the `Bump`.
    ///
    /// If the `init_entry` callback panics, if the panic were to unwind, the only problem
    /// would be that some memory would be wasted in the `Bump` until the `Bump` is dropped.
    /// No other logical issues or memory unsafety would occur.
    #[must_use]
    pub fn new_node_with<F>(
        arena:      &'bump Bump,
        height:     usize,
        entry_len:  usize,
        init_entry: F,
    ) -> &'bump Self
    where
        F: FnOnce(&mut [u8]),
    {
        // The outer function is generic over the callback `F`. As much of its functionality as
        // possible has been put into this inner function.
        fn alloc_node<'bump>(
            arena:  &'bump Bump,
            height: usize,
            entry:  &'bump [u8],
        ) -> &'bump Node<'bump> {
            debug_assert!(
                height <= MAX_HEIGHT,
                "this crate should never attempt to create a node with too great a height",
            );

            let skips = arena.alloc_slice_fill_default(height);
            // This satisfies the invariants:
            // - The returned `Node` will have been allocated in a `Bump` allocator
            // - We only return an immutable reference; only inside this function is a mutable
            //   reference available.
            // - None of this node's skips reference another node, so the third invariant will
            //   vacuously hold.
            // - We used a `Bump` that lives for at least `'bump` to create a `Node<'bump>`
            //   and `&'bump Node<'bump>`.
            // Note that `alloc` could panic. The worst that happens, then, is wasting memory
            // in the bump allocator. Memory unsafety, and the struct's invariants, are not
            // compromised. Well, unless that `Node` can somehow be caught when unwinding the
            // panic, but since `Node` is a private type, that should not be a concern provided
            // that *this* crate does not mess with `catch_unwind` where `Node` is a visible type.
            arena.alloc(Node {
                skips,
                entry,
            })
        }

        let entry: &mut [u8] = arena.alloc_slice_fill_default(entry_len);
        // If the callback panics, the worst that can happen is that some memory in the allocator
        // is wasted. And it isn't given privileged access to a `Node` here.
        init_entry(entry);

        // See the comment in `alloc_node` for why `Node`'s invariants are upheld.
        alloc_node(arena, height, entry)
    }

    #[inline]
    #[must_use]
    pub const fn entry(&self) -> &[u8] {
        self.entry
    }

    #[inline]
    #[must_use]
    pub const fn height(&self) -> usize {
        self.skips.len()
    }

    #[must_use]
    pub fn skip(&self, level: usize) -> Link<'bump> {
        // Using too high of a level, but still under `MAX_HEIGHT`, is still useful in searching
        // algorithms.
        if let Some(erased) = self.skips.get(level).map(Cell::get) {
            // SAFETY:
            // By the invariants of `Node` and its `skips` field, we know that if `erased`
            // refers to another node, then that node was allocated in the same bump allocator as
            // `self`. Since `self: &Node<'bump>`, that bump allocator lives for at least `'bump`,
            // and by the invariants of `Node`, the allocator could not have been invalidated
            // in the time since the node was allocated.
            unsafe { erased.into_link::<'bump>() }
        } else {
            None
        }
    }

    /// # Safety
    /// If the provided `link` is a `Some` value, referencing a `Node`, then that node must have
    /// been allocated in the same [`Bump`] allocator that `self` was allocated in.
    ///
    /// # Panics
    /// May or may not panic if `level >= self.height()`, that is, if there is no skip at the
    /// indicated `level` of this node.
    pub unsafe fn set_skip(&self, level: usize, link: Link<'_>) {
        debug_assert!(level < self.height(), "should not try to set a nonexistent skip of a node");

        if let Some(skip) = self.skips.get(level) {
            let erased = ErasedLink::from_link(link);
            // We must ensure that the invariants of `Node` and its `skips` field are upheld.
            // We aren't doing anything of note to `self` aside from modifying one of its skips,
            // so the relevant constraint is:
            //   For any `ErasedLink` in `skips` which semantically refers to another node `node`,
            //   that `node` must have been allocated in the same [`Bump`] allocator as `self`.
            // `erased` refers to another `node` iff `link` does, and if it does, then the caller
            // has asserted that it was allocated in the same `Bump` allocator as `self`.
            // Therefore, the invariant is upheld.
            skip.set(erased);
        }
    }

    /// # Safety
    /// The [`Bump`] allocator which `self` was allocated in must not be (or have been) dropped or
    /// otherwise invalidated (including by moving that `Bump`), starting from when `self` was
    /// allocated, up to at least the length of `'asserted_bump`.
    #[inline]
    #[must_use]
    pub const unsafe fn extend_lifetime<'asserted_bump>(&'bump self)
        -> &'asserted_bump Node<'asserted_bump>
    {
        let node: *const Node<'bump> = self;
        let node: *const Node<'asserted_bump> = node.cast::<Node<'asserted_bump>>();
        // SAFETY:
        // We are solely performing lifetime extension (as is clearly visible above).
        // Therefore, `node` is a properly-aligned, non-null, dereferenceable pointer.
        // We need to confirm that the pointee is a valid value of `Node<'asserted_bump>` and not
        // just of `Node<'bump>`, and that aliasing is satisfied for the `Node` itself as well.
        // The sole concern, then, is aliasing of the following:
        // - the pointee of `node->skips`
        // - the pointee of `node->entry`
        // - the pointee of `node`.
        // By the invariants of [`Node`], accessing them through shared aliasing is valid
        // up until their bump allocator is dropped or otherwise invalidated. The caller
        // asserts that will not happen for at least `'asserted_bump`, and that it has not already
        // happened. Therefore, it is sound to create a shared (immutable) reference to them of
        // lifetime `'asserted_bump`.
        unsafe { &*node }
    }

    /// # Safety
    /// If the `link` is a `Some` value, containing a reference to a [`Node`], then the [`Bump`]
    /// allocator which the node was allocated in must not be (or have been) dropped or otherwise
    /// invalidated (including by moving that `Bump`), starting from when the node was allocated, up
    /// to at least the length of `'asserted_bump`.
    #[inline]
    #[must_use]
    pub const unsafe fn extend_link_lifetime<'asserted_bump>(link: Link<'bump>)
        -> Link<'asserted_bump>
    {
        if let Some(node) = link {
            // SAFETY:
            // - The node was allocated in a bump allocator that the caller has asserted is valid
            //   for at least `'asserted_bump` (and has not been invalidated since `node` was
            //   allocated).
            Some(unsafe { node.extend_lifetime() })
        } else {
            None
        }
    }
}

impl SkiplistNode for Node<'_> {
    #[inline]
    fn next_node(&self) -> Option<&Self> {
        self.skip(0)
    }

    #[inline]
    fn node_entry(&self) -> &[u8] {
        self.entry()
    }
}


#[cfg(all(test, not(tests_with_leaks)))]
mod tests {
    use super::*;


    #[test]
    fn basic_entry_metadata() {
        let bump = Bump::new();

        // In practice, this crate *always* creates node with a height of at least 1.
        // (Else, `SkiplistNode::next_node` would panic on `Node`s, for example.)
        // However, it's still worth checking this edge case.
        let main_node = Node::new_node_with(&bump, 0, 1, |data| data[0] = 2);

        assert_eq!(main_node.height(), 0);
        assert_eq!(main_node.entry(), &[2]);
    }

    #[test]
    fn alloc_and_fill() {
        let bump = Bump::new();

        let node_height = MAX_HEIGHT;

        let main_node = Node::new_node_with(&bump, node_height, 0, |_| {});

        let _check_that_debug_works = format!("{:?}", main_node);

        for level in 0..node_height {
            let new_node = Node::new_node_with(&bump, node_height, 1, |data| {
                assert_eq!(data.len(), 1);
                data[0] = level as u8;
            });

            // SAFETY:
            // The node is allocated in the same bump allocator.
            unsafe { main_node.set_skip(level, Some(new_node)); }
        }

        let _check_that_debug_works = format!("{:?}", main_node);

        assert_eq!(main_node.entry(), &[]);

        for level in 0..node_height {
            assert_eq!(main_node.skip(level).unwrap().entry(), &[level as u8])
        }

        // Note that we never have cause to remove a node in the main skiplist implementation.
        // SAFETY:
        // The link is not a node.
        unsafe { main_node.set_skip(0, None); }

        assert!(main_node.skip(0).is_none());
    }

    #[test]
    fn extend_lifetimes() {
        let bump = Box::new(Bump::new());

        let entry = &[0, 1, 2, 3];
        let node = Node::new_node_with(&bump, 1, entry.len(), |data| data.copy_from_slice(entry));
        let link = node.skip(0);

        let parent = Node::new_node_with(&bump, 1, 0, |_| {});
        // SAFETY:
        // `node` and `parent` were allocated in the same `Bump`.
        unsafe { parent.set_skip(0, Some(node)); }
        let link_to_node = parent.skip(0);

        // SAFETY:
        // The new lifetime of the node lasts up to the end this function. That's also how
        // long the `Bump` lasts before being invalidated (by being dropped); since we aren't doing
        // something risky that depends on drop order, it simply holds that the `Bump` remains valid
        // up to at least the length of the new lifetime.
        // Note also that the `Bump` itself is not moved. Only the `Box` pointing to the `Bump` is.
        // In real scenarios throughout this crate, ideally the lifetime is nameable, not implicit,
        // and the scope of the `Bump` should never be remotely in question due to drop order.
        let node = unsafe { node.extend_lifetime() };

        // SAFETY:
        // See above. It's the same reason.
        let link = unsafe { Node::extend_link_lifetime(link) };
        // SAFETY:
        // See above. It's the same reason.
        let link_to_node = unsafe { Node::extend_link_lifetime(link_to_node) };

        // This does not move the underlying `Bump`. Also, the destructor is run at the end of
        // the function, not here.
        let _moved_box = bump;

        assert_eq!(node.entry(), entry);
        assert!(link.is_none());

        // The node isn't tall enough for this, so it's definitely `None`
        let tall_link = node.skip(1);

        assert_eq!(node.height(), 1);
        assert!(tall_link.is_none());

        let node_handle = link_to_node.unwrap();
        assert_eq!(node_handle.entry(), node.entry());
        assert_eq!(node_handle.entry().as_ptr(), node.entry().as_ptr());
    }
}
