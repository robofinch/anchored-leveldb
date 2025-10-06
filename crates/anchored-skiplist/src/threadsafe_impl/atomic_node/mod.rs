#![expect(
    unsafe_code,
    reason = "preserve invariance of inputs in a nominally covariant struct, \
              and assert that `Herd`s live longer than the lifetimes of provided references",
)]

mod erased;


use std::sync::atomic::Ordering;

use bumpalo_herd::Member;

use crate::{iter_defaults::SkiplistNode, node_heights::MAX_HEIGHT};

pub(super) use self::erased::AtomicErasedLink;


pub(super) type Link<'herd> = Option<&'herd Node<'herd>>;


/// # Invariants, which may be relied on by unsafe code:
/// Inside [`Node::new_node_with`], aside from its `init_entry` callback:
/// - [`Node::new_node_with`] is self-contained aside from calls to `std` and [`bumpalo_herd`].
///   Unsafe code in this crate or its dependents do not need to worry about being called from
///   [`Node::new_node_with`].
///
/// Outside [`Node::new_node_with`], or in its `init_entry` callback:
/// - Every `Node` value was allocated in a [`Herd`], via a [`Member`] of that [`Herd`].
/// - Every `Node` value is only accessible via immutable references (or sometimes pointers), never
///   mutable aliases; i.e., `Node` values can never be mutated except via internal mutability,
///   so a `Node` value may always be accessed via shared aliasing (immutable references) up until
///   its `Herd` is dropped, moved, or otherwise invalidated. The `Member`s of the herd, however,
///   may be dropped, moved, or whatever. (Internally, `bumpalo_herd` ensures that any
///   [`bumpalo::Bump`] is not moved after its initial construction. So long as the source
///   `Herd` isn't destroyed, the underlying `Bump`s are fine. However, the `Member`s contain
///   references to their source `Herd`, and even if that were to happen to change, it likely
///   wouldn't be semver-compatible to move the `Herd`.)
/// - For any `self: &Node<'_>`, if `self.skip(level)` references another node, then that other
///   node was allocated in the same [`Herd`] allocator as `self`. (Not necessarily via the same
///   `Member`.)
///
/// In either circumstance:
/// - The [`Herd`] allocator of a `Node<'herd>` or `&'herd Node<'herd>` is valid for at
///   least `'herd`. (The lifetime parameter is covariant; that is, shortening it is sound.)
///
/// [`Herd`]: bumpalo_herd::Herd
// TODO: reduce memory usage by 16 bytes per Node, by doing the DST more manually
// to avoid having two extra pointers
#[derive(Debug)]
pub(super) struct Node<'herd> {
    /// The `AtomicErasedLink`s have internal mutability that allow the pointers' values to be
    /// changed. Nothing else is allowed to be changed.
    ///
    /// Vital invariant: for any `AtomicErasedLink` in `skips` which semantically refers to another
    /// node `node`, that `node` must have been allocated in the same [`Herd`] allocator as `self`.
    ///
    /// Callers in this crate also generally put `Some` skips at the start and `None` skips at the
    /// end, though that is not a crucial invariant.
    ///
    /// [`Herd`]: bumpalo_herd::Herd
    skips: &'herd [AtomicErasedLink],
    entry: &'herd [u8],
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'herd> Node<'herd> {
    /// Allocate a new node into the provided `Member` allocator.
    ///
    /// The `init_entry` callback is passed a mutable slice of length `entry_len`, which is made
    /// the entry of the node.
    ///
    /// The passed `init_entry` callback may also allocate into the `Member` (or a different
    /// part of the `Herd`).
    ///
    /// If the `init_entry` callback panics, if the panic were to unwind, the only problem
    /// would be that some memory would be wasted in the `Member` until the `Herd` is dropped.
    /// No other logical issues or memory unsafety would occur.
    #[must_use]
    pub fn new_node_with<F>(
        arena:      &Member<'herd>,
        height:     usize,
        entry_len:  usize,
        init_entry: F,
    ) -> &'herd Self
    where
        F: FnOnce(&mut [u8]),
    {
        // The outer function is generic over the callback `F`. As much of its functionality as
        // possible has been put into this inner function.
        fn alloc_node<'herd>(
            arena:  &Member<'herd>,
            height: usize,
            entry:  &'herd [u8],
        ) -> &'herd Node<'herd> {
            debug_assert!(
                height <= MAX_HEIGHT,
                "this crate should never attempt to create a node with too great a height",
            );

            let skips = arena.alloc_slice_fill_default(height);
            // This satisfies the invariants:
            // - The returned `Node` will have been allocated in a `Herd` allocator, via a `Member`
            // - We only return an immutable reference; only inside this function is a mutable
            //   reference available.
            // - None of this node's skips reference another node, so the third invariant will
            //   vacuously hold.
            // - We used a `Member` whose `Herd` lives for at least `'herd` to create a
            //   `Node<'herd>` and `&'herd Node<'herd>`.
            // Note that `alloc` could panic. The worst that happens, then, is wasting memory
            // in the bump allocators. Memory unsafety, and the struct's invariants, are not
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

    /// # Panics
    /// Panics if `order` is [`Release`] or [`AcqRel`].
    ///
    /// [`Release`]: Ordering::Release
    /// [`AcqRel`]:  Ordering::AcqRel
    #[must_use]
    pub fn load_skip(&self, level: usize, order: Ordering) -> Link<'herd> {
        // Using too high of a level, but still under `MAX_HEIGHT`, is still useful in searching
        // algorithms.
        if let Some(erased) = self.skips.get(level) {
            // SAFETY:
            // By the invariants of `Node` and its `skips` field, we know that if `erased`
            // refers to another node, then that node was allocated in the same `Herd` as
            // `self`. Since `self: &Node<'herd>`, that `Herd` lives for at least `'herd`,
            // and by the invariants of `Node`, the allocator could not have been invalidated
            // in the time since the node was allocated.
            unsafe { erased.load_link(order) }
        } else {
            None
        }
    }

    /// # Safety
    /// If the provided `link` is a `Some` value, referencing a `Node`, then that node must have
    /// been allocated in the same [`Herd`] allocator that `self` was allocated in.
    ///
    /// # Panics
    /// Panics if `order` is [`Acquire`] or [`AcqRel`].
    ///
    /// May or may not panic if `level >= self.height()`, that is, if there is no skip at the
    /// indicated `level` of this node.
    ///
    /// [`Acquire`]: Ordering::Acquire
    /// [`AcqRel`]:  Ordering::AcqRel
    /// [`Herd`]: bumpalo_herd::Herd
    pub unsafe fn store_skip(&self, level: usize, link: Link<'_>, order: Ordering) {
        debug_assert!(level < self.height(), "should not try to set a nonexistent skip of a node");

        if let Some(erased) = self.skips.get(level) {
            // We must ensure that the invariants of `Node` and its `skips` field are upheld.
            // We aren't doing anything of note to `self` aside from modifying one of its skips,
            // so the relevant constraint is:
            //   For any `AtomicErasedLink` in `skips` which semantically refers to another node
            //   `node`, that `node` must have been allocated in the same `Herd` allocator as
            //   `self`.
            // If `link` refers to another node, then the caller has asserted that it was allocated
            // in the same `Herd` that `self` was. Therefore, adding this `link` to `self.skips`
            // maintains the invariant.
            erased.store_link(link, order);
        }
    }

    /// # Safety
    /// The [`Herd`] allocator which `self` was allocated in must not be (or have been) dropped,
    /// moved, or  otherwise invalidated, starting from when `self` was allocated, up to at least
    /// the length of `'asserted_herd`.
    ///
    /// [`Herd`]: bumpalo_herd::Herd
    #[inline]
    #[must_use]
    pub const unsafe fn extend_lifetime<'asserted_herd>(&'herd self)
        -> &'asserted_herd Node<'asserted_herd>
    {
        let node: *const Node<'herd> = self;
        let node: *const Node<'asserted_herd> = node.cast::<Node<'asserted_herd>>();
        // SAFETY:
        // We are solely performing lifetime extension (as is clearly visible above).
        // Therefore, `node` is a properly-aligned, non-null, dereferenceable pointer.
        // We need to confirm that the pointee is a valid value of `Node<'asserted_herd>` and not
        // just of `Node<'herd>`, and that aliasing is satisfied for the `Node` itself as well.
        // The sole concern, then, is aliasing of the following:
        // - the pointee of `node->skips`
        // - the pointee of `node->entry`
        // - the pointee of `node`.
        // By the invariants of [`Node`], accessing them through shared aliasing is valid
        // up until their `Herd` is dropped or otherwise invalidated, including by being moved.
        // The caller asserts that will not happen for at least `'asserted_herd`, and that it has
        // not already happened. Therefore, it is sound to create a shared (immutable) reference to
        // them of lifetime `'asserted_herd`.
        unsafe { &*node }
    }

    /// # Safety
    /// If the `link` is a `Some` value, containing a reference to a [`Node`], then the [`Herd`]
    /// allocator which the node was allocated in must not be (or have been) dropped, moved, or
    /// otherwise invalidated, starting from when the node was allocated, up to at least the length
    /// of `'asserted_herd`.
    ///
    /// [`Herd`]: bumpalo_herd::Herd
    #[inline]
    #[must_use]
    pub const unsafe fn extend_link_lifetime<'asserted_herd>(link: Link<'herd>)
        -> Link<'asserted_herd>
    {
        if let Some(node) = link {
            // SAFETY:
            // - The node was allocated in a `Herd` that the caller has asserted is valid for at
            //   least `'asserted_herd` (and has not been invalidated since `node` was allocated).
            Some(unsafe { node.extend_lifetime() })
        } else {
            None
        }
    }
}

impl SkiplistNode for Node<'_> {
    #[inline]
    fn next_node(&self) -> Option<&Self> {
        self.load_skip(0, Ordering::Acquire)
    }

    #[inline]
    fn node_entry(&self) -> &[u8] {
        self.entry()
    }
}


#[cfg(all(test, not(tests_with_leaks)))]
mod tests {
    use bumpalo_herd::Herd;

    use super::*;


    #[test]
    fn basic_entry_metadata() {
        let herd = Herd::new();
        let member = herd.get();

        // In practice, this crate *always* creates node with a height of at least 1.
        // (Else, `SkiplistNode::next_node` would panic on `Node`s, for example.)
        // However, it's still worth checking this edge case.
        let main_node = Node::new_node_with(&member, 0, 1, |data| data[0] = 2);

        assert_eq!(main_node.height(), 0);
        assert_eq!(main_node.entry(), &[2]);
    }

    #[test]
    fn alloc_and_fill() {
        let herd = Herd::new();
        let member = herd.get();

        let node_height = MAX_HEIGHT;

        let main_node = Node::new_node_with(&member, node_height, 0, |_| {});

        let _check_that_debug_works = format!("{:?}", main_node);

        for level in 0..node_height {
            let new_node = Node::new_node_with(&member, node_height, 1, |data| {
                assert_eq!(data.len(), 1);
                data[0] = level as u8;
            });

            // SAFETY:
            // The node is allocated in the same `Herd` allocator.
            unsafe { main_node.store_skip(level, Some(new_node), Ordering::Relaxed); }
        }

        let _check_that_debug_works = format!("{:?}", main_node);

        assert_eq!(main_node.entry(), &[]);

        for level in 0..node_height {
            let other_node = main_node.load_skip(level, Ordering::Relaxed).unwrap();
            assert_eq!(other_node.entry(), &[level as u8])
        }

        // Note that we never have cause to remove a node in the main skiplist implementation.
        // SAFETY:
        // The link is not a node.
        unsafe { main_node.store_skip(0, None, Ordering::Relaxed); }

        assert!(main_node.load_skip(0, Ordering::Relaxed).is_none());
    }

    #[test]
    fn extend_lifetimes() {
        let herd = Herd::new();
        let member = herd.get();

        let entry = &[0, 1, 2, 3];
        let node = Node::new_node_with(&member, 1, entry.len(), |data| data.copy_from_slice(entry));
        let link = node.load_skip(0, Ordering::Relaxed);

        let parent = Node::new_node_with(&member, 1, 0, |_| {});
        // SAFETY:
        // `node` and `parent` were allocated in the same `Herd`.
        unsafe { parent.store_skip(0, Some(node), Ordering::Relaxed) };
        let link_to_node = parent.load_skip(0, Ordering::Relaxed);


        // SAFETY:
        // The new lifetime of the node lasts up to the end this function. That's also how
        // long the `Herd` lasts before being invalidated (by being dropped); since we aren't doing
        // something risky that depends on drop order, it simply holds that the `Herd` remains valid
        // up to at least the length of the new lifetime.
        let node = unsafe { node.extend_lifetime() };

        // SAFETY:
        // See above. It's the same reason.
        let link = unsafe { Node::extend_link_lifetime(link) };

        // SAFETY:
        // See above. It's the same reason.
        let link_to_node = unsafe { Node::extend_link_lifetime(link_to_node) };

        // This does not move the underlying `Bump`, control over its allocation on the heap is
        // returned to the `Herd`.
        drop(member);

        assert_eq!(node.entry(), entry);
        assert!(link.is_none());

        // The node isn't tall enough for this, so it's definitely `None`
        let tall_link = node.load_skip(1, Ordering::Relaxed);

        assert_eq!(node.height(), 1);
        assert!(tall_link.is_none());

        let node_handle =  link_to_node.unwrap();
        assert_eq!(node_handle.entry(), node.entry());
        assert_eq!(node_handle.entry().as_ptr(), node.entry().as_ptr());
    }
}
