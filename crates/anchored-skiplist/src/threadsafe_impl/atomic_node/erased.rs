#![expect(unsafe_code, reason = "need to require that a `Herd` is not dropped too soon")]

use std::ptr;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::atomic::Ordering;

use crate::maybe_loom::AtomicPtr;
use super::{Link, Node};


/// Equivalent to some sort of atomic cell containing a [`Link<'_>`], but lifetime-erased.
///
/// Invariants, enforced by this type and relied on by `unsafe` code:
/// - The wrapped pointer is either null, or else was type-erased from a `&'herd Node<'herd>`.
/// - In the latter case, note that the invariants of [`Node`] apply to that node.
pub(in super::super) struct AtomicErasedLink(AtomicPtr<()>);

#[expect(unreachable_pub, reason = "control ErasedLink's visibility from one site, its definition")]
impl AtomicErasedLink {
    #[inline]
    #[must_use]
    pub fn new_null() -> Self {
        #![expect(clippy::missing_const_for_fn, reason = "loom's AtomicPtr::new is not const")]
        Self(AtomicPtr::new(ptr::null_mut()))
    }

    /// # Panics
    /// Panics if `order` is [`Acquire`] or [`AcqRel`].
    ///
    /// [`Acquire`]: Ordering::Acquire
    /// [`AcqRel`]:  Ordering::AcqRel
    #[inline]
    pub fn store_link(&self, link: Link<'_>, order: Ordering) {
        let erased = ErasedLink::from_link(link);
        let erased = erased.0.cast_mut();
        self.0.store(erased, order);
    }

    /// # Panics
    /// Panics if `order` is [`Acquire`] or [`AcqRel`].
    ///
    /// [`Acquire`]: Ordering::Acquire
    /// [`AcqRel`]:  Ordering::AcqRel
    #[inline]
    pub fn store_null(&self, order: Ordering) {
        let erased = ErasedLink::new_null();
        let erased = erased.0.cast_mut();
        self.0.store(erased, order);
    }

    /// # Panics
    /// Panics if `order` is [`Acquire`] or [`AcqRel`].
    ///
    /// [`Acquire`]: Ordering::Acquire
    /// [`AcqRel`]:  Ordering::AcqRel
    #[inline]
    pub fn store_erased<'herd>(&self, node: &'herd Node<'herd>, order: Ordering) {
        let erased = ErasedLink::new_erased(node);
        let erased = erased.0.cast_mut();
        self.0.store(erased, order);
    }

    /// # Panics
    /// Panics if `order` is [`Release`] or [`AcqRel`].
    ///
    /// [`Release`]: Ordering::Release
    /// [`AcqRel`]:  Ordering::AcqRel
    #[inline]
    #[must_use]
    fn is_null(&self, order: Ordering) -> bool {
        self.0.load(order).is_null()
    }

    /// # Safety
    /// If this `AtomicErasedLink` stores a non-null pointer, representing a
    /// `&'source_herd Node<'source_herd>` (necessarily obtained from
    /// [`AtomicErasedLink::store_link`] on a `Some` link or from
    /// [`AtomicErasedLink::store_erased`]), then:
    /// - The source [`Herd`] in which the node and its data were allocated in must outlive the
    ///   lifetime `'herd` here. That is, starting from whenever the node reference was
    ///   constructed, the source [`Herd`] must not be (or have been) dropped, moved, or otherwise
    ///   invalidated, for at least as long as `'herd`.
    ///
    /// Note that the invariants of [`Node`] must be upheld.
    ///
    /// # Panics
    /// Panics if `order` is [`Release`] or [`AcqRel`].
    ///
    /// [`Release`]: Ordering::Release
    /// [`AcqRel`]:  Ordering::AcqRel
    /// [`Herd`]: bumpalo_herd::Herd
    #[inline]
    #[must_use]
    pub unsafe fn load_link<'herd>(&self, order: Ordering) -> Link<'herd> {
        let erased = self.0.load(order);
        let erased = ErasedLink(erased.cast_const());
        // SAFETY:
        // If the pointer was non-null and thus the wrapping `ErasedLink` represents a `Some` link
        // referencing a node, then that node and its data were allocated in a `Herd` which
        // has not since been invalidated, and will not be for at least as long as `'herd`. The
        // caller has unsafely asserted this, and that satisfies the contract of
        // `ErasedLink::into_link`.
        unsafe { erased.into_link::<'herd>() }
    }
}

impl Default for AtomicErasedLink {
    #[inline]
    fn default() -> Self {
        Self::new_null()
    }
}

impl Debug for AtomicErasedLink {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let link = if self.is_null(Ordering::Relaxed) {
            "<None link>"
        } else {
            "<Some link>"
        };

        f.debug_tuple("AtomicErasedLink")
            .field(&link)
            .finish()
    }
}

/// A lifetime-erased version of [`Link<'_>`].
///
/// Invariants, enforced by this type and relied on by `unsafe` code:
/// - The wrapped `*const ()` is either a null pointer, or else it was type-erased from a
///   `&'herd Node<'herd>`.
/// - In the latter case, note that the invariants of [`Node`] apply to that node.
struct ErasedLink(*const ());

#[expect(unreachable_pub, reason = "control ErasedLink's visibility from one site, its definition")]
impl ErasedLink {
    /// Note that the invariants of [`Node`] must be upheld.
    #[inline]
    #[must_use]
    pub const fn from_link(link: Link<'_>) -> Self {
        if let Some(node) = link {
            Self::new_erased(node)
        } else {
            Self::new_null()
        }
    }

    #[inline]
    #[must_use]
    pub const fn new_null() -> Self {
        // Self invariant: the wrapped pointer is a null pointer.
        Self(ptr::null())
    }

    /// Note that the invariants of [`Node`] must be upheld.
    #[inline]
    #[must_use]
    pub const fn new_erased<'herd>(node: &'herd Node<'herd>) -> Self {
        let node: *const Node<'herd> = node;
        let node = node.cast::<()>();
        // Self invariant: the wrapped pointer was type-erased from a `&'herd Node<'herd>`,
        // and that node satisfies the invariants of `Node`.
        Self(node)
    }

    /// # Safety
    /// If this `ErasedLink` represents a `Some` link, containing a
    /// `&'source_herd Node<'source_herd>`, then:
    /// - The source [`Herd`] in which the node and its data were allocated in must outlive the
    ///   lifetime `'herd` here. That is, starting from whenever the node reference was
    ///   constructed, the source [`Herd`] must not be (or have been) dropped, moved, or otherwise
    ///   invalidated for the lifetime of `'herd`.
    ///
    /// Note that the invariants of [`Node`] must be upheld.
    ///
    /// [`Herd`]: bumpalo_herd::Herd
    #[inline]
    #[must_use]
    pub const unsafe fn into_link<'herd>(self) -> Link<'herd> {
        if self.0.is_null() {
            None
        } else {
            let node = self.0.cast::<Node<'herd>>();
            // SAFETY:
            // The constraints we need to satisfy for this conversion to be sound are:
            // - The pointer is properly aligned
            // - It is non-null
            // - It is dereferenceable
            // - The pointee must be a valid value of type `Node<'herd>`.
            // - While the reference exists, the pointee must not be mutated (except via interior
            //   mutability).
            //
            // If `node` and thus `self.0` were null, this branch would not have been taken,
            // so the first three constraints easily hold. `self.0` was created from a
            // `&Node<'_>` of some unknown lifetime.
            // The alignment and size of a type do not depend on its lifetime parameters,
            // so the pointer is properly aligned and dereferenceable. We also know it's non-null.
            //
            // For the fourth, we know it's a valid value of type `Node<'source_herd>`, so the
            // concern is whether the guarantees of the `skips` and `entry` fields are met for this
            // lifetime parameter. The sole concern, then, is aliasing of the following:
            // - the pointee of `node->skips`
            // - the pointee of `node->entry`
            // - the pointee of `node`
            // By the invariants of [`Node`], accessing them through shared aliasing is valid
            // up until their `Herd` is dropped or otherwise invalidated, including from the
            // `Herd` being moved. The caller asserts that will not happen for at least `'herd`.
            // Therefore, it is sound to create a shared (immutable) reference to them of lifetime
            // `'herd`.
            //
            // Additional invariants required by `Node`:
            // - the node was allocated in a `Herd` (via a `Member`)
            // - it may only be accessed via immutable references (or referred to by `*const`),
            //   not mutable references
            // - any `Node` referenced in a skip of `node` must have been allocated in the same
            //   `Herd`.
            // - the `Herd` of the node lives for at least `'herd`
            //
            // For the first and the third: this `ErasedLink` must have been constructed from a
            // `&Node<'_>`, and thus the invariants must have held (and continued to hold) of that
            // node; and the below act does not change the status of those invariants, so it's fine.
            // For the second: we are not creating a mutable reference.
            // For the fourth: the caller has unsafely asserted precisely that constraint.
            let node: &'herd Node<'herd> = unsafe { &*node };
            Some(node)
        }
    }
}

impl Default for ErasedLink {
    #[inline]
    fn default() -> Self {
        Self::new_null()
    }
}

impl Debug for ErasedLink {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let link = if self.0.is_null() { "<None link>" } else { "<Some link>" };

        f.debug_tuple("ErasedLink")
            .field(&link)
            .finish()
    }
}


#[cfg(all(test, not(tests_with_leaks)))]
mod tests {
    use bumpalo_herd::Herd;

    use super::*;


    #[test]
    fn atomic_from_and_to_node() {
        let herd = Herd::new();
        let member = herd.get();

        let atomic_erased = AtomicErasedLink::new_null();

        let node = Node::new_node_with(&member, 1, 1, |data| data[0] = 2);

        atomic_erased.store_link(Some(node), Ordering::Release);
        assert!(!atomic_erased.is_null(Ordering::Relaxed));

        // SAFETY:
        // The `Herd` which the node was allocated in is not dropped, moved, or otherwise
        // invalidated until the end of this function. The end of this function is also the end of
        // the implicit lifetime of `link`, so the `Herd` lasts long enough.
        let link = unsafe { atomic_erased.load_link(Ordering::Relaxed) };

        let node = link.unwrap();
        assert_eq!(node.height(), 1);
        assert_eq!(node.entry(), &[2]);

        let _check_that_debug_works = format!("{atomic_erased:?}");
    }

    #[test]
    fn atomic_from_and_to_null() {
        let null = AtomicErasedLink::new_null();
        assert!(null.is_null(Ordering::Relaxed));

        let null = AtomicErasedLink::default();
        assert!(null.is_null(Ordering::Relaxed));

        // SAFETY:
        // `null` does not refer to a node, so the lifetime of `link` can be anything.
        let link = unsafe { null.load_link(Ordering::Relaxed) };
        assert!(link.is_none());

        null.store_null(Ordering::Relaxed);
        assert!(null.is_null(Ordering::Relaxed));
        null.store_link(None, Ordering::Relaxed);
        assert!(null.is_null(Ordering::Relaxed));

        // SAFETY:
        // `null` does not refer to a node, so the lifetime of `link` can be anything.
        let link_two = unsafe { null.load_link(Ordering::Relaxed) };

        assert!(link.is_none());
        assert!(link_two.is_none());

        let _check_that_debug_works = format!("{null:?}");
    }

    #[test]
    fn atomic_extend_lifetimes() {
        let herd = Herd::new();
        let member = herd.get();

        let entry = &[0, 1, 2, 3];
        let node = Node::new_node_with(&member, 1, entry.len(), |data| data.copy_from_slice(entry));
        let link = node.load_skip(0, Ordering::Relaxed);

        let erased_node = AtomicErasedLink::new_null();
        erased_node.store_erased(node, Ordering::Relaxed);
        let erased_link = AtomicErasedLink::new_null();
        erased_link.store_link(link, Ordering::Relaxed);

        // This does not move the underlying `Bump`. (`Member` cannot have the `Bump` be inlined,
        // else it could not be returned to the `Herd`, so this is not merely a coincidence of the
        // current implementation.) Also, the destructor of `herd` is run at the end of the
        // function, not here.
        let _moved_member = member;

        // SAFETY:
        // The source `Herd` is not dropped, moved, or otherwise invalidated until the end of this
        // function, so setting the lifetime of `node` to last for the rest of this function is
        // sound.
        let node = unsafe { erased_node.load_link(Ordering::Relaxed) };
        let node = node.unwrap();

        // SAFETY:
        // Same as for `node` above. Though, in this case we also know that it's `None`.
        let link = unsafe { erased_link.load_link(Ordering::Relaxed) };
        assert!(link.is_none());

        assert_eq!(node.height(), 1);
        assert_eq!(node.entry(), entry);

        let _check_that_debug_works = format!("{erased_node:?}");
        let _check_that_debug_works = format!("{erased_link:?}");
    }

    #[test]
    fn erased_from_and_to_node() {
        let herd = Herd::new();
        let member = herd.get();

        let node = Node::new_node_with(&member, 1, 1, |data| data[0] = 2);

        let erased = ErasedLink::new_erased(node);
        assert!(!erased.0.is_null());

        let _check_that_debug_works = format!("{erased:?}");

        // SAFETY:
        // The `Herd` which the node was allocated in is not dropped, moved, or otherwise
        // invalidated until the end of this function. The end of this function is also the end of
        // the implicit lifetime of `link`, so the `Herd` lasts long enough.
        let link = unsafe { erased.into_link() };

        let node = link.unwrap();
        assert_eq!(node.height(), 1);
        assert_eq!(node.entry(), &[2]);
    }

    #[test]
    fn erased_from_and_to_null() {
        let null = ErasedLink::new_null();
        assert!(null.0.is_null());
        let null = ErasedLink::default();
        assert!(null.0.is_null());

        // SAFETY:
        // `null` does not refer to a node, so the lifetime of `link` can be anything.
        let link = unsafe { null.into_link() };
        assert!(link.is_none());

        let null = ErasedLink::from_link(None);
        assert!(null.0.is_null());

        let _check_that_debug_works = format!("{null:?}");

        // SAFETY:
        // `null` does not refer to a node, so the lifetime of `link` can be anything.
        let link = unsafe { null.into_link() };
        assert!(link.is_none());
    }

    #[test]
    fn erased_extend_lifetimes() {
        let herd = Herd::new();
        let member = herd.get();

        let entry = &[0, 1, 2, 3];
        let node = Node::new_node_with(&member, 1, entry.len(), |data| data.copy_from_slice(entry));
        let link = node.load_skip(0, Ordering::Relaxed);

        let erased_node = ErasedLink::new_erased(node);
        let erased_link = ErasedLink::from_link(link);

        // This does not move the underlying `Bump`. (`Member` cannot have the `Bump` be inlined,
        // else it could not be returned to the `Herd`, so this is not merely a coincidence of the
        // current implementation.) Also, the destructor of `herd` is run at the end of the
        // function, not here.
        let _moved_member = member;

        let _check_that_debug_works = format!("{erased_node:?}");
        let _check_that_debug_works = format!("{erased_link:?}");

        // SAFETY:
        // The source `Herd` is not dropped, moved, or otherwise invalidated until the end of this
        // function, so setting the lifetime of `node` to last for the rest of this function is
        // sound.
        let node = unsafe { erased_node.into_link() };
        let node = node.unwrap();

        // SAFETY:
        // Same as for `node` above. Though, in this case we also know that it's `None`.
        let link = unsafe { erased_link.into_link() };
        assert!(link.is_none());

        assert_eq!(node.height(), 1);
        assert_eq!(node.entry(), entry);
    }
}
