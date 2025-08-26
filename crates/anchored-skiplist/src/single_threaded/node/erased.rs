#![expect(unsafe_code, reason = "need to require that a `Bump` is not dropped too soon")]

use std::ptr;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use super::{Link, Node};


/// A lifetime-erased version of [`Link<'_>`].
///
/// Invariants, enforced by this type and relied on by `unsafe` code:
/// - The wrapped `*const ()` is either a null pointer, or else it was type-erased from a
///   `&'bump Node<'bump>`.
/// - In the latter case, note that the invariants of [`Node`] apply to that node.
#[derive(Clone, Copy)]
pub(in super::super) struct ErasedLink(*const ());

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
    pub const fn new_erased<'bump>(node: &'bump Node<'bump>) -> Self {
        let node: *const Node<'bump> = node;
        let node = node.cast::<()>();
        // Self invariant: the wrapped pointer was type-erased from a `&'bump Node<'bump>`,
        // where the node upheld the invariants of `Node`.
        Self(node)
    }

    /// # Safety
    /// If this `ErasedLink` was constructed from a `&'source_bump Node<'source_bump>` (by using
    /// [`ErasedLink::from_link`] on a `Some` link or by using [`ErasedLink::new_erased`]),
    /// then:
    /// - The source [`Bump`] in which the node and its data were allocated in must outlive the
    ///   lifetime `'bump` here. That is, starting from whenever the node reference was constructed,
    ///   the source [`Bump`] must not be (or have been) dropped or otherwise invalidated (including
    ///   by moving that `Bump`) for the lifetime of `'bump`.
    ///
    /// Note that the invariants of [`Node`] must be upheld.
    ///
    /// [`Bump`]: bumpalo::Bump
    #[inline]
    #[must_use]
    pub const unsafe fn into_link<'bump>(self) -> Link<'bump> {
        if self.0.is_null() {
            None
        } else {
            let node = self.0.cast::<Node<'bump>>();
            // SAFETY:
            // The constraints we need to satisfy for this conversion to be sound are:
            // - The pointer is properly aligned
            // - It is non-null
            // - It is dereferenceable
            // - The pointee must be a valid value of type `Node<'bump>`.
            // - While the reference exists, the pointee must not be mutated (except via interior
            //   mutability).
            //
            // If `node` and thus `self.0` were null, this branch would not have been taken,
            // so the first three constraints easily hold. `self.0` was created from a
            // `&Node<'_>` of some unknown lifetime.
            // The alignment and size of a type do not depend on its lifetime parameters,
            // so the pointer is properly aligned and dereferenceable. We also know it's non-null.
            //
            // For the fourth, we know it's a valid value of type `Node<'source_bump>`, so the
            // concern is whether the guarantees of the `skips` and `entry` fields are met for this
            // lifetime parameter. The sole concern, then, is aliasing of the following:
            // - the pointee of `node->skips`
            // - the pointee of `node->entry`
            // - the pointee of `node`
            // By the invariants of [`Node`], accessing them through shared aliasing is valid
            // up until their bump allocator is dropped or otherwise invalidated. The caller
            // asserts that will not happen for at least `'bump`. Therefore, it is sound to create
            // a shared (immutable) reference to them of lifetime `'bump`.
            //
            // Additional invariants required by `Node`:
            // - the node was allocated in a bump allocator
            // - it may only be accessed via immutable references (or referred to by `*const`),
            //   not mutable references
            // - any `Node` referenced in a skip of `node` must have been allocated in the same
            //   bump allocator.
            // - the bump allocator of the node lives for at least `'bump`
            //
            // For the first and the third: this `ErasedLink` must have been constructed from a
            // `&Node<'_>`, and thus the invariants must held (and continued to hold) of that node;
            // this act does not change the status of those invariants, so it's fine.
            // For the second: we are not creating a mutable reference.
            // For the fourth: the caller has unsafely asserted precisely that constraint.
            let node: &'bump Node<'bump> = unsafe { &*node };
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
    use bumpalo::Bump;

    use super::*;


    #[test]
    fn from_and_to_node() {
        let bump = Bump::new();

        let node = Node::new_node_with(&bump, 1, 1, |data| data[0] = 2);

        let erased = ErasedLink::new_erased(node);
        assert!(!erased.0.is_null());

        // SAFETY:
        // The `Bump` which the node was allocated in is not dropped, moved, or otherwise
        // invalidated until the end of this function. The end of this function is also the
        // end of the implicit lifetime of `link`, so the `Bump` lasts long enough.
        let link = unsafe { erased.into_link() };

        let node = link.unwrap();
        assert_eq!(node.height(), 1);
        assert_eq!(node.entry(), &[2]);
    }

    #[test]
    fn from_and_to_null() {
        let null = ErasedLink::new_null();
        assert!(null.0.is_null());

        // SAFETY:
        // `null` does not refer to a node, so the lifetime of `link` can be anything.
        let link = unsafe { null.into_link() };
        assert!(link.is_none());

        let null = ErasedLink::from_link(None);
        assert!(null.0.is_null());

        // SAFETY:
        // `null` does not refer to a node, so the lifetime of `link` can be anything.
        let link = unsafe { null.into_link() };
        assert!(link.is_none());
    }

    #[test]
    fn extend_lifetimes() {
        let bump = Box::new(Bump::new());

        let entry = &[0, 1, 2, 3];
        let node = Node::new_node_with(&bump, 1, entry.len(), |data| data.copy_from_slice(entry));
        let link = node.skip(0);

        let erased_node = ErasedLink::new_erased(node);
        let erased_link = ErasedLink::from_link(link);

        // This does not move the underlying `Bump`. Also, the destructor is run at the end of
        // the function, not here.
        let _moved_box = bump;

        // SAFETY:
        // The source `Bump` is not dropped, moved, or otherwise invalidated until the end of this
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
