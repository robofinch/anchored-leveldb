#![expect(unsafe_code, reason = "allow lifetime extension of list-allocated nodes")]

use std::ptr;
use std::marker::PhantomData;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use super::SkiplistSeek;


/// A lifetime-erased version of `Option<&'_ List::Node<'_>>`.
///
/// This is essentially a `crate::single_threaded::node::erased_node::ErasedNode`,
/// but fewer restrictions are placed on it. That is, there is no mention of `Bump` allocators,
/// or invariants of the `Node` type.
///
/// Invariant, enforced by this type and relied on by `unsafe` code:
/// - The wrapped `*const ()` is either a null pointer, or else it was type-erased from a
///   `&'source List::Node<'source>`.
pub(super) struct ErasedListLink<List>(*const (), PhantomData<List>);

#[expect(unreachable_pub, reason = "control visibility from one site: the type definition")]
impl<List> ErasedListLink<List> {
    #[inline]
    #[must_use]
    pub const fn new_null() -> Self {
        Self(ptr::null(), PhantomData)
    }

    #[inline]
    pub const fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

#[expect(unreachable_pub, reason = "control visibility from one site: the type definition")]
impl<List: SkiplistSeek> ErasedListLink<List> {
    #[inline]
    #[must_use]
    pub const fn from_link<'a>(link: Option<&'a List::Node<'a>>) -> Self {
        if let Some(node) = link {
            Self::new_erased(node)
        } else {
            Self::new_null()
        }
    }

    #[inline]
    #[must_use]
    pub const fn new_erased<'a>(node: &'a List::Node<'a>) -> Self {
        let node: *const List::Node<'a> = node;
        let node = node.cast::<()>();

        Self(node, PhantomData)
    }

    /// # Safety
    /// If this `ErasedListLink` was constructed from a `&'a List::Node<'a>` (by using
    /// [`ErasedListLink::from_link`] on a `Some` link or by using [`ErasedListLink::new_erased`]),
    /// then:
    /// - That node reference must have been obtained from one of the four methods of
    ///   the `List: SkiplistSeek` type, or from recursively applying `SkiplistNode::next_node` to
    ///   such a reference.
    /// - The source `List` which the node reference came from must outlive the `'a` reference here;
    ///   for at least the length of `'a`, the source `List` must not be dropped or otherwise
    ///   invalidated, except by moving that `List`.
    #[inline]
    #[must_use]
    pub const unsafe fn into_link<'a>(self) -> Option<&'a List::Node<'a>> {
        if self.0.is_null() {
            None
        } else {
            let node = self.0.cast::<List::Node<'a>>();
            // SAFETY:
            // The constraints we need to satisfy for this conversion to be sound are:
            // - The pointer is properly aligned
            // - It is non-null
            // - It is dereferenceable
            // - The pointee must be a valid value of type `List::Node<'a>`.
            // - While the reference exists, the pointee must not be mutated (except via interior
            //   mutability).
            //
            // If `node` and thus `self.0` were null, this branch would not have been taken,
            // so the first three constraints easily hold. `self.0` was created from a
            // `&List::Node<'_>` of some unknown lifetime.
            // The alignment and size of a type do not depend on its lifetime parameters,
            // so the pointer is properly aligned and dereferenceable. We also know it's non-null.
            //
            // For the fourth and fifth, we know the pointee is a valid value of type
            // `List::Node<'_>`, so we need to justify why a reference like `&'_ List::Node<'_>`
            // is actually valid as `&'a List::Node<'a>`.
            // The caller asserts that the source `List` is not dropped or otherwise invalidated
            // for at least `'a`, except by moving that `List`, and that the original pointee
            // was returned by one of the `SkiplistSeek` methods applied to that `List`, or a
            // `SkiplistNode` method recursively applied to such a node.
            // By the unsafe contract of `SkiplistSeek`, constructing this `&'a List::Node<'a>`
            // reference is sound.
            let node: &'a List::Node<'a> = unsafe { &*node };
            Some(node)
        }
    }
}

impl<List> Clone for ErasedListLink<List> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<List> Copy for ErasedListLink<List> {}

impl<List> Default for ErasedListLink<List> {
    #[inline]
    fn default() -> Self {
        Self::new_null()
    }
}

impl<List> Debug for ErasedListLink<List> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let link = if self.0.is_null() { "<None link>" } else { "<Some link>" };

        f.debug_tuple("ErasedListLink")
            .field(&link)
            .finish()
    }
}


#[cfg(all(test, not(tests_with_leaks)))]
mod tests {
    use seekable_iterator::DefaultComparator;
    use crate::{SimpleSkiplist, Skiplist};
    use super::super::{SkiplistNode, SkiplistSeek};
    use super::ErasedListLink;


    #[test]
    fn from_and_to_node() {
        let mut list = SimpleSkiplist::new(DefaultComparator);
        list.insert_copy(&[1, 2, 3]);

        inner_test(list.get_list_seek());

        // We need to be able to provide a `ErasedListLink<List>` type annotation.
        fn inner_test<List: SkiplistSeek>(list: List) {
            let first = list.get_first();
            let node = first.unwrap();

            let erased: ErasedListLink<List> = ErasedListLink::new_erased(node);
            assert!(!erased.is_null());

            // SAFETY:
            // - We got the source node reference via a `SkiplistSeek` method.
            // - The source list, `list`, has not been invalidated and will not be invalidated
            //   until the end of this function, which is the new lifetime. Therefore, the lifetime
            //   is not too long, and this is sound.
            let link = unsafe { erased.into_link() };

            let node = link.unwrap();
            assert_eq!(node.node_entry(), &[1, 2, 3]);
            assert!(matches!(node.next_node(), None));
        }
    }

    #[test]
    fn from_and_to_null() {
        let list = SimpleSkiplist::new(DefaultComparator).get_list_seek();

        inner_test(list);

        // We need to be able to provide a `ErasedListLink<List>` type annotation.
        fn inner_test<List: SkiplistSeek>(_list: List) {
            let null: ErasedListLink<List> = ErasedListLink::default();
            assert!(null.is_null());

            let null: ErasedListLink<List> = ErasedListLink::new_null();
            assert!(null.is_null());

            // SAFETY:
            // `null` does not refer to a node, so the lifetime of `link` can be anything.
            let link: Option<&List::Node<'_>> = unsafe { null.into_link() };
            assert!(matches!(link, None));

            let null: ErasedListLink<List> = ErasedListLink::from_link(None);
            assert!(null.is_null());

            // SAFETY:
            // `null` does not refer to a node, so the lifetime of `link` can be anything.
            let link: Option<&List::Node<'_>> = unsafe { null.into_link() };
            assert!(matches!(link, None));
        }
    }

    #[test]
    fn extend_lifetimes() {
        let mut list = SimpleSkiplist::new(DefaultComparator);
        list.insert_copy(&[4, 5]);
        // Note that the list should be sorted, so the one starting with `1` should be first.
        list.insert_copy(&[1, 2, 3]);

        inner_test(list.get_list_seek());

        fn inner_test<List: SkiplistSeek>(list: List) {
            let entry_one = &[1, 2, 3];
            let entry_two = &[4, 5];

            let node_one = list.get_first().unwrap();
            let node_two = node_one.next_node().unwrap();

            assert_eq!(node_one.node_entry(), entry_one);
            assert_eq!(node_two.node_entry(), entry_two);

            let erased_node: ErasedListLink<List> = ErasedListLink::new_erased(node_one);
            let erased_link: ErasedListLink<List> = ErasedListLink::from_link(Some(node_two));

            let _moved_list = list;

            // SAFETY:
            // - The inner `node` was obtained from a `SkiplistSeek` method
            // - The source list has not been invalidated in any way other than moving it,
            //   and will not be until it is dropped at the end of this function, so setting
            //   the lifetime to last until the end of the function is sound.
            let node_one = unsafe { erased_node.into_link() };
            let node_one = node_one.unwrap();

            // SAFETY:
            // - The inner `node` was obtained from applying `SkiplistNode::next_node` to a node
            //   obtained from a `SkiplistSeek` method
            // - The source list has not been invalidated in any way other than moving it,
            //   and will not be until it is dropped at the end of this function, so setting
            //   the lifetime to last until the end of the function is sound.
            let link = unsafe { erased_link.into_link() };
            let node_two = link.unwrap();

            assert_eq!(node_one.node_entry(), entry_one);
            assert_eq!(node_two.node_entry(), entry_two);
        }
    }
}
