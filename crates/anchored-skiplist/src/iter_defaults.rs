#![expect(unsafe_code, reason = "allow lifetime extension of arena-allocated nodes")]

use std::ptr;
use std::marker::PhantomData;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use crate::interface::{SkiplistIterator, SkiplistLendingIterator};


// TODO: add comments describing the time complexity of various operations.


/// Methods for seeking through a skiplist, used by [`SkiplistIter`] and [`SkiplistLendingIter`] to
/// provide implementations of [`SkiplistIterator`] and [`SkiplistLendingIterator`].
///
/// Implementing this trait is not mandatory; it is intended for avoiding iterator boilerplate.
///
/// # Safety
/// Any returned references to nodes must remain valid at least until the source `Self` value
/// is dropped or invalidated in some way, other than by moving that `Self` value. In particular,
/// references returned by the methods of this crate must be able to be soundly lifetime-extended
/// to `&'a Self::Node<'a>`, provided that the source `Self` value remains valid (aside from being
/// moved) for at least as long as the lifetime `'a`.
pub unsafe trait SkiplistSeek {
    type Node<'a>: SkiplistNode where Self: 'a;

    /// Return the first node in the skiplist, if the skiplist is nonempty.
    ///
    /// This operation should be fast.
    #[must_use]
    fn get_first(&self) -> Option<&Self::Node<'_>>;

    /// Return the last node in the skiplist, if the skiplist is nonempty.
    #[must_use]
    fn find_last(&self) -> Option<&Self::Node<'_>>;

    /// Return the first node whose entry compares greater than or equal to the provided `entry`,
    /// if there is such a node.
    #[must_use]
    fn find_greater_or_equal(&self, entry: &[u8]) -> Option<&Self::Node<'_>>;

    /// Return the last node whose entry compares strictly less than the provided `entry`,
    /// if there is such a node.
    #[must_use]
    fn find_strictly_less(&self, entry: &[u8]) -> Option<&Self::Node<'_>>;
}

/// A node storing an entry of a skiplist, relevant for [`SkiplistSeek`].
///
/// Implementing this trait is not mandatory; it is intended for avoiding iterator boilerplate.
pub trait SkiplistNode {
    /// Get a reference to the following node of the skiplist, if there is one.
    #[must_use]
    fn next_node(&self) -> Option<&Self>;

    /// Get the entry stored in the node.
    #[must_use]
    fn node_entry(&self) -> &[u8];
}

#[derive(Debug)]
pub struct SkiplistIter<'a, List: SkiplistSeek> {
    list:   &'a List,
    cursor: Option<&'a List::Node<'a>>,
}

impl<'a, List: SkiplistSeek> SkiplistIter<'a, List> {
    #[inline]
    pub const fn new(list: &'a List) -> Self {
        Self {
            list,
            cursor: None,
        }
    }
}

impl<List: SkiplistSeek> Clone for SkiplistIter<'_, List> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            list:   self.list,
            cursor: self.cursor,
        }
    }
}

impl<'a, List: SkiplistSeek> Iterator for SkiplistIter<'a, List> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.cursor = if let Some(node) = self.cursor {
            node.next_node()
        } else {
            self.list.get_first()
        };

        self.current()
    }
}

impl<'a, List: SkiplistSeek> SkiplistIterator<'a> for SkiplistIter<'a, List> {
    #[inline]
    fn is_valid(&self) -> bool {
        self.cursor.is_some()
    }

    #[inline]
    fn reset(&mut self) {
        self.cursor = None;
    }

    #[inline]
    fn current(&self) -> Option<&'a [u8]> {
        self.cursor.map(List::Node::node_entry)
    }

    fn prev(&mut self) -> Option<&'a [u8]> {
        self.cursor = if let Some(node) = self.cursor {
            self.list.find_strictly_less(node.node_entry())
        } else {
            self.list.find_last()
        };

        self.current()
    }

    fn seek(&mut self, min_bound: &[u8]) {
        self.cursor = self.list.find_greater_or_equal(min_bound);
    }

    #[inline]
    fn seek_to_first(&mut self) {
        self.cursor = self.list.get_first();
    }

    fn seek_to_end(&mut self) {
        self.cursor = self.list.find_last();
    }
}

#[derive(Debug, Clone)]
pub struct SkiplistLendingIter<List: SkiplistSeek> {
    /// Invariant: after construction of this iter, `self.list` must not be dropped or otherwise
    /// invalidated, except by being moved, until `self` is being dropped or invalidated, except
    /// by being moved.
    ///
    /// (Basically, only call methods on `self.list`, and do not overwrite it.)
    list:   List,
    /// Invariant: if a `&List::Node<'_>` is written to `self.cursor` as a `Some` link,
    /// then that node reference must have been obtained from one of the four `SkiplistSeek`
    /// methods applied to `self.list`.
    cursor: ErasedListLink<List>,
}

impl<List: SkiplistSeek> SkiplistLendingIter<List> {
    #[rustversion::attr(
        since(1.87.0),
        expect(
            clippy::elidable_lifetime_names,
            reason = "Being careful around `unsafe`",
        ),
    )]
    #[rustversion::attr(
        before(1.87.0),
        expect(
            clippy::needless_lifetimes,
            reason = "Being careful around `unsafe`",
        ),
    )]
    #[inline]
    const fn cursor_link<'a>(&'a self) -> Option<&'a List::Node<'a>> {
        // SAFETY:
        // If `self.cursor` encodes a `&List::Node<'_>`, then:
        // - That node reference was obtained from a `SkiplistSeek` method applied to `self.list`
        // - Since then, `self`, and thus also `self.list`, has not been dropped or otherwise
        //   invalidated except by maybe being moved (..since it still exists right now..), and
        //   since it's borrowed for `'a`, that continues to hold true for at least `'a`.
        unsafe { self.cursor.into_link() }
    }
}

impl<List: SkiplistSeek> SkiplistLendingIter<List> {
    #[inline]
    #[must_use]
    pub const fn new(list: List) -> Self {
        Self {
            list,
            cursor: ErasedListLink::new_null(),
        }
    }

    #[inline]
    #[must_use]
    pub fn into_list(self) -> List {
        self.list
    }
}

impl<List: SkiplistSeek> SkiplistLendingIterator for SkiplistLendingIter<List> {
    #[inline]
    fn is_valid(&self) -> bool {
        self.cursor.is_null()
    }

    #[inline]
    fn reset(&mut self) {
        // Invariant remains satisfied; we're writing a `None`, not a `Some`.
        self.cursor = ErasedListLink::new_null();
    }

    #[inline]
    fn next(&mut self) -> Option<&[u8]> {
        let next = if let Some(node) = self.cursor_link() {
            node.next_node()
        } else {
            self.list.get_first()
        };
        self.cursor = ErasedListLink::from_link(next);

        self.current()
    }

    #[inline]
    fn current(&self) -> Option<&[u8]> {
        self.cursor_link().map(List::Node::node_entry)
    }

    fn prev(&mut self) -> Option<&[u8]> {
        let prev = if let Some(node) = self.cursor_link() {
            self.list.find_strictly_less(node.node_entry())
        } else {
            self.list.find_last()
        };
        self.cursor = ErasedListLink::from_link(prev);

        self.current()
    }

    fn seek(&mut self, min_bound: &[u8]) {
        self.cursor = ErasedListLink::from_link(self.list.find_greater_or_equal(min_bound));
    }

    #[inline]
    fn seek_to_first(&mut self) {
        self.cursor = ErasedListLink::from_link(self.list.get_first());
    }

    fn seek_to_end(&mut self) {
        self.cursor = ErasedListLink::from_link(self.list.find_last());
    }
}

/// A lifetime-erased version of `Option<&'_ List::Node<'_>>`.
///
/// This is essentially a `crate::single_threaded::node::erased_node::ErasedNode`,
/// but fewer restrictions are placed on it. That is, there is no mention of `Bump` allocators,
/// or invariants of the `Node` type. (Though "an implementor of `SkiplistNode` can't secretly
/// require unsafe preconditions to use its methods" is perhaps an invariant of some sort.)
///
/// Invariant, enforced by this type and relied on by `unsafe` code:
/// - The wrapped `*const ()` is either a null pointer, or else it was type-erased from a
///   `&'source List::Node<'source>`.
struct ErasedListLink<List: SkiplistSeek>(*const (), PhantomData<List>);

impl<List: SkiplistSeek> ErasedListLink<List> {
    #[inline]
    #[must_use]
    const fn from_link<'a>(link: Option<&'a List::Node<'a>>) -> Self {
        if let Some(node) = link {
            Self::new_erased(node)
        } else {
            Self::new_null()
        }
    }

    #[inline]
    #[must_use]
    const fn new_null() -> Self {
        Self(ptr::null(), PhantomData)
    }

    #[inline]
    #[must_use]
    const fn new_erased<'a>(node: &'a List::Node<'a>) -> Self {
        let node: *const List::Node<'a> = node;
        let node = node.cast::<()>();

        Self(node, PhantomData)
    }

    #[inline]
    const fn is_null(&self) -> bool {
        self.0.is_null()
    }

    /// # Safety
    /// If this `ErasedListLink` was constructed from a `&'a List::Node<'a>` (by using
    /// [`ErasedListLink::from_link`] on a `Some` link or by using [`ErasedListLink::new_erased`]),
    /// then:
    /// - That node reference must have been obtained from one of the four methods of
    ///   the `List: SkiplistSeek` type.
    /// - The source `List` which the node reference came from must outlive the `'a` reference here;
    ///   for at least the length of `'a`, the source `List` must not be dropped or otherwise
    ///   invalidated, except by moving that `List`.
    #[inline]
    #[must_use]
    const unsafe fn into_link<'a>(self) -> Option<&'a List::Node<'a>> {
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
            // was returned by one of the `SkiplistSeek` methods applied to that `List`.
            // By the unsafe contract of `SkiplistSeek`, constructing this `&'a List::Node<'a>`
            // reference is sound.
            let node: &'a List::Node<'a> = unsafe { &*node };
            Some(node)
        }
    }
}

impl<List: SkiplistSeek> Clone for ErasedListLink<List> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<List: SkiplistSeek> Copy for ErasedListLink<List> {}

impl<List: SkiplistSeek> Default for ErasedListLink<List> {
    #[inline]
    fn default() -> Self {
        Self::new_null()
    }
}

impl<List: SkiplistSeek> Debug for ErasedListLink<List> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("ErasedListLink").finish_non_exhaustive()
    }
}
