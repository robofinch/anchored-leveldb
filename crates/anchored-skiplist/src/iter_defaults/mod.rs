#![expect(unsafe_code, reason = "allow lifetime extension of list-allocated nodes")]

mod erased;
mod macros;


use clone_behavior::{DeepClone, Speed};
use seekable_iterator::{Comparator, CursorIterator, CursorLendingIterator, LendItem, Seekable};

use self::erased::ErasedListLink;


// TODO: add comments describing the time complexity of various operations.


/// Methods for seeking through a skiplist, used by [`SkiplistIter`] and [`SkiplistLendingIter`] to
/// provide implementations of [`SeekableIterator`] and [`SeekableLendingIterator`].
///
/// Entries should be compared using the comparator type indicated by the generic.
///
/// Implementing this trait is not mandatory; it is intended for use by implementors to simplify
/// how a skiplist's iterators are implemented.
///
/// # Safety
/// Any returned references to nodes (via one of the four `SkiplistSeek` methods, or via the
/// [`SkiplistNode::next_node`] method recursively applied to such a reference) must remain valid at
/// least until the source `self` value, and any reference-counted clones associated with `self`,
/// are dropped or invalidated in some way, other than by moving the values.
///
/// That is, node references returned by the methods of this trait must be able to be soundly
/// lifetime-extended to `&'a Self::Node<'a>`, provided that the source `self` value (or a
/// reference-counted clone associated with `self`) remains valid (aside from being moved), starting
/// from when the node reference was obtained, up to at least as long as the lifetime `'a`.
///
/// Note that the interface of `SkiplistNode` does not require `unsafe`, so, in principal,
/// a node obtained from `SkiplistSeek` could be kept around, lifetime extended, and then have
/// `next_node` recursively applied to it. Therefore, only the four `SkiplistSeek` methods are
/// truly impacted by this unsafe contract.
///
/// [`SeekableIterator`]: seekable_iterator::SeekableIterator
/// [`SeekableLendingIterator`]: seekable_iterator::SeekableLendingIterator
pub unsafe trait SkiplistSeek {
    type Node<'a>: SkiplistNode where Self: 'a;
    type Cmp:      Comparator<[u8]>;

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
/// Implementing this trait is not mandatory; it is intended for use by implementors to simplify
/// how a skiplist's iterators are implemented.
pub trait SkiplistNode {
    /// Get a reference to the following node of the skiplist, if there is one.
    #[must_use]
    fn next_node(&self) -> Option<&Self>;

    /// Get the entry stored in the node.
    #[must_use]
    fn node_entry(&self) -> &[u8];
}

/// A type intended for use by implementors of [`Skiplist`] to reduce the boilerplate needed
/// to create an iterator for [`Skiplist::Iter`].
///
/// See also [`skiplistiter_wrapper`], which can create wrapper structs around this type, for the
/// sake of not exposing this type (or the `List`) in your public API.
///
/// # Safety of lifetime extension
/// The returned entry references remain valid until the `List` containing the entry
/// is dropped or otherwise invalidated, aside from by being moved.
///
/// If there are reference-counted clones associated with the `List`, then a slightly stronger
/// condition holds: the returned entry references remain valid until *all* the reference-counted
/// clones associated with the `List` are dropped or otherwise invalidated, aside from by being
/// moved.
///
/// The returned entry references may thus be lifetime-extended, provided that the `List` value
/// (or at least one of its associated reference-counted clones, possibly trading off which
/// clones are valid at a given point in time) remains valid as described above for
/// at least the length of the extended lifetime.
///
/// In particular, these assurances apply to [`Iterator`] methods, [`SkiplistIter::current`], and
/// [`SkiplistIter::prev`].
///
/// Extending the lifetime of the `SkiplistIter` itself is *not* covered by the above guarantees,
/// and may be unsound.
///
/// [`Skiplist`]: crate::interface::Skiplist
/// [`Skiplist::Iter`]: crate::interface::Skiplist::Iter
/// [`skiplistiter_wrapper`]: crate::skiplistiter_wrapper
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

    #[inline]
    fn fold<B, F>(mut self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        // Having this branch once instead of `n` times seems slightly better than the default
        // `fold` implementation.
        if self.cursor.is_none() {
            self.cursor = self.list.get_first();
        }

        let mut accumulator = init;
        while let Some(node) = self.cursor {
            self.cursor = node.next_node();
            accumulator = f(accumulator, node.node_entry());
        }

        accumulator
    }
}

impl<'a, List: SkiplistSeek> CursorIterator for SkiplistIter<'a, List> {
    #[inline]
    fn valid(&self) -> bool {
        self.cursor.is_some()
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
}

impl<List: SkiplistSeek> Seekable<[u8], List::Cmp> for SkiplistIter<'_, List> {
    #[inline]
    fn reset(&mut self) {
        self.cursor = None;
    }

    fn seek(&mut self, min_bound: &[u8]) {
        self.cursor = self.list.find_greater_or_equal(min_bound);
    }

    fn seek_before(&mut self, strict_upper_bound: &[u8]) {
        self.cursor = self.list.find_strictly_less(strict_upper_bound);
    }

    #[inline]
    fn seek_to_first(&mut self) {
        self.cursor = self.list.get_first();
    }

    fn seek_to_last(&mut self) {
        self.cursor = self.list.find_last();
    }
}

/// A type intended for use by implementors of [`Skiplist`] to reduce the boilerplate needed
/// to create a lending iterator for [`Skiplist::LendingIter`].
///
/// See also [`skiplistlendingiter_wrapper`], which can create wrapper structs around this type,
/// for the sake of not exposing this type (or the `List`) in your public API.
///
/// # Safety of lifetime extension
/// The returned entry references remain valid until the `List` containing the entry
/// is dropped or otherwise invalidated, aside from by being moved. (Neither
/// [`SkiplistLendingIter::new`] nor [`SkiplistLendingIter::into_list`] invalidate the
/// backing storage; they only move the `List`. Likewise, moving the `SkiplistLendingIter`
/// is fine.)
///
/// If there are reference-counted clones associated with the `List`, then a slightly stronger
/// condition holds: the returned entry references remain valid until *all* the reference-counted
/// clones associated with the `List` are dropped or otherwise invalidated, aside from by being
/// moved.
///
/// The returned entry references may thus be lifetime-extended, provided that the `List` value
/// (or at least one of its associated reference-counted clones, possibly trading off which
/// clones are valid at a given point in time) remains valid as described above for
/// at least the length of the extended lifetime.
///
/// In particular, these assurances apply to [`next`], [`current`], and [`prev`].
///
/// [`Skiplist`]: crate::interface::Skiplist
/// [`Skiplist::LendingIter`]: crate::interface::Skiplist::LendingIter
/// [`skiplistlendingiter_wrapper`]: crate::skiplistlendingiter_wrapper
/// [`next`]: Self::next
/// [`current`]: Self::current
/// [`prev`]: Self::prev
#[derive(Debug, Clone)]
pub struct SkiplistLendingIter<List> {
    /// Invariant: after construction of this iter, `self.list` must not be dropped or otherwise
    /// invalidated, except by being moved, until `self` is being dropped or invalidated, except
    /// by being moved.
    ///
    /// (Basically, only call methods on `self.list`, and do not overwrite it.)
    list:   List,
    /// Invariant: if a `&List::Node<'_>` is written to `self.cursor` as a `Some` link,
    /// then that node reference must have been obtained from one of the four `SkiplistSeek`
    /// methods applied to `self.list`, or from `SkiplistNode::next_node` applied to such a node.
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

impl<List> SkiplistLendingIter<List> {
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

impl<S: Speed, List: DeepClone<S>> DeepClone<S> for SkiplistLendingIter<List> {
    #[inline]
    fn deep_clone(&self) -> Self {
        Self {
            list:   self.list.deep_clone(),
            cursor: self.cursor,
        }
    }
}

impl<'lend, List> LendItem<'lend> for SkiplistLendingIter<List> {
    type Item = &'lend [u8];
}

impl<List: SkiplistSeek> CursorLendingIterator for SkiplistLendingIter<List> {
    #[inline]
    fn valid(&self) -> bool {
        !self.cursor.is_null()
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
}

impl<List: SkiplistSeek> Seekable<[u8], List::Cmp> for SkiplistLendingIter<List> {
    #[inline]
    fn reset(&mut self) {
        // Invariant remains satisfied; we're writing a `None`, not a `Some`.
        self.cursor = ErasedListLink::new_null();
    }

    fn seek(&mut self, min_bound: &[u8]) {
        self.cursor = ErasedListLink::from_link(self.list.find_greater_or_equal(min_bound));
    }

    fn seek_before(&mut self, strict_upper_bound: &[u8]) {
        self.cursor = ErasedListLink::from_link(self.list.find_strictly_less(strict_upper_bound));
    }

    #[inline]
    fn seek_to_first(&mut self) {
        self.cursor = ErasedListLink::from_link(self.list.get_first());
    }

    fn seek_to_last(&mut self) {
        self.cursor = ErasedListLink::from_link(self.list.find_last());
    }
}


// Note: since this crate internally uses `SkiplistIter` and `SkiplistLendingIter` to implement
// its iterators, tests on those types are de-facto tests for these adapters as well.
// Dedicated tests for the adapters don't make sense when the other iterators just *are*
// these adapters, inside newtypes.
