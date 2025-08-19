mod block_iter_impl;
mod builder;
mod iters;


use std::{borrow::Borrow, convert::Infallible};

use clone_behavior::{ConstantTime, IndependentClone, MirroredClone, Speed};
use generic_container::Container;

use crate::comparator::ComparatorAdapter;


pub use self::builder::BlockBuilder;
pub use self::{
    block_iter_impl::{BlockIterImpl, BlockIterImplPieces},
    iters::{BorrowedBlockIter, OwnedBlockIter, OwnedBlockIterPieces},
};


pub trait BlockContentsContainer:
    'static
        + Borrow<Vec<u8>>
        + for<'a> Container<Vec<u8>, Ref<'a> = &'a Vec<u8>, RefError = Infallible>
{}

impl<C> BlockContentsContainer for C
where
    for<'a> C: 'static
        + Borrow<Vec<u8>>
        + Container<Vec<u8>, Ref<'a> = &'a Vec<u8>, RefError = Infallible>,
{}

/// A [`Block`] whose comparator is an adapted [`TableComparator`].
///
/// [`TableComparator`]: crate::comparator::TableComparator
pub type TableBlock<BlockContents, TableCmp> = Block<BlockContents, ComparatorAdapter<TableCmp>>;

/// A `Block` is an immutable ordered set of key/value entries.
///
/// The structure internally looks like follows:
///
/// A block is a list of `entries`, followed by a list of `restart`s, terminated by `num_restarts`.
///
/// An `entry` consists of three varints, `shared`, `non_shared`, and `value_size`; a `key`;
/// and a `value`.
///
/// - `shared` denotes how many bytes the entry's key shares with the previous one.
/// - `non_shared` is the size of the key minus `shared`.
/// - `value_size` is the size of the value.
/// - `key` and `value` are byte strings; the length of `key` is `non_shared`.
/// - a `restart` is a fixed u32 pointing to the beginning of an `entry`. The key of a restart
///   entry must have `shared` set to `0` (though that does not imply being a restart entry).
///   The very first entry _must_ be a restart. There must not be multiple restarts pointing at the
///   same entry.
/// - `num_restarts` is a fixed u32 indicating the number of restarts.
///
/// The keys must be sorted in the order of the provided `Cmp`, which should implement
/// `Comparator<[u8]>`. The list of `restarts` must likewise be sorted such that a restart is
/// sorted earlier iff the restart entry it refers to is sorted earlier.
///
/// The keys should all compare distinct from each other under the provided `Cmp`; otherwise,
/// seeking can become unpredictable and slightly logically incorrect.
///
/// Note that all these guarantees are satisfied by Google's C++ implementation of LevelDB.
#[derive(Debug, Clone)]
pub struct Block<Contents, Cmp> {
    /// The methods of the [`Block`] struct assume, and do not necessarily validate, that any
    /// provided `contents` are a valid byte representation of a `Block`.
    ///
    /// See the type-level documentation for details of the format, and do not carelessly
    /// modify `contents` or provide invalid data.
    pub contents: Contents,
    pub cmp:      Cmp,
}

impl<Contents, Cmp> Block<Contents, Cmp> {
    #[expect(clippy::missing_const_for_fn, reason = "don't commit to having no validation")]
    #[inline]
    #[must_use]
    pub fn new(contents: Contents, cmp: Cmp) -> Self {
        Self {
            contents,
            cmp,
        }
    }
}

impl<Contents, Cmp> Block<Contents, Cmp>
where
    Contents: Borrow<Vec<u8>>,
{
    /// # Panics
    /// May panic if the `[u8]` slice referred to by `contents` is not a valid byte representation
    /// of a [`Block`].
    #[expect(clippy::iter_not_returning_iterator, reason = "it's a lending iterator")]
    #[inline]
    #[must_use]
    pub fn iter(&self) -> BorrowedBlockIter<'_, Cmp> {
        BorrowedBlockIter::new(self.contents.borrow(), &self.cmp)
    }

    /// # Panics
    /// May panic if the `[u8]` slice referred to by `contents` is not a valid byte representation
    /// of a [`Block`].
    #[expect(clippy::should_implement_trait, reason = "it's IntoIterator but for a lending iter")]
    #[inline]
    #[must_use]
    pub fn into_iter(self) -> OwnedBlockIter<Contents, Cmp> {
        OwnedBlockIter::new(self)
    }
}

impl<Contents, Cmp> Block<Contents, Cmp>
where
    Contents: MirroredClone<ConstantTime> + Borrow<Vec<u8>>,
    Cmp:      MirroredClone<ConstantTime>,
{
    #[inline]
    #[must_use]
    pub fn refcounted_iter(&self) -> OwnedBlockIter<Contents, Cmp> {
        self.mirrored_clone().into_iter()
    }
}

impl<S, Contents, Cmp> IndependentClone<S> for Block<Contents, Cmp>
where
    S:        Speed,
    Contents: IndependentClone<S>,
    Cmp:      IndependentClone<S>,
{
    #[inline]
    fn independent_clone(&self) -> Self {
        Self {
            contents: self.contents.independent_clone(),
            cmp:      self.cmp.independent_clone(),
        }
    }
}

impl<S, Contents, Cmp> MirroredClone<S> for Block<Contents, Cmp>
where
    S:        Speed,
    Contents: MirroredClone<S>,
    Cmp:      MirroredClone<S>,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self {
            contents: self.contents.mirrored_clone(),
            cmp:      self.cmp.mirrored_clone(),
        }
    }
}
