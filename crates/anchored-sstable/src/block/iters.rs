use std::borrow::Borrow;

use seekable_iterator::{Comparator, CursorLendingIterator, LendItem, LentItem, Seekable};

use super::Block;
use super::block_iter_impl::{BlockIterImpl, BlockIterImplPieces};



#[derive(Debug)]
pub struct BorrowedBlockIter<'a, Cmp> {
    block: &'a [u8],
    cmp:   &'a Cmp,
    iter:  BlockIterImpl,
}

impl<'a, Cmp> BorrowedBlockIter<'a, Cmp> {
    #[inline]
    #[must_use]
    pub fn new(block: &'a [u8], cmp: &'a Cmp) -> Self {
        Self {
            block,
            cmp,
            iter: BlockIterImpl::new(block),
        }
    }

    /// Reuse the `BorrowedBlockIter` on a new block, resetting almost everything, but keeping
    /// buffers' capacities.
    pub fn reuse_as_new(&mut self, block: &'a [u8]) {
        self.iter.reuse_as_new(block);
    }
}

impl<'a, Cmp> BorrowedBlockIter<'a, Cmp> {
    #[inline]
    #[must_use]
    pub const fn block_ref(&self) -> &'a [u8] {
        self.block
    }
}

impl<'a, 'lend, Cmp> LendItem<'lend> for BorrowedBlockIter<'a, Cmp> {
    /// For a `BorrowedBlockIter`, the key references are invalidated by dropping
    /// or mutably accessing the iterator.
    ///
    /// However, the value references are valid for as long as the source `Block` reference
    /// is valid.
    type Item = (&'lend [u8], &'a [u8]);
}

impl<Cmp> CursorLendingIterator for BorrowedBlockIter<'_, Cmp> {
    #[inline]
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    #[inline]
    fn next(&mut self) -> Option<LentItem<'_, Self>> {
        self.iter.next(self.block)
    }

    #[inline]
    fn current(&self) -> Option<LentItem<'_, Self>> {
        self.iter.current(self.block)
    }

    /// Move the iterator one position back, and return the entry at that position.
    /// Returns `None` if the iterator was at the first entry.
    ///
    /// # Speed
    /// This operation is slower than `self.next()`. If possible, this method should not be used.
    fn prev(&mut self) -> Option<LentItem<'_, Self>> {
        self.iter.prev(self.block)
    }
}

impl<Cmp: Comparator<[u8]>> Seekable<[u8], Cmp> for BorrowedBlockIter<'_, Cmp> {
    #[inline]
    fn reset(&mut self) {
        self.iter.reset();
    }

    #[inline]
    fn seek(&mut self, min_bound: &[u8]) {
        // Note that the block was sorted in the order of this `Cmp`
        // (if the constructed block was valid).
        self.iter.seek(self.block, self.cmp, min_bound);
    }

    /// Move the iterator to the greatest key which is strictly less than the provided
    /// `strict_upper_bound`.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the collection).
    ///
    /// # Speed
    /// This operation uses `self.prev()`, and is thus somewhat inefficient. If possible,
    /// this method should be avoided in favor of `self.seek()`.
    ///
    /// # Correctness
    /// It is required for logical correctness that the block's keys were sorted in the given
    /// comparator's order, and that no two keys compare equal to each other.
    /// The latter constraint holds true of any valid `Block`.
    fn seek_before(&mut self, strict_upper_bound: &[u8]) {
        // Note that the block was sorted in the order of this `Cmp`
        // (if the constructed block was valid).
        self.iter.seek_before(self.block, self.cmp, strict_upper_bound);
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first(self.block);
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last(self.block);
    }
}

#[derive(Default, Debug)]
pub struct OwnedBlockIterPieces {
    iter: BlockIterImplPieces
}

impl OwnedBlockIterPieces {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug)]
pub struct OwnedBlockIter<BlockContents, Cmp> {
    block: Block<BlockContents, Cmp>,
    iter:  BlockIterImpl,
}

impl<BlockContents, Cmp> OwnedBlockIter<BlockContents, Cmp>
where
    BlockContents: Borrow<Vec<u8>>,
{
    #[inline]
    #[must_use]
    pub fn new(block: Block<BlockContents, Cmp>) -> Self {
        let iter = BlockIterImpl::new(block.contents.borrow());
        Self {
            block,
            iter,
        }
    }

    #[inline]
    #[must_use]
    pub fn from_pieces(
        block:  Block<BlockContents, Cmp>,
        pieces: OwnedBlockIterPieces,
    ) -> Self {
        let iter = BlockIterImpl::from_pieces(block.contents.borrow(), pieces.iter);
        Self {
            block,
            iter,
        }
    }
}

impl<BlockContents, Cmp> OwnedBlockIter<BlockContents, Cmp> {
    #[inline]
    #[must_use]
    pub const fn block(&self) -> &Block<BlockContents, Cmp> {
        &self.block
    }

    #[inline]
    #[must_use]
    pub fn into_pieces(self) -> (Block<BlockContents, Cmp>, OwnedBlockIterPieces) {
        let pieces = OwnedBlockIterPieces {
            iter: self.iter.into_pieces(),
        };
        (self.block, pieces)
    }
}

impl<'lend, BlockContents, Cmp> LendItem<'lend> for OwnedBlockIter<BlockContents, Cmp> {
    /// For an `OwnedBlockIter`, both the key and value references are invalidated by dropping
    /// or mutably accessing the iterator.
    type Item = (&'lend [u8], &'lend [u8]);
}

impl<BlockContents: Borrow<Vec<u8>>, Cmp> CursorLendingIterator
for OwnedBlockIter<BlockContents, Cmp>
{
    #[inline]
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    #[inline]
    fn next(&mut self) -> Option<LentItem<'_, Self>> {
        self.iter.next(self.block.contents.borrow())
    }

    #[inline]
    fn current(&self) -> Option<LentItem<'_, Self>> {
        self.iter.current(self.block.contents.borrow())
    }

    /// Move the iterator one position back, and return the entry at that position.
    /// Returns `None` if the iterator was at the first entry.
    ///
    /// # Speed
    /// This operation is slower than `self.next()`. If possible, this method should not be used.
    fn prev(&mut self) -> Option<LentItem<'_, Self>> {
        self.iter.prev(self.block.contents.borrow())
    }
}

impl<BlockContents, Cmp> Seekable<[u8], Cmp> for OwnedBlockIter<BlockContents, Cmp>
where
    BlockContents: Borrow<Vec<u8>>,
    Cmp:           Comparator<[u8]>,
{
    #[inline]
    fn reset(&mut self) {
        self.iter.reset();
    }

    #[inline]
    fn seek(&mut self, min_bound: &[u8]) {
        self.iter.seek(self.block.contents.borrow(), &self.block.cmp, min_bound);
    }

    /// Move the iterator to the greatest key which is strictly less than the provided
    /// `strict_upper_bound`.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the collection).
    ///
    /// # Speed
    /// This operation uses `self.prev()`, and is thus somewhat inefficient. If possible,
    /// this method should be avoided in favor of `self.seek()`.
    ///
    /// # Correctness
    /// It is required for logical correctness that the block's keys were sorted in the given
    /// comparator's order, and that no two keys compare equal to each other.
    /// The latter constraint holds true of any valid `Block`.
    fn seek_before(&mut self, strict_upper_bound: &[u8]) {
        self.iter.seek_before(self.block.contents.borrow(), &self.block.cmp, strict_upper_bound);
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first(self.block.contents.borrow());
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last(self.block.contents.borrow());
    }
}

// TODO: PooledBlockIter using Yoke and BorrowedBlockIter
