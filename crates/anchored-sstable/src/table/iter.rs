#![expect(unsafe_code, reason = "Re-add Send and Sync impls removed by PhantomData")]
#![expect(unused, reason = "in-progress file")]

use std::{borrow::Borrow, marker::PhantomData};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone};
use generic_container::FragileContainer;
use seekable_iterator::{CursorLendingIterator, LendItem, LentItem, Seekable};

use anchored_vfs::traits::RandomAccess;

use crate::{compressors::CompressorList, filter::FilterPolicy};
use crate::{
    block::{
        BlockContentsContainer, BlockIterImpl, BlockIterImplPieces,
        OwnedBlockIter, OwnedBlockIterPieces,
    },
    comparator::{ComparatorAdapter, TableComparator},
};
use super::{cache::TableBlockCache, table_struct::Table};


#[derive(Debug)]
enum CurrentIter<BlockContents, TableCmp> {
    Initialized(OwnedBlockIter<BlockContents, ComparatorAdapter<TableCmp>>),
    InPieces {
        /// Need not be a valid block
        block_buffer: BlockContents,
        cmp:          ComparatorAdapter<TableCmp>,
        iter:         OwnedBlockIterPieces,
    },
}

#[derive(Debug)]
pub struct TableIterPieces<BlockContents, TableCmp> {
    current_iter: CurrentIter<BlockContents, TableCmp>,
    index_iter:   BlockIterImplPieces,
}

impl<BlockContents, TableCmp> TableIterPieces<BlockContents, TableCmp>
where
    BlockContents: BlockContentsContainer,
{
    #[expect(clippy::needless_pass_by_value, reason = "will be needed by value")]
    #[inline]
    #[must_use]
    pub fn new(cmp: TableCmp) -> Self {
        Self {
            current_iter: todo!(),
            index_iter:   BlockIterImplPieces::new(),
        }
    }
}

impl<BlockContents, TableCmp> Default for TableIterPieces<BlockContents, TableCmp>
where
    BlockContents: BlockContentsContainer,
    TableCmp:      Default,
{
    #[inline]
    fn default() -> Self {
        Self::new(TableCmp::default())
    }
}

pub struct TableIter<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer> {
    table:           TableContainer,
    _table_generics: PhantomData<(CompList, Policy, TableCmp, File, Cache, BlockContents)>,

    current_iter:    CurrentIter<BlockContents, TableCmp>,
    block_offset:    u64,
    /// Invariant: this `index_iter` must only be passed the table's `index_block` contents
    /// and comparator.
    index_iter:      BlockIterImpl,
}

impl<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
    TableIter<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         FilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          TableBlockCache<BlockContents, TableCmp>,
    BlockContents:  BlockContentsContainer,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, BlockContents>>,
{
    #[inline]
    #[must_use]
    pub fn new(table: TableContainer) -> Self {
        let table_ref = table.get_ref();

        let current_iter = todo!();
        let index_iter = BlockIterImpl::new(&table_ref.index_block().contents);

        drop(table_ref);

        Self {
            table,
            _table_generics: PhantomData,
            current_iter,
            block_offset:    0,
            index_iter,
        }
    }

    pub fn from_pieces(
        table:      TableContainer,
        mut pieces: TableIterPieces<BlockContents, TableCmp>,
    ) -> Self {
        let table_ref = table.get_ref();
        let index_iter = BlockIterImpl::from_pieces(
            &table_ref.index_block().contents,
            pieces.index_iter,
        );
        drop(table_ref);

        // Do something with `pieces.current_iter`
        todo!();

        Self {
            table,
            _table_generics: PhantomData,
            current_iter:    pieces.current_iter,
            block_offset:    0,
            index_iter,
        }
    }

    pub fn into_pieces(self) -> (TableContainer, TableIterPieces<BlockContents, TableCmp>) {
        todo!()
    }
}

impl<'lend, CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
    LendItem<'lend>
for TableIter<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
{
    type Item = Result<(&'lend [u8], &'lend [u8]), ()>;
}

impl<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
    CursorLendingIterator
for TableIter<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         FilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          TableBlockCache<BlockContents, TableCmp>,
    BlockContents:  Borrow<Vec<u8>>,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, BlockContents>>,
{
    fn valid(&self) -> bool {
        todo!()
    }

    fn next(&mut self) -> Option<LentItem<'_, Self>> {
        todo!()
    }

    fn current(&self) -> Option<LentItem<'_, Self>> {
        todo!()
    }

    fn prev(&mut self) -> Option<LentItem<'_, Self>> {
        todo!()
    }
}

impl<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
    Seekable<[u8], ComparatorAdapter<TableCmp>>
for TableIter<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         FilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          TableBlockCache<BlockContents, TableCmp>,
    BlockContents:  Borrow<Vec<u8>>,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, BlockContents>>,
{
    fn reset(&mut self) {
        todo!()
    }

    fn seek(&mut self, min_bound: &[u8]) {
        todo!()
    }

    fn seek_before(&mut self, strict_upper_bound: &[u8]) {
        todo!()
    }

    fn seek_to_first(&mut self) {
        todo!()
    }

    fn seek_to_last(&mut self) {
        todo!()
    }
}

impl<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer> Debug
for TableIter<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
where
    TableCmp:       Debug,
    BlockContents:  Debug,
    TableContainer: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("TableIter")
            .field("table",           &self.table)
            .field("_table_generics", &self._table_generics)
            .field("current_iter",    &self.current_iter)
            .field("block_offset",    &self.block_offset)
            .field("index_iter",      &self.index_iter)
            .finish()
    }
}

// Safety: we only actually store a `TableContainer`, types which are `Send`, and
// an `OwnedBlockIter` which is `Send` if `BlockContents` and `TableCmp` are.
unsafe impl<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer> Send
for TableIter<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
where
    TableCmp:       Send,
    BlockContents:  Send,
    TableContainer: Send,
{}

// Safety: we only actually store a `TableContainer`, types which are `Sync`, and
// an `CurrentIter` which is `Sync` if `BlockContents` and `TableCmp` are.
unsafe impl<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer> Sync
for TableIter<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
where
    TableCmp:       Sync,
    BlockContents:  Sync,
    TableContainer: Sync,
{}
