#![expect(unsafe_code, reason = "Re-add Send and Sync impls removed by PhantomData")]
#![expect(unused, reason = "in-progress file")]

use std::{borrow::Borrow as _, marker::PhantomData};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone};
use generic_container::FragileContainer;
use seekable_iterator::{CursorLendingIterator, LendItem, LentItem, Seekable};

use anchored_vfs::traits::RandomAccess;

use crate::{
    cache::TableBlockCache, compressors::CompressorList, filter::FilterPolicy, pool::BufferPool,
};
use crate::{
    block::{BlockIterImpl, BlockIterImplPieces, OwnedBlockIter, OwnedBlockIterPieces},
    comparator::{ComparatorAdapter, TableComparator},
};
use super::table_struct::Table;


struct TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>(
    PhantomData<(CompList, Policy, TableCmp, File, Cache, Pool)>,
);

impl<CompList, Policy, TableCmp, File, Cache, Pool>
    TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{
    #[inline]
    #[must_use]
    const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool> Debug
for TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("TableGenerics").field(&self.0).finish()
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool> Clone
for TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool> Copy
for TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{}

// Safety: we don't actually store any such data in this struct
unsafe impl<CompList, Policy, TableCmp, File, Cache, Pool> Send
for TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{}

// Safety: we don't actually store any such data in this struct
unsafe impl<CompList, Policy, TableCmp, File, Cache, Pool> Sync
for TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{}

#[derive(Debug)]
enum CurrentIter<PooledBuffer, TableCmp> {
    Initialized(OwnedBlockIter<PooledBuffer, ComparatorAdapter<TableCmp>>),
    InPieces {
        /// Need not be a valid block
        block_buffer: PooledBuffer,
        cmp:          ComparatorAdapter<TableCmp>,
        iter:         OwnedBlockIterPieces,
    },
}

#[derive(Debug)]
pub struct TableIterPieces<PooledBuffer, TableCmp> {
    current_iter: CurrentIter<PooledBuffer, TableCmp>,
    index_iter:   BlockIterImplPieces,
}

impl<PooledBuffer, TableCmp> TableIterPieces<PooledBuffer, TableCmp> {
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

impl<PooledBuffer, TableCmp> Default for TableIterPieces<PooledBuffer, TableCmp>
where
    TableCmp:      Default,
{
    #[inline]
    fn default() -> Self {
        Self::new(TableCmp::default())
    }
}

pub struct TableIter<CompList, Policy, TableCmp, File, Cache, Pool: BufferPool, TableContainer> {
    table:           TableContainer,
    _table_generics: TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>,

    current_iter:    CurrentIter<Pool::PooledBuffer, TableCmp>,
    block_offset:    u64,
    /// Invariant: this `index_iter` must only be passed the table's `index_block` contents
    /// and comparator.
    index_iter:      BlockIterImpl,
}

impl<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
    TableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         FilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          TableBlockCache<Pool::PooledBuffer, TableCmp>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
{
    #[inline]
    #[must_use]
    pub fn new(table: TableContainer) -> Self {
        let table_ref = table.get_ref();

        let current_iter = todo!();
        let index_iter = BlockIterImpl::new(table_ref.index_block().contents.borrow());

        drop(table_ref);

        Self {
            table,
            _table_generics: TableGenerics::new(),
            current_iter,
            block_offset:    0,
            index_iter,
        }
    }

    pub fn from_pieces(
        table:      TableContainer,
        mut pieces: TableIterPieces<Pool::PooledBuffer, TableCmp>,
    ) -> Self {
        let table_ref = table.get_ref();
        let index_iter = BlockIterImpl::from_pieces(
            table_ref.index_block().contents.borrow(),
            pieces.index_iter,
        );
        drop(table_ref);

        // Do something with `pieces.current_iter`
        todo!();

        Self {
            table,
            _table_generics: TableGenerics::new(),
            current_iter:    pieces.current_iter,
            block_offset:    0,
            index_iter,
        }
    }

    pub fn into_pieces(self) -> (TableContainer, TableIterPieces<Pool, TableCmp>) {
        todo!()
    }
}

impl<'lend, CompList, Policy, TableCmp, File, Cache, Pool: BufferPool, TableContainer>
    LendItem<'lend>
for TableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
{
    type Item = Result<(&'lend [u8], &'lend [u8]), ()>;
}

impl<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
    CursorLendingIterator
for TableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         FilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          TableBlockCache<Pool::PooledBuffer, TableCmp>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
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

impl<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
    Seekable<[u8], ComparatorAdapter<TableCmp>>
for TableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         FilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          TableBlockCache<Pool::PooledBuffer, TableCmp>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
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

impl<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer> Debug
for TableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    TableCmp:           Debug,
    Pool:               BufferPool,
    Pool::PooledBuffer: Debug,
    TableContainer:     Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        #[expect(clippy::used_underscore_binding, reason = "this is a Debug impl")]
        f.debug_struct("TableIter")
            .field("table",           &self.table)
            .field("_table_generics", &self._table_generics)
            .field("current_iter",    &self.current_iter)
            .field("block_offset",    &self.block_offset)
            .field("index_iter",      &self.index_iter)
            .finish()
    }
}
