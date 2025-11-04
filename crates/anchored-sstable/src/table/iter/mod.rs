mod iter_impl;


use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone};
use generic_container::FragileContainer;
use seekable_iterator::{CursorLendingIterator, LendItem, LentItem, Seekable};

use anchored_vfs::traits::RandomAccess;

use crate::{compressors::CompressorList, filters::TableFilterPolicy, pool::BufferPool};
use crate::{
    caches::{BlockCacheKey, KVCache},
    comparator::{ComparatorAdapter, TableComparator},
};
use super::table_struct::Table;

pub use self::iter_impl::TableIterImpl;


/// Note that entries in a [`Table`] have unique keys, so the keys of this iterator's entries
/// are all distinct.
pub struct TableIter<CompList, Policy, TableCmp, File, Cache, Pool: BufferPool, TableContainer> {
    /// Invariant: the container is Fragile, so `table.get_ref()` may never be called while
    /// another table reference is live.
    ///
    /// Below, each function calls `get_ref` at most once, and they're all one-liners
    /// which do not call each other.
    table: TableContainer,
    iter:  TableIterImpl<CompList, Policy, TableCmp, File, Cache, Pool>,
}

impl<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
    TableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         TableFilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
{
    #[must_use]
    pub fn new(table: TableContainer) -> Self {
        let table_ref = table.get_ref();
        let deref_table_ref: &Table<CompList, Policy, TableCmp, File, Cache, Pool> = &table_ref;

        let iter = TableIterImpl::new(deref_table_ref);

        drop(table_ref);

        Self {
            table,
            iter,
        }
    }
}

impl<'lend, CompList, Policy, TableCmp, File, Cache, Pool: BufferPool, TableContainer>
    LendItem<'lend>
for TableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
{
    type Item = (&'lend [u8], &'lend [u8]);
}

impl<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
    CursorLendingIterator
for TableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         TableFilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
{
    #[inline]
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn next(&mut self) -> Option<LentItem<'_, Self>> {
        self.iter.next(&self.table.get_ref())
    }

    #[inline]
    fn current(&self) -> Option<LentItem<'_, Self>> {
        self.iter.current()
    }

    fn prev(&mut self) -> Option<LentItem<'_, Self>> {
        self.iter.prev(&self.table.get_ref())
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
    Seekable<[u8], ComparatorAdapter<TableCmp>>
for TableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         TableFilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
{
    fn reset(&mut self) {
        self.iter.reset();
    }

    fn seek(&mut self, min_bound: &[u8]) {
        self.iter.seek(&self.table.get_ref(), min_bound);
    }

    fn seek_before(&mut self, strict_upper_bound: &[u8]) {
        self.iter.seek_before(&self.table.get_ref(), strict_upper_bound);
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first(&self.table.get_ref());
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last(&self.table.get_ref());
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
        f.debug_struct("TableIter")
            .field("table", &self.table)
            .field("iter",  &self.iter)
            .finish()
    }
}

/// Note that entries in a [`Table`] have unique keys, so the keys of this iterator's entries
/// are all distinct.
pub struct OptionalTableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    Pool: BufferPool,
{
    /// Invariant: the container is Fragile, so `table.get_ref()` may never be called while
    /// another table reference is live.
    ///
    /// Below, the only four places where the method is called do not overlap, and the relevant
    /// functions do not call each other or themselves.
    table: Option<TableContainer>,
    iter:  TableIterImpl<CompList, Policy, TableCmp, File, Cache, Pool>,
}

impl<CompList, Policy, TableCmp, File, Cache, Pool: BufferPool, TableContainer>
    OptionalTableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
{
    #[must_use]
    pub const fn new_empty(cmp: TableCmp) -> Self {
        Self {
            table: None,
            iter:  TableIterImpl::new_empty(ComparatorAdapter(cmp)),
        }
    }

    #[must_use]
    pub const fn is_set(&self) -> bool {
        self.table.is_some()
    }

    pub fn clear(&mut self) {
        self.table = None;
        self.iter.clear();
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
    OptionalTableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         TableFilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
{
    pub fn set(&mut self, table: TableContainer)  {
        let table_ref = table.get_ref();
        let deref_table_ref: &Table<CompList, Policy, TableCmp, File, Cache, Pool> = &table_ref;

        self.iter.set(deref_table_ref);

        drop(table_ref);

        self.table = Some(table);
    }
}

impl<'lend, CompList, Policy, TableCmp, File, Cache, Pool: BufferPool, TableContainer>
    LendItem<'lend>
for OptionalTableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
{
    type Item = (&'lend [u8], &'lend [u8]);
}

impl<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
    CursorLendingIterator
for OptionalTableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         TableFilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
{
    #[inline]
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn next(&mut self) -> Option<LentItem<'_, Self>> {
        self.iter.next(&self.table.as_ref()?.get_ref())
    }

    #[inline]
    fn current(&self) -> Option<LentItem<'_, Self>> {
        self.iter.current()
    }

    fn prev(&mut self) -> Option<LentItem<'_, Self>> {
        self.iter.prev(&self.table.as_ref()?.get_ref())
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
    Seekable<[u8], ComparatorAdapter<TableCmp>>
for OptionalTableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         TableFilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
{
    fn reset(&mut self) {
        self.iter.reset();
    }

    fn seek(&mut self, min_bound: &[u8]) {
        if let Some(table) = self.table.as_ref() {
            self.iter.seek(&table.get_ref(), min_bound);
        }
    }

    fn seek_before(&mut self, strict_upper_bound: &[u8]) {
        if let Some(table) = self.table.as_ref() {
            self.iter.seek_before(&table.get_ref(), strict_upper_bound);
        }
    }

    fn seek_to_first(&mut self) {
        if let Some(table) = self.table.as_ref() {
            self.iter.seek_to_first(&table.get_ref());
        }
    }

    fn seek_to_last(&mut self) {
        if let Some(table) = self.table.as_ref() {
            self.iter.seek_to_last(&table.get_ref());
        }
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer> Debug
for OptionalTableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
where
    TableCmp:           Debug,
    Pool:               BufferPool,
    Pool::PooledBuffer: Debug,
    TableContainer:     Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("OptionalTableIter")
            .field("table", &self.table)
            .field("iter",  &self.iter)
            .finish()
    }
}
