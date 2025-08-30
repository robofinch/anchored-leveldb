#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to perform Polonius-style lifetime extension"),
)]

mod generics;
mod current_iter;


use std::borrow::Borrow as _;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone};
use generic_container::FragileContainer;
use seekable_iterator::{CursorLendingIterator, LendItem, LentItem, Seekable};

use anchored_vfs::traits::RandomAccess;

use crate::{
    caches::TableBlockCache, compressors::CompressorList, filters::FilterPolicy, pool::BufferPool,
};
use crate::{
    block::{BlockIterImpl, BlockIterImplPieces},
    comparator::{ComparatorAdapter, TableComparator},
};
#[cfg(not(feature = "polonius"))]
use crate::block::OwnedBlockIter;
use super::table_struct::Table;
use self::{current_iter::CurrentIter, generics::TableGenerics};


#[derive(Debug)]
pub struct TableIterPieces<PooledBuffer, TableCmp> {
    /// Invariant: `current_iter` must be in pieces
    current_iter: CurrentIter<PooledBuffer, TableCmp>,
    index_iter:   BlockIterImplPieces,
}

impl<PooledBuffer, TableCmp> TableIterPieces<PooledBuffer, TableCmp> {
    #[inline]
    #[must_use]
    pub fn new(cmp: ComparatorAdapter<TableCmp>) -> Self {
        Self {
            current_iter: CurrentIter::new_in_pieces(cmp),
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
        Self::new(ComparatorAdapter(TableCmp::default()))
    }
}

/// Note that entries in a [`Table`] have unique keys, so the keys of this iterator's entries
/// are all distinct.
pub struct TableIter<CompList, Policy, TableCmp, File, Cache, Pool: BufferPool, TableContainer> {
    /// Invariant: the container is Fragile, so `table.get_ref()` may never be called while
    /// another table reference is live.
    ///
    /// Below, the only four places where the method is called do not overlap, and the relevant
    /// functions do not call each other or themselves.
    table:           TableContainer,
    _table_generics: TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>,

    /// Invariants:
    /// - if the `current_iter` is `Initialized`, then it must be `valid()`.
    /// - the `current_iter` should be `Initialized` if and only if `index_iter` is `valid()`.
    ///
    /// If `current_iter` becomes `!valid()`, then a new block iter should be retrieved via
    /// `index_iter`, if possible. If there is no such block iter, it should be converted into
    /// pieces.
    current_iter:    CurrentIter<Pool::PooledBuffer, TableCmp>,
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
    Cache:          TableBlockCache<Pool::PooledBuffer>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
{
    #[must_use]
    pub fn new(table: TableContainer) -> Self {
        let table_ref = table.get_ref();
        let deref_table_ref: &Table<CompList, Policy, TableCmp, File, Cache, Pool> = &table_ref;

        let comparator = table_ref.comparator().mirrored_clone();

        let current_iter = CurrentIter::new_in_pieces(comparator);
        let index_iter = BlockIterImpl::new(deref_table_ref.index_block().contents.borrow());

        drop(table_ref);

        Self {
            table,
            _table_generics: TableGenerics::new(),
            current_iter,
            index_iter,
        }
    }

    #[must_use]
    pub fn from_pieces(
        table:  TableContainer,
        pieces: TableIterPieces<Pool::PooledBuffer, TableCmp>,
    ) -> Self {
        let table_ref = table.get_ref();
        let index_iter = BlockIterImpl::from_pieces(
            table_ref.index_block().contents.borrow(),
            pieces.index_iter,
        );
        drop(table_ref);

        Self {
            table,
            _table_generics: TableGenerics::new(),
            current_iter:    pieces.current_iter,
            index_iter,
        }
    }

    #[must_use]
    pub fn into_pieces(
        mut self,
    ) -> (TableContainer, TableIterPieces<Pool::PooledBuffer, TableCmp>) {
        self.current_iter.convert_to_pieces();

        // We satisfy the `current_iter` invariant by calling `convert_to_pieces()` first.
        let pieces = TableIterPieces {
            current_iter: self.current_iter,
            index_iter:   self.index_iter.into_pieces(),
        };

        (self.table, pieces)
    }

    /// Assuming that `self.current_iter` is either not initialized or not `valid()`, get either the
    /// next entry of the next nonempty block, or the previous entry of the previous nonempty block,
    /// depending on whether `NEXT` is true or false.
    ///
    /// After this call, `self.current_iter` is either not initialized, or is initialized
    /// and `valid()`. Additionally, `self.index_iter` is `valid()` iff `self.current_iter`
    /// is initialized and valid.
    #[expect(clippy::expect_used, reason = "get code functional before handling errors")]
    #[inline(never)]
    fn next_or_prev_fallback<const NEXT: bool>(
        &mut self,
    ) -> Option<LentItem<'_, Self>> {
        let table_ref = self.table.get_ref();
        let index_block_contents = table_ref.index_block().contents.borrow();

        while let Some((_, block_handle)) = self.index_iter.next(index_block_contents) {
            let block_contents = table_ref.read_block_from_encoded_handle(block_handle)
                .expect("TODO: do proper error handling in iterators");

            let initialized = self.current_iter.initialize(block_contents);

            // Unfortunately this is a case where Rust's current NLL borrow checker is overly
            // conservative; the newer, in-progress Polonius borrow checker accepts it.
            // To get this to work on stable Rust requires unsafe code.
            {
                #[cfg(not(feature = "polonius"))]
                let initialized: &mut OwnedBlockIter<_, _> = {
                    let initialized: *mut OwnedBlockIter<_, _> = initialized;

                    // SAFETY:
                    // Because `initialized` came from a `&mut OwnedBlockIter<_, _>`...
                    // - the pointer is properly aligned, non-null, and dereferenceable
                    // - we have not mutated the value via a raw pointer, it's still a valid value
                    // - the aliasing rules are satisfied, as proven by how the code compiles fine
                    //   under Polonius; we don't use the `initialized` reference after the
                    //   if-let block.
                    unsafe { &mut *initialized }
                };

                let entry = if NEXT {
                    initialized.next()
                } else {
                    initialized.prev()
                };

                if entry.is_some() {
                    // In this branch, `self.index_iter` and `self.current_iter` are `valid()`.
                    return entry;
                }

                // TODO: if entry is None, then we **know** that `block_contents` is a corrupted
                // data block and `initialized` encountered a corruption error, since every
                // data block written into an SSTable must be nonempty.
            }
        }

        // In this branch, `self.index_iter` is `!valid()`.
        self.current_iter.convert_to_pieces();
        None
    }

    fn next_or_prev<const NEXT: bool>(&mut self) -> Option<LentItem<'_, Self>> {
        if let Some(initialized) = self.current_iter.get_iter_mut() {
            // Unfortunately this is a case where Rust's current NLL borrow checker is overly
            // conservative; the newer, in-progress Polonius borrow checker accepts it.
            // To get this to work on stable Rust requires unsafe code.
            {
                #[cfg(not(feature = "polonius"))]
                let initialized: &mut OwnedBlockIter<_, _> = {
                    let initialized: *mut OwnedBlockIter<_, _> = initialized;

                    // SAFETY:
                    // Because `initialized` came from a `&mut OwnedBlockIter<_, _>`...
                    // - the pointer is properly aligned, non-null, and dereferenceable
                    // - we have not mutated the value via a raw pointer, it's still a valid value
                    // - the aliasing rules are satisfied, as proven by how the code compiles fine
                    //   under Polonius; we don't use the `initialized` reference after the
                    //   if-let block.
                    unsafe { &mut *initialized }
                };

                let entry = if NEXT {
                    initialized.next()
                } else {
                    initialized.prev()
                };

                if entry.is_some() {
                    // In this branch, `self.current_iter` is `valid()`, and we haven't
                    // touched `self.index_iter`, so that should still be valid.
                    return entry;
                }
            }
        }

        // Either `self.current_iter` is not initialized, or calling `next` or `prev` made it
        // `!valid()`.
        self.next_or_prev_fallback::<NEXT>()
    }

    #[expect(clippy::expect_used, reason = "get code functional before handling errors")]
    fn seek_bound<const GEQ: bool>(&mut self, bound: &[u8]) {
        let table_ref = self.table.get_ref();
        let index_block_contents = table_ref.index_block().contents.borrow();
        let table_comparator = table_ref.comparator();

        if GEQ {
            self.index_iter.seek(index_block_contents, table_comparator, bound);
        } else {
            self.index_iter.seek_before(index_block_contents, table_comparator, bound);
        }

        let mut current_index = self.index_iter.current(index_block_contents);

        while let Some((_, block_handle)) = current_index {
            let block_contents = table_ref.read_block_from_encoded_handle(block_handle)
                .expect("TODO: do proper error handling in iterators");

            let initialized = self.current_iter.initialize(block_contents);

            if GEQ {
                initialized.seek(bound);
            } else {
                initialized.seek_before(bound);
            }

            if initialized.valid() {
                // In this branch, `self.index_iter` and `self.current_iter` are `valid()`.
                return;
            } else {
                current_index = if GEQ {
                    self.index_iter.next(index_block_contents)
                } else {
                    self.index_iter.prev(index_block_contents)
                };
            }
        }

        // In this branch, we seeked too far forwards or backwards;
        // `self.index_iter` is `!valid()`, and we make `self.current_iter` be not initialized.
        self.current_iter.convert_to_pieces();
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
    Policy:         FilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          TableBlockCache<Pool::PooledBuffer>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
{
    fn valid(&self) -> bool {
        // `self.current_iter` is initialized if and only if `self.current_iter`
        // and `self.index_iter` are `valid()`.
        self.current_iter.is_initialized()
    }

    fn next(&mut self) -> Option<LentItem<'_, Self>> {
        self.next_or_prev::<true>()
    }

    fn current(&self) -> Option<LentItem<'_, Self>> {
        #[expect(
            clippy::unwrap_used,
            reason = "invariant of self.current_iter is that it is `valid()` if initialized",
        )]
        self.current_iter
            .get_iter_ref()
            .map(|current_iter| current_iter.current().unwrap())
    }

    fn prev(&mut self) -> Option<LentItem<'_, Self>> {
        self.next_or_prev::<false>()
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
    Cache:          TableBlockCache<Pool::PooledBuffer>,
    Pool:           BufferPool,
    TableContainer: FragileContainer<Table<CompList, Policy, TableCmp, File, Cache, Pool>>,
{
    fn reset(&mut self) {
        // After these calls, `self.current_iter` is not initialized and `self.index_iter`
        // is `!valid()`, so the invariants are satisfied.
        self.current_iter.convert_to_pieces();
        self.index_iter.reset();
    }

    fn seek(&mut self, min_bound: &[u8]) {
        self.seek_bound::<true>(min_bound);
    }

    fn seek_before(&mut self, strict_upper_bound: &[u8]) {
        self.seek_bound::<false>(strict_upper_bound);
    }

    fn seek_to_first(&mut self) {
        self.reset();
        self.next();
    }

    fn seek_to_last(&mut self) {
        self.reset();
        self.prev();
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
            .field("index_iter",      &self.index_iter)
            .finish()
    }
}
