#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to perform Polonius-style lifetime extension"),
)]

#[cfg(not(feature = "polonius"))]
use std::slice;
use std::{borrow::Borrow as _, marker::PhantomData};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone};
use generic_container::FragileContainer;
use seekable_iterator::{CursorLendingIterator as _, LendItem, LentItem, Seekable as _};

use anchored_vfs::traits::RandomAccess;

use crate::iter::OptionalBlockIter;
use crate::{compressors::CompressorList, filters::TableFilterPolicy, pool::BufferPool};
use crate::{
    block::BlockIterImpl,
    caches::{BlockCacheKey, KVCache},
    comparator::{ComparatorAdapter, TableComparator},
};
use super::super::table_struct::Table;


/// Note that entries in a [`Table`] have unique keys, so the keys of this iterator's entries
/// are all distinct.
#[expect(clippy::type_complexity, reason = "Only triggered by PhantomData")]
pub struct TableIterImpl<CompList, Policy, TableCmp, File, Cache, Pool: BufferPool> {
    /// Invariants:
    /// - if the `current_iter` currently references a block, then the iter must be `valid()`.
    /// - the `current_iter` should have a block if and only if `index_iter` is `valid()`.
    ///
    /// If `current_iter` becomes `!valid()`, then a new block iter should be retrieved via
    /// `index_iter`, if possible. If there is no such block iter, it should be cleared.
    current_iter:    OptionalBlockIter<Pool::PooledBuffer, ComparatorAdapter<TableCmp>>,
    /// Invariant: this `index_iter` must only be passed the table's `index_block` contents
    /// and comparator.
    index_iter:      BlockIterImpl,
    _table_generics: PhantomData<fn() -> (CompList, Policy, TableCmp, File, Cache, Pool)>,
}

impl<CompList, Policy, TableCmp, File, Cache, Pool: BufferPool>
    TableIterImpl<CompList, Policy, TableCmp, File, Cache, Pool>
{
    #[must_use]
    pub const fn new_empty(cmp: ComparatorAdapter<TableCmp>) -> Self {
        Self {
            current_iter:    OptionalBlockIter::new(cmp),
            index_iter:      BlockIterImpl::new_empty(),
            _table_generics: PhantomData,
        }
    }

    pub fn clear(&mut self) {
        self.current_iter.clear();
        self.index_iter.clear();
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool>
    TableIterImpl<CompList, Policy, TableCmp, File, Cache, Pool>
where
    CompList: FragileContainer<CompressorList>,
    Policy:   TableFilterPolicy,
    TableCmp:      TableComparator + MirroredClone<ConstantTime>,
    File:     RandomAccess,
    Cache:    KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:     BufferPool,
{
    #[must_use]
    pub fn new(table: &Table<CompList, Policy, TableCmp, File, Cache, Pool>) -> Self {
        let comparator = table.comparator().mirrored_clone();
        let index_iter = BlockIterImpl::new(table.index_block().contents.borrow());

        Self {
            current_iter:    OptionalBlockIter::new(comparator),
            index_iter,
            _table_generics: PhantomData,
        }
    }

    pub fn set(
        &mut self,
        table: &Table<CompList, Policy, TableCmp, File, Cache, Pool>,
    ) {
        self.current_iter.clear();
        self.index_iter.set(table.index_block().contents.borrow());
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
        table: &Table<CompList, Policy, TableCmp, File, Cache, Pool>,
    ) -> Option<LentItem<'_, Self>> {
        let index_block_contents = table.index_block().contents.borrow();

        while let Some((_, block_handle)) = self.index_iter.next(index_block_contents) {
            let block_contents = table.read_block_from_encoded_handle(block_handle)
                .expect("TODO: do proper error handling in iterators");

            self.current_iter.set(block_contents);
            let entry = if NEXT {
                self.current_iter.next()
            } else {
                self.current_iter.prev()
            };

            if let Some((key, value)) = entry {
                // In this branch, `self.index_iter` and `self.current_iter` are `valid()`.

                // Unfortunately this is a case where Rust's current NLL borrow checker is overly
                // conservative; the newer, in-progress Polonius borrow checker accepts it.
                // To get this to work on stable Rust requires unsafe code.

                // SAFETY: `key.as_ptr()` is non-null, properly aligned, valid for reads of
                // `key.len()` bytes, points to `key.len()`-many valid bytes, and doesn't have
                // too long of a length, since it came from a valid slice.
                // The sole remaining constraint is the lifetime. The returned references are valid
                // for as long as `self.current_iter` is borrowed, which is as long as `self`
                // is borrowed, which is the `'_` lifetime to which we are extending these
                // lifetimes.
                // Further, the code compiles under Polonius, so it's sound.
                #[cfg(not(feature = "polonius"))]
                let key: &[u8] = unsafe { slice::from_raw_parts(key.as_ptr(), key.len()) };
                // SAFETY: same as the line above.
                #[cfg(not(feature = "polonius"))]
                let value: &[u8] = unsafe { slice::from_raw_parts(value.as_ptr(), value.len()) };

                return Some((key, value));
            }

            // TODO: if entry is None, then we **know** that `block_contents` is a
            // corrupted data block and `initialized` encountered a corruption error, since every
            // data block written into an SSTable must be nonempty.
        }

        // In this branch, `self.index_iter` is `!valid()`.
        self.current_iter.clear();
        None
    }

    fn next_or_prev<const NEXT: bool>(
        &mut self,
        table: &Table<CompList, Policy, TableCmp, File, Cache, Pool>,
    ) -> Option<LentItem<'_, Self>> {
        if self.current_iter.is_set() {
            let entry = if NEXT {
                self.current_iter.next()
            } else {
                self.current_iter.prev()
            };

            if let Some((key, value)) = entry {
                // In this branch, `self.current_iter` is `valid()`, and we haven't
                // touched `self.index_iter`, so that should still be valid.

                // Unfortunately this is a case where Rust's current NLL borrow checker is overly
                // conservative; the newer, in-progress Polonius borrow checker accepts it.
                // To get this to work on stable Rust requires unsafe code.

                // SAFETY: `key.as_ptr()` is non-null, properly aligned, valid for reads of
                // `key.len()` bytes, points to `key.len()`-many valid bytes, and doesn't have
                // too long of a length, since it came from a valid slice.
                // The sole remaining constraint is the lifetime. The returned references are valid
                // for as long as `self.current_iter` is borrowed, which is as long as `self`
                // is borrowed, which is the `'_` lifetime to which we are extending these
                // lifetimes.
                // Further, the code compiles under Polonius, so it's sound.
                #[cfg(not(feature = "polonius"))]
                let key: &[u8] = unsafe { slice::from_raw_parts(key.as_ptr(), key.len()) };
                // SAFETY: same as the line above.
                #[cfg(not(feature = "polonius"))]
                let value: &[u8] = unsafe { slice::from_raw_parts(value.as_ptr(), value.len()) };

                return Some((key, value));
            }
        }

        // Either `self.current_iter` is not initialized, or calling `next` or `prev` made it
        // `!valid()`.
        self.next_or_prev_fallback::<NEXT>(table)
    }

    #[expect(clippy::expect_used, reason = "get code functional before handling errors")]
    fn seek_bound<const GEQ: bool>(
        &mut self,
        table: &Table<CompList, Policy, TableCmp, File, Cache, Pool>,
        bound: &[u8],
    ) {
        let index_block_contents = table.index_block().contents.borrow();
        let table_comparator = table.comparator();

        if GEQ {
            self.index_iter.seek(index_block_contents, table_comparator, bound);
        } else {
            self.index_iter.seek_before(index_block_contents, table_comparator, bound);
        }

        let mut current_index = self.index_iter.current(index_block_contents);

        while let Some((_, block_handle)) = current_index {
            let block_contents = table.read_block_from_encoded_handle(block_handle)
                .expect("TODO: do proper error handling in iterators");

            self.current_iter.set(block_contents);

            if GEQ {
                self.current_iter.seek(bound);
            } else {
                self.current_iter.seek_before(bound);
            }

            if self.current_iter.valid() {
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
        self.current_iter.clear();
    }
}

impl<'lend, CompList, Policy, TableCmp, File, Cache, Pool: BufferPool>
    LendItem<'lend>
for TableIterImpl<CompList, Policy, TableCmp, File, Cache, Pool>
{
    type Item = (&'lend [u8], &'lend [u8]);
}

impl<CompList, Policy, TableCmp, File, Cache, Pool>
    TableIterImpl<CompList, Policy, TableCmp, File, Cache, Pool>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         TableFilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:           BufferPool,
{
    #[inline]
    pub const fn valid(&self) -> bool {
        // `self.current_iter` is initialized if and only if `self.current_iter`
        // and `self.index_iter` are `valid()`.
        self.current_iter.is_set()
    }

    pub fn next(
        &mut self,
        table: &Table<CompList, Policy, TableCmp, File, Cache, Pool>,
    ) -> Option<LentItem<'_, Self>> {
        self.next_or_prev::<true>(table)
    }

    #[inline]
    pub fn current(&self) -> Option<LentItem<'_, Self>> {
        self.current_iter.current()
    }

    pub fn prev(
        &mut self,
        table: &Table<CompList, Policy, TableCmp, File, Cache, Pool>,
    ) -> Option<LentItem<'_, Self>> {
        self.next_or_prev::<false>(table)
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool>
    TableIterImpl<CompList, Policy, TableCmp, File, Cache, Pool>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         TableFilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:           BufferPool,
{
    pub fn reset(&mut self) {
        // After these calls, `self.current_iter` is not initialized and `self.index_iter`
        // is `!valid()`, so the invariants are satisfied.
        self.current_iter.clear();
        self.index_iter.reset();
    }

    pub fn seek(
        &mut self,
        table: &Table<CompList, Policy, TableCmp, File, Cache, Pool>,
        min_bound: &[u8],
    ) {
        self.seek_bound::<true>(table, min_bound);
    }

    pub fn seek_before(
        &mut self,
        table: &Table<CompList, Policy, TableCmp, File, Cache, Pool>,
        strict_upper_bound: &[u8],
    ) {
        self.seek_bound::<false>(table, strict_upper_bound);
    }

    pub fn seek_to_first(
        &mut self,
        table: &Table<CompList, Policy, TableCmp, File, Cache, Pool>,
    ) {
        self.reset();
        self.next(table);
    }

    pub fn seek_to_last(
        &mut self,
        table: &Table<CompList, Policy, TableCmp, File, Cache, Pool>,
    ) {
        self.reset();
        self.prev(table);
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool> Debug
for TableIterImpl<CompList, Policy, TableCmp, File, Cache, Pool>
where
    TableCmp:           Debug,
    Pool:               BufferPool,
    Pool::PooledBuffer: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("TableIter")
            .field("current_iter",    &self.current_iter)
            .field("index_iter",      &self.index_iter)
            .field("_table_generics", &self._table_generics)
            .finish()
    }
}
