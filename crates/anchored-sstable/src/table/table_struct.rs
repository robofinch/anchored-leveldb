use std::cmp::Ordering;
use std::{
    borrow::{Borrow as _, BorrowMut as _},
    fmt::{Debug, Formatter, Result as FmtResult},
};

use clone_behavior::{ConstantTime, MirroredClone};
use generic_container::FragileContainer;
use seekable_iterator::{CursorLendingIterator as _, Seekable as _};

use anchored_vfs::traits::RandomAccess;

use crate::{
    block::TableBlock,
    compressors::CompressorList,
    filters::FilterPolicy,
    filter_block::FilterBlockReader,
    iter::BlockIterImpl,
    option_structs::ReadTableOptions,
    pool::BufferPool,
};
use crate::{
    caches::{CacheDebugAdapter, CacheKey, TableBlockCache},
    comparator::{ComparatorAdapter, MetaindexComparator, TableComparator},
};
use super::{entry::TableEntry, iter::TableIter, read::TableBlockReader};
use super::format::{BlockHandle, TableFooter};


pub struct Table<CompList, Policy, TableCmp, File, Cache, Pool: BufferPool> {
    compressor_list:  CompList,
    verify_checksums: bool,
    buffer_pool:      Pool,

    file:             File,
    metaindex_offset: u64,

    block_cache:      Option<CacheDebugAdapter<Cache, Pool::PooledBuffer>>,
    #[allow(clippy::struct_field_names, reason = "clarify what the ID identifies")]
    table_id:         u64,

    index_block:      TableBlock<Pool::PooledBuffer, TableCmp>,
    filter_block:     Option<FilterBlockReader<Policy, Pool::PooledBuffer>>,
}

#[expect(
    clippy::result_unit_err, clippy::map_err_ignore,
    reason = "temporary. TODO: return actual errors.",
)]
impl<CompList, Policy, TableCmp, File, Cache, Pool>
    Table<CompList, Policy, TableCmp, File, Cache, Pool>
where
    CompList: FragileContainer<CompressorList>,
    Policy:   FilterPolicy,
    TableCmp: TableComparator + MirroredClone<ConstantTime>,
    File:     RandomAccess,
    Cache:    TableBlockCache<Pool::PooledBuffer>,
    Pool:     BufferPool,
{
    pub fn new(
        opts:      ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool>,
        file:      File,
        file_size: u64,
        table_id:  u64,
    ) -> Result<Self, ()> {
        // We need to read the footer and the index block, at the very least.
        // Additionally, if a `Policy` was selected, then we need to read the metaindex block
        // and filter block.
        // Because we only need the metaindex block temporarily (if at all), we can reuse the
        // buffer for the index block.

        let mut scratch_buffer = opts.buffer_pool.get_buffer();
        let scratch_buffer: &mut Vec<u8> = scratch_buffer.borrow_mut();

        #[expect(clippy::as_conversions, reason = "the constant is far less than `u64::MAX`")]
        file.read_exact_at(
            file_size - TableFooter::ENCODED_LENGTH as u64,
            scratch_buffer,
        ).map_err(|_| ())?;
        let footer = TableFooter::decode_from(&*scratch_buffer)?;
        scratch_buffer.clear();

        let mut block_reader = TableBlockReader {
            file:             &file,
            compressor_list:  &opts.compressor_list,
            verify_checksums: opts.verify_checksums,
            buffer_pool:      &opts.buffer_pool,
            scratch_buffer,
        };

        let mut block_buffer = opts.buffer_pool.get_buffer();

        let filter_block = if let Some(policy) = opts.policy {
            block_reader.read_table_block(footer.metaindex, block_buffer.borrow_mut())?;
            let metaindex_cmp = ComparatorAdapter(MetaindexComparator::new());
            let metaindex_block = TableBlock::new(block_buffer, metaindex_cmp);

            let filter_block = block_reader.read_filter_block(policy, &metaindex_block)?;

            block_buffer = metaindex_block.contents;
            block_buffer.borrow_mut().clear();

            filter_block
        } else {
            None
        };

        block_reader.read_table_block(footer.index, block_buffer.borrow_mut())?;
        let index_block = TableBlock::new(
            block_buffer,
            ComparatorAdapter(opts.comparator),
        );

        Ok(Self {
            compressor_list:  opts.compressor_list,
            verify_checksums: opts.verify_checksums,
            buffer_pool:      opts.buffer_pool,
            file,
            metaindex_offset: footer.metaindex.offset,
            block_cache:      opts.block_cache.map(CacheDebugAdapter::new),
            table_id,
            index_block,
            filter_block,
        })
    }

    /// Attempt to get an entry with a key "near" `min_bound`.
    ///
    /// If an entry is returned as `Ok(Some(_))`, it is the least entry in the `Table` whose key
    /// is greater than or equal to `min_bound`.
    ///
    /// Unless an error occurs, this method is guaranteed to return `Ok(None)` if:
    /// - there is no entry with a key greater than or equal to `min_bound`, or
    /// - a [`FilterPolicy`] was supplied to this [`Table`], and the relevant filter of that
    ///   `Policy` did not match `min_bound`.
    ///
    /// This method may or may not return `Ok(None)` if there exist entries in the `Table` with
    /// keys `from` and `to` such that:
    /// - `from < min_bound`,
    /// - `min_bound < to`, and
    /// - where `separator` is the result of applying `TableCmp::from_short_separator` to
    ///   `from` and `to`, it holds that `min_bound <= separator`.
    ///
    /// All comparisons refer to the `TableCmp` comparator provided to this `Table`.
    ///
    /// # Errors
    /// Returns `Err(_)` if corruption was encountered.
    pub fn get(&self, min_bound: &[u8]) -> Result<Option<TableEntry<Pool::PooledBuffer>>, ()> {
        let mut index_iter = self.index_block.iter();
        index_iter.seek(min_bound);

        // If this returns `None` in the `else` branch, then `min_bound` is past the last entry.
        if let Some((_, block_handle)) = index_iter.current() {
            let (handle, _) = BlockHandle::decode_from(block_handle)?;

            if self.filter_block.as_ref().is_some_and(|filter_block| {
                !filter_block.key_may_match(handle.offset, min_bound)
            }) {
                return Ok(None);
            }

            // Note in this branch that `[some_key_in_block] <= min_bound <= separator`
            // and, if there is a following block,
            // `separator` was returned from `TableCmp::from_short_separator` applied to
            // `[some_key_in_block]` and a strictly-greater `[some_key_in_next_block]`.

            let block_buffer = self.read_block(handle)?;
            let block_contents = block_buffer.borrow();
            let mut block_iter = BlockIterImpl::new(block_contents);

            block_iter.seek(block_contents, self.comparator(), min_bound);

            // If `TableEntry::new` returns `None`, then:
            // - `[some_key_in_block] < min_bound` for each key in this block, else we'd have
            //   found and returned the entry corresponding to a GEQ key;
            // - if there is not a next block, `min_bound` is strictly past the last entry
            //   in the `Table`;
            // - if there is a next block,
            //   `last_key_in_block < min_bound <= separator < first_key_in_next_block`
            // If this returns `Some`, then it's the first entry which is greater than or equal to
            // `min_bound` in the first block whose keys are not bounded above by an index strictly
            // less than `min_bound`.
            //
            // TLDR: we satisfy the documentation of this `Table::get` method.
            Ok(TableEntry::new(block_buffer, block_iter))
        } else {
            Ok(None)
        }
    }

    /// Attempt to get an entry with a key "near" `min_bound`.
    ///
    /// The least entry in the `Table` whose key is greater than or equal to `min_bound` is
    /// returned as `Ok(Some(_))`, unless:
    /// - there is no entry whose key is greater than or equal to `min_bound` and strictly less
    ///   than `strict_upper_bound`,
    /// - a [`FilterPolicy`] was supplied to this `Table`, and the relevant filter(s) of that
    ///   `Policy` did not match `min_bound`, or
    /// - corruption was encounted.
    ///
    /// In the first two cases, `Ok(None)` is returned.
    ///
    /// # Errors
    /// Returns `Err(_)` if corruption was encountered.
    pub fn get_range(
        &self,
        min_bound: &[u8],
        strict_upper_bound: &[u8],
    ) -> Result<Option<TableEntry<Pool::PooledBuffer>>, ()> {
        let mut index_iter = self.index_block.iter();
        let comparator = &self.comparator().0;

        index_iter.seek(min_bound);
        let mut current_index_entry = index_iter.current();

        // If this loop condition isn't met, then `min_bound` is past the last entry.
        while let Some((index, block_handle)) = current_index_entry {
            let (handle, _) = BlockHandle::decode_from(block_handle)?;

            if self.filter_block.as_ref().is_some_and(|filter_block| {
                !filter_block.key_may_match(handle.offset, min_bound)
            }) {
                return Ok(None);
            }

            let block_buffer = self.read_block(handle)?;
            let block_contents = block_buffer.borrow();
            let mut block_iter = BlockIterImpl::new(block_contents);

            block_iter.seek(block_contents, self.comparator(), min_bound);

            if let Some(entry) = block_iter.current(block_contents) {
                if comparator.cmp(entry.0, strict_upper_bound) == Ordering::Less {
                    return Ok(TableEntry::new(block_buffer, block_iter));
                } else {
                    // The smallest entry greater than or equal to `min_bound` is too large
                    return Ok(None);
                }
            }

            if comparator.cmp(index, strict_upper_bound) != Ordering::Less {
                // The `index` separator is a strict lower bound on the key values of any
                // following blocks. If `strict_upper_bound <= index < following_keys`,
                // then any following entry is too large. Only if
                // `index < following_keys < strict_upper_bound` could we return `Ok(Some(_))`.
                return Ok(None);
            }

            current_index_entry = index_iter.next();
        }

        Ok(None)
    }

    pub fn approximate_offset_of_key(&self, key: &[u8]) -> u64 {
        let mut iter = self.index_block.iter();
        iter.seek(key);

        if let Some((_, block_handle)) = iter.current() {
            if let Ok((block_handle, _)) = BlockHandle::decode_from(block_handle) {
                return block_handle.offset;
            }
        }

        // Either the `key` is greater than the largest key in this table,
        // or the index block is corrupt. The `metaindex_offset` is the offset just after the
        // last data block, so at least if the index block isn't corrupt, the answer is reasonable.
        self.metaindex_offset
    }

    #[expect(clippy::should_implement_trait, reason = "the iterator is a lending iterator")]
    #[inline]
    #[must_use]
    pub fn into_iter(self) -> TableIter<CompList, Policy, TableCmp, File, Cache, Pool, Self> {
        TableIter::new(self)
    }

    #[inline]
    #[must_use]
    pub fn new_iter<TableContainer>(
        table_container: TableContainer,
    ) -> TableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
    where
        TableContainer: FragileContainer<Self>,
    {
        TableIter::new(table_container)
    }

    /// Used by [`TableIter`].
    pub(super) const fn comparator(&self) -> &ComparatorAdapter<TableCmp> {
        &self.index_block.cmp
    }

    /// Used by [`TableIter`].
    pub(super) const fn index_block(&self) -> &TableBlock<Pool::PooledBuffer, TableCmp> {
        &self.index_block
    }

    /// Read and cache the block with the given encoded handle, and return the block contents on
    /// success.
    ///
    /// Used by [`TableIter`].
    pub(super) fn read_block_from_encoded_handle(
        &self,
        encoded_handle: &[u8],
    ) -> Result<Pool::PooledBuffer, ()> {
        let (handle, _) = BlockHandle::decode_from(encoded_handle)?;

        self.read_block(handle)
    }

    /// Read and cache the block with the given handle, and return the block contents on success.
    ///
    /// Used by [`TableIter`].
    pub(super) fn read_block(&self, handle: BlockHandle) -> Result<Pool::PooledBuffer, ()> {
        let cache_key = CacheKey {
            table_id:      self.table_id,
            handle_offset: handle.offset,
        };

        if let Some(cache) = self.block_cache.as_ref() {
            if let Some(block) = cache.get(&cache_key) {
                return Ok(block);
            }
        }

        let mut scratch_buffer = self.buffer_pool.get_buffer();
        let mut block_reader = TableBlockReader {
            file:             &self.file,
            compressor_list:  &self.compressor_list,
            verify_checksums: self.verify_checksums,
            buffer_pool:      &self.buffer_pool,
            scratch_buffer:   scratch_buffer.borrow_mut(),
        };

        let mut block_buffer = self.buffer_pool.get_buffer();
        block_reader.read_table_block(handle, block_buffer.borrow_mut())?;

        if let Some(cache) = self.block_cache.as_ref() {
            cache.insert(cache_key, &block_buffer);
        }

        Ok(block_buffer)
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool> Debug
for Table<CompList, Policy, TableCmp, File, Cache, Pool>
where
    CompList:           Debug,
    Policy:             Debug,
    TableCmp:           Debug,
    File:               Debug,
    Cache:              TableBlockCache<Pool::PooledBuffer>,
    Pool:               Debug + BufferPool,
    Pool::PooledBuffer: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Table")
            .field("compressor_list",  &self.compressor_list)
            .field("verify_checksums", &self.verify_checksums)
            .field("buffer_pool",      &self.buffer_pool)
            .field("file",             &self.file)
            .field("metaindex_offset", &self.metaindex_offset)
            .field("block_cache",      &self.block_cache)
            .field("table_id",         &self.table_id)
            .field("index_block",      &self.index_block)
            .field("filter_block",     &self.filter_block)
            .finish()
    }
}
