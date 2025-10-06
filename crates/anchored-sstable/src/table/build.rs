use generic_container::FragileContainer;

use anchored_vfs::traits::WritableFile;

use crate::{
    block::BlockBuilder,
    filter_block::FilterBlockBuilder,
    option_structs::WriteTableOptions,
};
use crate::{
    comparator::{ComparatorAdapter, TableComparator},
    compressors::{CompressorList, NO_COMPRESSION},
    filters::{FILTER_NUM_KEYS_LIMIT, TableFilterPolicy},
};
use super::format::{BlockHandle, BLOCK_TRAILER_LEN, FILTER_META_PREFIX, TableFooter};


/// A `TableBuilder` is used to create a [`Table`] from data entries.
///
/// The data entries _must_ be added in the order of their keys under the provided `TableCmp`
/// comparator. The `TableBuilder` does not necessarily validate this behavior, and failing to
/// uphold that requirement may result in panics or an invalid [`Table`] being produced.
///
/// # Active and Inactive Builders
/// A builder is active only while it has an associated in-progress table file, provided in
/// [`TableBuilder::start`] and consumed in [`TableBuilder::finish`]. A just-constructed
/// builder is inactive.
///
/// [`add_entry`] and [`finish`] must only be called on active builders, or else a panic will
/// occur.
///
/// Note that if an active builder is dropped, the file may be closed, but the table file will
/// _not_ be properly finished; it would be an invalid table file.
///
/// [`add_entry`]: TableBuilder::add_entry
/// [`finish`]: TableBuilder::finish
/// [`Table`]: crate::table::Table
#[derive(Debug)]
pub struct TableBuilder<CompList, Policy, TableCmp, File> {
    compressor_list:         CompList,
    selected_compressor:     u8,
    comparator:              TableCmp,

    table_file:              Option<File>,
    offset_in_file:          u64,
    num_entries:             usize,

    block_size:              usize,
    data_block:              BlockBuilder<ComparatorAdapter<TableCmp>>,
    index_block:             BlockBuilder<ComparatorAdapter<TableCmp>>,
    filter_block:            Option<FilterBlockBuilder<Policy>>,

    /// Should almost always be `empty()`, except while in direct use.
    short_scratch:           Vec<u8>,
    compression_scratch_buf: Vec<u8>,
}

/// Macro to explode `&mut self` into borrows to several fields.
///
/// Note that this uses `self.compression_scratch_buf`.
///
/// # Panics
/// Panics if the builder is not currently [active].
///
/// [active]: TableBuilder::active
macro_rules! builder_write_block {
    ($builder:expr, $block_contents:expr, $compressor_id:expr $(,)?) => {
        Self::write_block(
            // mfw no view types
            &$builder.compressor_list,
            $builder.table_file
                .as_mut()
                .expect("add_entry or finish called on an inactive TableBuilder"),
            &mut $builder.offset_in_file,
            &mut $builder.compression_scratch_buf,
            // Actual arguments
            $block_contents,
            $compressor_id,
        )
    };
}

#[expect(
    clippy::result_unit_err, clippy::map_err_ignore,
    reason = "temporary. TODO: return actual errors.",
)]
impl<CompList, Policy, TableCmp, File> TableBuilder<CompList, Policy, TableCmp, File>
where
    CompList: FragileContainer<CompressorList>,
    Policy:   TableFilterPolicy,
    TableCmp: TableComparator,
    File:     WritableFile,
{
    /// Create a new and initially [inactive] builder. Before [`add_entry`] or [`finish`] is
    /// called on the returned builder, [`start`] must be called on it.
    ///
    /// [inactive]: TableBuilder::active
    /// [`start`]: TableBuilder::start
    /// [`add_entry`]: TableBuilder::add_entry
    /// [`finish`]: TableBuilder::finish
    #[inline]
    #[must_use]
    pub fn new(opts: WriteTableOptions<CompList, Policy, TableCmp>) -> Self {
        Self {
            compressor_list:         opts.compressor_list,
            selected_compressor:     opts.selected_compressor,
            comparator:              opts.comparator,
            table_file:              None,
            offset_in_file:          0,
            num_entries:             0,
            block_size:              opts.block_size,
            data_block:              BlockBuilder::new(opts.block_restart_interval),
            index_block:             BlockBuilder::new(opts.block_restart_interval),
            filter_block:            opts.filter_policy.map(FilterBlockBuilder::new),
            short_scratch:           Vec::new(),
            compression_scratch_buf: Vec::new(),
        }
    }

    /// Begin writing a table file to the provided [`WritableFile`], which should be empty at the
    /// time it is passed to this function.
    ///
    /// The builder then becomes [active], and may have [`add_entry`] or [`finish`] called on it.
    ///
    /// Note that if the builder was already active, the previous table file would be closed, but
    /// it would _not_ be properly finished; that file would be an invalid table file.
    ///
    /// [active]: TableBuilder::active
    /// [`add_entry`]: TableBuilder::add_entry
    /// [`finish`]: TableBuilder::finish
    #[inline]
    pub fn start(&mut self, table_file: File) {
        self.table_file     = Some(table_file);
        self.offset_in_file = 0;
        self.num_entries    = 0;

        self.data_block.reset();
        self.index_block.reset();

        if let Some(filter_block) = &mut self.filter_block {
            filter_block.reuse_as_new();
            filter_block.start_block(0);
        }

        self.short_scratch.clear();
        self.compression_scratch_buf.clear();
    }

    /// Abandon the previous table file (if any), making the builder [inactive].
    ///
    /// Note that if the builder was active, the previous table file would be closed, but
    /// it would _not_ be properly finished; that file would be an invalid table file.
    ///
    /// [inactive]: TableBuilder::active
    pub fn deactivate(&mut self) {
        // This causes the previous table file to be dropped.
        // We don't need to do anything else; `start` would handle resetting stuff.
        self.table_file = None;
    }

    /// Determines whether the builder has an associated table file.
    ///
    /// A builder is active only while it has an associated in-progress table file, provided in
    /// [`TableBuilder::start`] and consumed in [`TableBuilder::finish`]. A just-constructed
    /// builder is inactive.
    ///
    /// [`add_entry`] and [`finish`] must only be called on active builders, or else a panic will
    /// occur.
    ///
    /// [`add_entry`]: TableBuilder::add_entry
    /// [`finish`]: TableBuilder::finish
    #[inline]
    #[must_use]
    pub const fn active(&self) -> bool {
        self.table_file.is_some()
    }

    /// Get the number of entries which have been added to the current table with
    /// [`TableBulder::add_entry`].
    ///
    /// If the builder is not [active], then the value is unspecified, though a panic will not
    /// occur.
    ///
    /// [active]: TableBuilder::active
    #[must_use]
    pub const fn num_entries(&self) -> usize {
        self.num_entries
    }

    /// Estimates the length that the table file currently being built would have if `self.finish()`
    /// were called now.
    ///
    /// This is a rough estimate that does not take into account:
    /// - compression of the current data block,
    /// - compression of the index block,
    /// - the metaindex block, which contains the name of any filter policy.
    ///
    /// If the builder is not [active], then the value is unspecified, though a panic will not
    /// occur.
    ///
    /// [active]: TableBuilder::active
    #[must_use]
    pub fn estimated_finished_file_length(&self) -> u64 {
        let additional_len = self.data_block.finished_length()
            + self.index_block.finished_length()
            + self.filter_block.as_ref().map(FilterBlockBuilder::finished_length).unwrap_or(0)
            + TableFooter::ENCODED_LENGTH;
        let additional_len = u64::try_from(additional_len).unwrap_or(u64::MAX);

        self.offset_in_file.saturating_add(additional_len)
    }

    /// Add a new entry to the table.
    ///
    /// With respect to the `TableCmp` comparator that was provided to this builder, the `key` must
    /// compare strictly greater than any previously-added key. If this requirement is not met,
    /// a panic may occur, or an invalid [`Table`] may be produced by this builder.
    ///
    /// # Panics
    /// Panics if the builder is not currently [active].
    ///
    /// May panic if `key.len() + value.len() + opts.block_size + 30` exceeds `u32::MAX`, where
    /// `opts` refers to the [`WriteTableOptions`] struct which was provided to [`Self::new`].
    /// More precisely, if the current block's size ends up exceeding `u32::MAX`, a panic would
    /// occur. See [`BlockBuilder::add_entry`] for more.
    ///
    /// May also panic if adding this entry would result in at least 4 gigabytes of key data,
    /// produced by [`Policy::append_key_data`], associated with the current block.
    /// Note that the key data is not necessarily equivalent to concatenating the keys together.
    /// Lastly, this function may panic if at least 4 gigabytes of filters are generated
    /// by `Policy` for this table; such an event would generally only occur if hundreds of millions
    /// of entries were added to a single table. See [`FilterBlockBuilder`] for more.
    ///
    /// [active]: TableBuilder::active
    /// [`Table`]: crate::table::Table
    /// [`Policy::append_key_data`]: TableFilterPolicy::append_key_data
    //
    // This function uses `self.short_scratch` and `self.compression_scratch_buf`.
    pub fn add_entry(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        let reached_block_size_limit = self.data_block.finished_length() > self.block_size
            && self.data_block.num_entries() > 0;
        let reached_filter_limit = self.data_block.num_entries()
            >= usize::try_from(FILTER_NUM_KEYS_LIMIT).unwrap_or(usize::MAX);

        if reached_block_size_limit || reached_filter_limit {
            // `key` will be the first key in the next block, so it's less than or equal to any
            // key in the next block. And the caller asserts it's strictly greater than anything
            // already inserted.
            self.write_data_block(Some(key))?;
        }

        if let Some(filter_block) = &mut self.filter_block {
            // We've called `filter_block.start_block(_)` as appropriate, and the caller
            // asserts that this key is strictly greater than previously inserted keys.
            filter_block.add_key(key);
        }

        // The caller asserts that this key is strictly greater than previously inserted keys.
        self.data_block.add_entry(key, value);
        self.num_entries += 1;

        Ok(())
    }

    /// Finish writing the entire table to the table file. Optionally, sync the file to
    /// persistent storage.
    ///
    /// On success, the total number of bytes written to the table file is returned.
    ///
    /// After this method is called, the builder becomes [inactive], and no other
    /// [`TableBuilder`] methods should be called other than `self.start(_)` (or `drop`).
    ///
    /// # Panics
    /// Panics if the builder is not currently [active].
    ///
    /// This function may also panic if more than 4 gigabytes of filters are generated
    /// by `Policy` for this table; such an event would generally only occur if hundreds of millions
    /// of entries were added to a single table.
    ///
    /// [inactive]: TableBuilder::active
    /// [active]: TableBuilder::active
    //
    // This function uses `self.short_scratch` and `self.compression_scratch_buf`.
    pub fn finish(&mut self, sync_file_data: bool) -> Result<u64, ()> {
        // Write any pending data block
        if self.data_block.num_entries() > 0 {
            // There's no next block. We will not write any other blocks to the table being built.
            self.write_data_block(None)?;
        }

        // Create metaindex block. We can reuse the data block builder, since this table builder
        // will not be writing any more data blocks until the table is completed.
        // Note that `self.data_block` has already been reset; either it had zero entries,
        // and was thus already in a blank-slate state, or `self.write_data_block(..)`
        // would have called `self.data_block.reset()`.
        //
        // Also, IMPORTANT NOTE:
        // If any more than one entry were to be added to the metaindex block, this approach
        // would not work. Or, it technically would, but I'd want to refactor something.
        // Why? Because the metaindex block, unlike every other block in an SSTable, is *always*
        // sorted by the default bytewise comparator. Technically, the `BlockBuilder` doesn't
        // actually care about its `Cmp` parameter, but still, it'd go against the stated letter
        // of the law to say that `ComparatorAdapter<TableCmp>` can be used as the parameter of
        // a `BlockBuilder` when the block is actually sorted by
        // `ComparatorAdapter<LexicographicComparator>`.
        // Adding one key is acceptable because, regardless of comparator, inserting the key
        // doesn't violate the block invariants and always has the same behavior.
        if let Some(filter_block) = &mut self.filter_block {
            self.short_scratch.extend(FILTER_META_PREFIX);
            self.short_scratch.extend(filter_block.policy().name());

            // From this point on, no other methods should be called on `filter_block`,
            // until `self.reuse_as_new(..)` calls `filter_block.reuse_as_new(..)`.
            let filter_block_data = filter_block.finish();

            let filter_handle = builder_write_block!(self, filter_block_data, NO_COMPRESSION)?;

            let mut encoded_handle = [0_u8; BlockHandle::MAX_ENCODED_LENGTH];
            let encoded_len = filter_handle.encode_to(&mut encoded_handle);

            // Reminder: `self.data_block` is currently actually the metaindex block.
            // Also, this is the first entry added, so it's guaranteed to vacuously be strictly
            // greater than any previously-inserted entry.
            #[expect(clippy::indexing_slicing, reason = "`encoded_len <= MAX_ENCODED_LENGTH`")]
            self.data_block.add_entry(
                &self.short_scratch,
                &encoded_handle[..encoded_len],
            );
            self.short_scratch.clear();
        }

        // Write the metaindex and index blocks

        let metaindex_block = self.data_block.finish_block_contents();
        let metaindex = builder_write_block!(self, metaindex_block, self.selected_compressor)?;

        let index_block = self.index_block.finish_block_contents();
        let index = builder_write_block!(self, index_block, self.selected_compressor)?;

        // Write the footer
        self.short_scratch.resize(TableFooter::ENCODED_LENGTH, 0);
        // The only way for it to fail is if the buffer isn't long enough, but we resized it
        // so that it's long enough.
        let _success = (TableFooter { metaindex, index }).encode_to(&mut self.short_scratch);

        #[expect(
            clippy::expect_used,
            reason = "Panic is declared, and can only occur due to user mistake",
        )]
        let table_file = self.table_file.as_mut()
            .expect("add_entry or finish called on an inactive TableBuilder");
        table_file.write_all(&self.short_scratch).map_err(|_| ())?;
        self.short_scratch.clear();

        {
            #![expect(clippy::as_conversions, reason = "the constant is far less than `u64::MAX`")]
            self.offset_in_file += TableFooter::ENCODED_LENGTH as u64;
        };
        table_file.flush().map_err(|_| ())?;
        if sync_file_data {
            table_file.sync_data().map_err(|_| ())?;
        }
        self.table_file = None;

        Ok(self.offset_in_file)
    }

    /// If `Some`, `next_key` must be strictly greater than any key in the current block, and
    /// less than or equal to any key in the next block (if there is one).
    ///
    /// If `None`, no other blocks may be written to the table currently being built.
    ///
    /// This function uses `self.short_scratch` and `self.compression_scratch_buf`.
    ///
    /// # Correctness
    /// This function must not be called if `self.data_block` is empty (has zero entries).
    ///
    /// # Panics
    /// Panics if the builder is not currently [active].
    ///
    /// [active]: TableBuilder::active
    fn write_data_block(&mut self, next_key: Option<&[u8]>) -> Result<(), ()> {
        if let Some(next_key) = next_key {
            self.comparator.find_short_separator(
                self.data_block.last_key(),
                next_key,
                &mut self.short_scratch,
            );
        } else {
            self.comparator.find_short_successor(
                self.data_block.last_key(),
                &mut self.short_scratch,
            );
        }

        let block_contents = self.data_block.finish_block_contents();

        let block_handle = builder_write_block!(self, block_contents, self.selected_compressor)?;
        self.data_block.reset();

        let mut encoded_handle = [0_u8; BlockHandle::MAX_ENCODED_LENGTH];
        let encoded_len = block_handle.encode_to(&mut encoded_handle);

        // First, note that `self.data_block` is necessarily nonempty if we get here.
        // Therefore, `self.data_block.last_key()` refers to the last key which was inserted
        // in this table, which is strictly greater than anything previously inserted.
        // All previous keys in the `index_block` will be strictly less than the first key of
        // what was in `self.data_block`. Therefore, the `self.short_scratch` separator or
        // successor is strictly greater than all previous keys in `index_block`.
        #[expect(clippy::indexing_slicing, reason = "`encoded_len <= MAX_ENCODED_LENGTH`")]
        self.index_block.add_entry(&self.short_scratch, &encoded_handle[..encoded_len]);
        self.short_scratch.clear();

        if let Some(filter_block) = &mut self.filter_block {
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "file size will not reach 16 exabytes",
            )]
            filter_block.start_block(self.offset_in_file as usize);
        }

        Ok(())
    }

    fn write_block(
        // mfw no view types; use `builder_write_block!(..)`
        compressor_list:        &CompList,
        table_file:             &mut File,
        offset_in_file:         &mut u64,
        scratch_buffer:         &mut Vec<u8>,
        // Actual arguments
        block_contents:         &[u8],
        compressor_id:          u8,
    ) -> Result<BlockHandle, ()> {
        // Scope for destructor of `compressor_list.get_ref()`
        {
            let compressor_list: &CompressorList = &compressor_list.get_ref();
            let Some(compressor) = compressor_list.get(compressor_id) else {
                return Err(());
            };

            compressor.encode_into(block_contents, scratch_buffer).map_err(|_| ())?;
        };

        let mut digest = crc32c::crc32c(scratch_buffer);
        digest = crc32c::crc32c_append(digest, &[compressor_id]);

        // Write the block: the compressed contents, followed by the table block trailer.
        table_file.write_all(scratch_buffer).map_err(|_| ())?;
        table_file.write_all(&[compressor_id]).map_err(|_| ())?;
        table_file.write_all(&digest.to_le_bytes()).map_err(|_| ())?;

        let block_size = u64::try_from(scratch_buffer.len()).map_err(|_| ())?;
        // We're done with the scratch buffer data.
        scratch_buffer.clear();

        let block_handle = BlockHandle {
            offset:     *offset_in_file,
            block_size,
        };

        {
            #![expect(clippy::as_conversions, reason = "constant is far less than `u64::MAX`")]
            *offset_in_file += block_size + BLOCK_TRAILER_LEN as u64;
        };

        Ok(block_handle)
    }
}
