use std::num::NonZeroU32;

use clone_behavior::FastMirroredClone;

use anchored_vfs::{LevelDBFilesystem, WritableFile};

use crate::utils::ReturnBuffer as _;
use crate::{
    all_errors::types::{AddBlockEntryError, AddTableEntryError, WriteTableError},
    options::{DynamicOptions, InternallyMutableOptions, InternalOptions},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::{CodecsCompressionError, CompressionCodecs, CompressorId},
        pool::{BufferPool, ByteBuffer},
    },
    pub_typed_bytes::{
        BlockHandle, FileOffset, FileSize, IndexNonZeroLevel as _, NonZeroLevel, ShortSlice,
        TableBlockSize,
    },
    typed_bytes::{EncodedInternalKey, MaybeUserValue},
};
use super::{block_builder::BlockBuilder, filter_block::FilterBlockBuilder};
use super::footer::{BLOCK_FOOTER_LEN, FILTER_META_PREFIX, TableFooter};


/// A `TableBuilder` is used to create an SSTable from data entries.
///
/// The data entries _must_ be added in the order of their keys under the provided
/// `InternalComparator<Cmp>` comparator. The `TableBuilder` does not validate this behavior, and
/// failing to uphold that requirement may result in panics or the creation of a corrupt SSTable.
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
pub(crate) struct TableBuilder<File, Policy, Pool: BufferPool> {
    table_file:       Option<File>,
    offset_in_file:   FileOffset,
    num_entries:      usize,

    data_block:       BlockBuilder,
    index_block:      BlockBuilder,
    filter_block:     Option<FilterBlockBuilder<Policy>>,
    filter_error:     bool,

    key_scratch:      Vec<u8>,
    compression_buf:  Option<Pool::PooledBuffer>,

    block_size:       usize,
    compressor:       Option<CompressorId>,
    compression_goal: u8,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File, Policy, Pool> TableBuilder<File, Policy, Pool>
where
    File:   WritableFile,
    Policy: FilterPolicy,
    Pool:   BufferPool,
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
    pub fn new<Cmp, Codecs>(opts: &InternalOptions<Cmp, Policy, Codecs>) -> Self
    where
        Policy: FastMirroredClone,
    {
        // These values do not actually matter, since they are overwritten in `Self::start`.
        #[allow(clippy::unwrap_used, reason = "validated at compile time")]
        let dummy_restart_interval = const { NonZeroU32::new(16).unwrap() };
        let dummy_block_size       = 4 << 10_u8;
        let dummy_compressor       = None;
        let dummy_compression_goal = 0;

        let filter_block = opts.policy.as_ref().map(|policy| {
            FilterBlockBuilder::new(policy.fast_mirrored_clone(), opts.filter_chunk_size_log2)
        });
        Self {
            table_file:       None,
            offset_in_file:   FileOffset(0),
            num_entries:      0,
            data_block:       BlockBuilder::new(dummy_restart_interval),
            index_block:      BlockBuilder::new(dummy_restart_interval),
            filter_block,
            filter_error:     false,
            key_scratch:      Vec::new(),
            compression_buf:  None,
            block_size:       dummy_block_size,
            compressor:       dummy_compressor,
            compression_goal: dummy_compression_goal,
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
    pub fn start(
        &mut self,
        dynamic_opts: &DynamicOptions,
        table_file:   File,
        table_level:  Option<NonZeroLevel>,
    ) {
        self.table_file     = Some(table_file);
        self.offset_in_file = FileOffset(0);
        self.num_entries    = 0;

        let (compressor, compression_goal) = if let Some(level) = table_level {
            (
                *dynamic_opts.table_compressors.infallible_index(level),
                *dynamic_opts.table_compression_goals.infallible_index(level),
            )
        } else {
            (dynamic_opts.memtable_compressor, dynamic_opts.memtable_compression_goal)
        };

        self.data_block.reset_with_restart_interval(dynamic_opts.sstable_block_restart_interval);
        self.index_block.reset_with_restart_interval(dynamic_opts.sstable_block_restart_interval);
        self.filter_error = false;

        self.compressor = compressor;
        self.compression_goal = compression_goal;

        if let Some(filter_block) = &mut self.filter_block {
            filter_block.reset();
            // Can be elided. Does nothing. (So we don't have to `expect` or ignore the error.)
            // filter_block.start_block(FileOffset(0));
        }
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
    /// [`TableBuilder::add_entry`].
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
    pub fn estimated_finished_file_length(&self) -> FileSize {
        let filter_len = if self.filter_error {
            0
        } else {
            self.filter_block.as_ref().map_or(0, FilterBlockBuilder::estimated_finished_length)
        };

        let additional_len = self.data_block.finished_length()
            + self.index_block.finished_length()
            + filter_len
            + TableFooter::ENCODED_LENGTH;
        let additional_len = u64::try_from(additional_len).unwrap_or(u64::MAX);

        FileSize(self.offset_in_file.0.saturating_add(additional_len))
    }

    /// Add a new entry to the table.
    ///
    /// With respect to the `TableCmp` comparator that was provided to this builder, the `key` must
    /// compare strictly greater than any previously-added key. If this requirement is not met,
    /// a panic may occur, or an invalid [`Table`] may be produced by this builder.
    ///
    /// # Errors
    /// A return value of [`AddTableEntryError::AddEntryError`] indicates that this table is too
    /// full to have the given entry added to it. This error will *never* be returned for an
    /// empty table.
    ///
    /// # Panics
    /// Panics if the builder is not currently [active].
    ///
    /// [active]: TableBuilder::active
    /// [`Table`]: crate::table::Table
    //
    // This function uses `self.key_scratch` and `self.compression_scratch_buf`.
    pub fn add_entry<FS, Cmp, Codecs>(
        &mut self,
        key:      EncodedInternalKey<'_>,
        value:    MaybeUserValue<'_>,
        opts:     &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts: &InternallyMutableOptions<FS, Policy, Pool>,
        encoders: &mut Codecs::Encoders,
    ) -> Result<(), AddTableEntryError<WriteTableError<Codecs::CompressionError>>>
    where
        FS:         LevelDBFilesystem,
        Cmp:        LevelDBComparator,
        Codecs:     CompressionCodecs,
        Policy::Eq: CoarserThan<Cmp::Eq>,
    {
        // Empty data blocks are not considered to have reached the limit, to ensure that at least
        // one key and value can be added per block.
        let reached_block_size_limit = self.data_block.finished_length() > self.block_size
            && self.data_block.num_entries() > 0;

        // Note that `||` is short-circuiting, so we do not attempt to insert an entry
        // if we reached the block size limit.
        // The caller asserts that this key is strictly greater than previously inserted keys.
        let needs_new_data_block = reached_block_size_limit || matches!(
            self.data_block.add_entry(key.short(), value.0),
            Err(AddBlockEntryError {}),
        );

        if needs_new_data_block {
            // First, note that in this branch, we check that `self.data_block.num_entries() > 0` in
            // `reached_block_size_limit`, OR that `self.data_block` is too full to add an entry.
            // Since empty blocks cannot possibly be too full, we know that
            // `self.data_block.num_entries() > 0` in the second case as well.
            // Therefore, we can make a call to `self.write_data_block`, and access
            // the previous key from `self.data_block.last_key()`.

            // We need to ensure that *after* the below call to
            // `self.write_data_block(Some(_), ..)`, we can still make at least one more call
            // to `self.write_data_block(_, ..)` in `self.finish(..)` or `self.add_entry(..)`
            // without panicking. In particular, each call to `self.write_data_block` may need
            // to insert one entry into `self.index_block`. The capability to insert an entry
            // does not depend on that entry's length, only the length of previous entries. Since
            // the first insertion always succeeds, we therefore need only check *before* each
            // insertion that the *following* insertion would succeed. (This depends solely
            // on the first insertion, which we may inductively assume succeeds.)
            //
            // We will be inserting the following into `self.index_block`:
            // - first:
            //   - key: a separator between `self.data_block.last_key()` and `Some(key)`
            //          (length is at most `self.data_block.last_key().len()`).
            //   - value: a block handle (length at most `BlockHandle::MAX_ENCODED_LENGTH`).
            // - following: whatever.
            //
            // We know by assumption that the first insertion does not fail.
            if !self.index_block.could_add_following_entry(
                self.data_block.last_key().len(),
                BlockHandle::MAX_ENCODED_LENGTH_MIN_U32_USIZE,
            ) {
                return Err(AddTableEntryError::AddEntryError);
            }

            // `key` will be the first key in the next block, so it's less than or equal to any
            // key in the next block. And the caller asserts it's strictly greater than anything
            // already inserted.
            // Correctness: we check that `self.data_block.num_entries() > 0` in
            // `reached_block_size_limit`, OR that `self.data_block` is too full to add an entry.
            // Since empty blocks cannot possibly be too full, we know that
            // `self.data_block.num_entries() > 0` in the second case as well.
            self.write_data_block(Some(key), opts, mut_opts, encoders)
                .map_err(AddTableEntryError::Write)?;
            // `self.write_data_block` reset the data block, so it's now empty. Therefore, this
            // does not panic.
            self.data_block.add_first_entry(key.short(), value.0);
        } else {
            // We have that `!reached_block_size_limit` and
            // `!matches(self.data_block.add_entry(key.short(), value.0)), Err(_))`, so we've
            // taken the `Ok(_)` branch and added an entry to the data block.
            // Note that we always reach this branch if the table was empty. This ensures that
            // regardless of circumstances, at least *one* entry can be added per table,
            // even if keys and values are each 3 GiB long.
        }

        self.num_entries += 1;

        if let Some(filter_block) = &mut self.filter_block {
            if !self.filter_error {
                // We've called `filter_block.start_block(_)` as appropriate, and the caller
                // asserts that this key is strictly greater than previously inserted keys.
                filter_block.add_key(key.as_internal_key().0);
            }
        }

        Ok(())
    }

    /// Finish writing the entire table to the table file and sync it to persistent storage.
    /// WARNING: if the table file was newly created, then the data of the file's parent directory
    /// would also need to be synced to persistent storage in order to ensure crash resilience.
    ///
    /// On success, the total number of bytes written to the table file is returned.
    ///
    /// After this method is called, the builder becomes [inactive], and no other
    /// [`TableBuilder`] methods should be called other than `self.start(_)` (or `drop`).
    ///
    /// # Panics
    /// Panics if the builder is not currently [active].
    ///
    /// [inactive]: TableBuilder::active
    /// [active]: TableBuilder::active
    //
    // This function uses `self.key_scratch` and `self.compression_scratch_buf`.
    pub fn finish<FS, Cmp, Codecs>(
        &mut self,
        opts:     &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts: &InternallyMutableOptions<FS, Policy, Pool>,
        encoders: &mut Codecs::Encoders,
    ) -> Result<FileSize, WriteTableError<Codecs::CompressionError>>
    where
        FS:         LevelDBFilesystem,
        Cmp:        LevelDBComparator,
        Codecs:     CompressionCodecs,
        Policy::Eq: CoarserThan<Cmp::Eq>,
    {
        macro_rules! write_block {
            ($uncompressed_block:expr, NoCompression) => {
                {
                    let table_file = self.table_file.as_mut()
                        .expect(
                            "`add_entry` and `finish` should not be called \
                             on an inactive `TableBuilder`",
                        );
                    // Note that the `compression_goal` does not matter if `compressor` is `None`,
                    // so we can unconditionally use `self.compression_goal`.
                    Self::write_block::<Codecs>(
                        // exploded `self`
                        table_file,
                        &mut self.offset_in_file,
                        &mut self.compression_buf,
                        self.compression_goal,
                        // Actual args
                        $uncompressed_block,
                        None,
                        &mut_opts.buffer_pool,
                        encoders,
                    )
                }
            };

            ($uncompressed_block:expr) => {
                {
                    let table_file = self.table_file.as_mut()
                        .expect(
                            "`add_entry` and `finish` should not be called \
                             on an inactive `TableBuilder`",
                        );
                    Self::write_block::<Codecs>(
                        // exploded `self`
                        table_file,
                        &mut self.offset_in_file,
                        &mut self.compression_buf,
                        self.compression_goal,
                        // Actual args
                        $uncompressed_block,
                        self.compressor,
                        &mut_opts.buffer_pool,
                        encoders,
                    )
                }
            };
        }

        // Write any pending data block
        if self.data_block.num_entries() > 0 {
            // There's no next block. We will not write any other blocks to the table being built.
            self.write_data_block(None, opts, mut_opts, encoders)?;
        }

        // Create metaindex block. We can reuse the data block builder, since this table builder
        // will not be writing any more data blocks until the table is completed.
        // Note that `self.data_block` has already been reset; either it had zero entries,
        // and was thus already in a blank-slate state, or `self.write_data_block(..)`
        // would have called `self.data_block.reset()`.
        'filter: {
            if let Some(filter_block) = &mut self.filter_block {
                if self.filter_error {
                    break 'filter;
                }

                self.key_scratch.clear();
                self.key_scratch.extend(FILTER_META_PREFIX);
                self.key_scratch.extend(filter_block.policy().0.name());
                // To elaborate on the below: it'd also be pretty absurd if the user give a filter a
                // 1GiB-long name. (Also, if the name is at most 1GiB in length, it follows that
                // the length of that name + the length of `FILTER_META_PREFIX` does not exceed
                // `u32::MAX` (which is 4 GiB), and thus the prefixed name fits in a `ShortSlice`.)
                #[expect(
                    clippy::expect_used,
                    reason = "this actually **can** panic, but the user is clearly warned",
                )]
                let prefixed_filter_name = ShortSlice::new(&self.key_scratch)
                    .expect("a FilterPolicy's name must not exceed 1 GiB in length");

                // From this point on, no other methods should be called on `filter_block`,
                // until `self.start(..)` calls `filter_block.reset()`.
                let filter_block = match filter_block.finish() {
                    Ok(filter_block) => filter_block,
                    Err(_build_filter_err) => {
                        // TODO: log error
                        break 'filter;
                    }
                };

                let filter_handle = write_block!(filter_block, NoCompression)?;

                let mut encoded_handle = [0_u8; BlockHandle::MAX_ENCODED_LENGTH];
                let encoded_handle = filter_handle.encode_short(&mut encoded_handle);

                // Reminder: `self.data_block` is currently actually the metaindex block.
                // This is the first entry added, so it's guaranteed to vacuously be strictly
                // greater than any previously-inserted entry, and this method does not panic.
                self.data_block.add_first_entry(prefixed_filter_name, encoded_handle);
            }
        }

        // Write the metaindex and index blocks

        let metaindex_block = self.data_block.finish_block_contents();
        let metaindex = write_block!(metaindex_block)?;
        self.data_block.reset();

        let index_block = self.index_block.finish_block_contents();
        let index = write_block!(index_block)?;

        // Write the footer
        let mut table_footer = [0; TableFooter::ENCODED_LENGTH];
        TableFooter { metaindex, index }.encode_to(&mut table_footer);

        #[expect(
            clippy::expect_used,
            reason = "Panic is declared, and can only occur due to user mistake",
        )]
        let table_file = self.table_file.as_mut()
            .expect("add_entry or finish called on an inactive TableBuilder");
        table_file.write_all(&table_footer).map_err(WriteTableError::WriteTable)?;

        {
            // (Also, there's no way the file size will overflow `u64::MAX`.)
            #![expect(clippy::as_conversions, reason = "the constant is far less than `u64::MAX`")]
            self.offset_in_file.0 += TableFooter::ENCODED_LENGTH as u64;
        };

        table_file.sync_data().map_err(WriteTableError::SyncTable)?;

        self.table_file = None;

        Ok(FileSize(self.offset_in_file.0))
    }

    /// If `Some`, `next_key` must be strictly greater than any key in the current block, and
    /// less than or equal to any key in the next block (if there is one).
    ///
    /// If `None`, no other blocks may be written to the table currently being built.
    ///
    /// This function uses `self.key_scratch` and `self.compression_scratch_buf`.
    ///
    /// # Side Effects
    /// This function writes the current data block in `self.data_block` to the table file,
    /// adds a new entry to `self.index_block`, possibly adds to the filter block,
    /// and clears `self.data_block`.
    ///
    /// # Correctness
    /// This function **must not** be called if `self.data_block` is empty (has zero entries).
    ///
    /// # Panics
    /// Panics if the builder is not currently [active].
    ///
    /// [active]: TableBuilder::active
    fn write_data_block<FS, Cmp, Codecs>(
        &mut self,
        next_key:  Option<EncodedInternalKey<'_>>,
        opts:      &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:  &InternallyMutableOptions<FS, Policy, Pool>,
        encoders:  &mut Codecs::Encoders,
    ) -> Result<(), WriteTableError<Codecs::CompressionError>>
    where
        FS:         LevelDBFilesystem,
        Cmp:        LevelDBComparator,
        Codecs:     CompressionCodecs,
        Policy::Eq: CoarserThan<Cmp::Eq>,
    {
        // The caller asserts that `self.data_block` has at least one entry; therefore, it returns
        // a previously-inserted key. All keys that we insert are valid encoded keys, so there's
        // no need to re-validate it.
        let prev_key = EncodedInternalKey::new_unchecked(self.data_block.last_key().inner());
        self.key_scratch.clear();

        if let Some(next_key) = next_key {
            opts.cmp.find_short_separator(
                prev_key.as_internal_key(),
                next_key.as_internal_key(),
                &mut self.key_scratch,
            );
        } else {
            opts.cmp.find_short_successor(
                prev_key.as_internal_key(),
                &mut self.key_scratch,
            );
        }
        // `find_short_separator` and `find_short_successor` are guaranteed to output
        // a valid `EncodedInternalKey` if they successfully return. Therefore, there's no need
        // to validate this key.
        let index_key = EncodedInternalKey::new_unchecked(&self.key_scratch);

        let uncompressed_block = self.data_block.finish_block_contents();

        #[expect(clippy::expect_used, reason = "panic is documented, and is a caller bug")]
        let table_file = self.table_file
            .as_mut()
            .expect("`add_entry` and `finish` should not be called on an inactive `TableBuilder`");
        let block_handle = Self::write_block::<Codecs>(
            // exploded `self`
            table_file,
            &mut self.offset_in_file,
            &mut self.compression_buf,
            self.compression_goal,
            // Actual args
            uncompressed_block,
            self.compressor,
            &mut_opts.buffer_pool,
            encoders,
        )?;
        self.data_block.reset();

        let mut encoded_handle = [0_u8; BlockHandle::MAX_ENCODED_LENGTH];
        let encoded_handle = block_handle.encode_short(&mut encoded_handle);

        // First, note that `self.data_block` is necessarily nonempty if we get here.
        // Therefore, `self.data_block.last_key()` refers to the last key which was inserted
        // in this table, which is strictly greater than anything previously inserted.
        // All previous keys in the `index_block` will be strictly less than the first key of
        // what was in `self.data_block`. Therefore, the `self.short_scratch` separator or
        // successor is strictly greater than all previous keys in `index_block`.
        #[expect(clippy::expect_used, reason = "hard to thoroughly verify, but should not panic")]
        self.index_block.add_entry(index_key.short(), encoded_handle)
            .expect(
                "bug in `TableBuilder`; `AddTableEntryError` should be returned earlier to \
                 prevent adding an index block entry from failing",
            );

        if let Some(filter_block) = &mut self.filter_block {
            if let Err(_err) = filter_block.start_block(self.offset_in_file) {
                // TODO: log error
                self.filter_error = true;
            }
        }

        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "morally, there are only 5 arguments (including `&mut self`)",
    )]
    fn write_block<Codecs: CompressionCodecs>(
        // mfw no view types
        table_file:         &mut File,
        offset_in_file:     &mut FileOffset,
        compression_buf:    &mut Option<Pool::PooledBuffer>,
        compression_goal:   u8,
        // Actual arguments
        uncompressed_block: &[u8],
        compressor:         Option<CompressorId>,
        buffer_pool:        &Pool,
        encoders:           &mut Codecs::Encoders,
    ) -> Result<BlockHandle, WriteTableError<Codecs::CompressionError>> {
        let compressed_buf = if let Some(compressor_id) = compressor {
            let multiplied_len = u128::try_from(uncompressed_block.len())
                .ok()
                .and_then(|src_len| src_len.checked_mul(u128::from(compression_goal)))
                .unwrap_or(0);

            // Without overflow/wrapping,
            // `multiplied_len <= 255 * block_contents.len() <= 255 * usize::MAX`
            // so `multiplied_len / 256 <= block_contents.len() <= usize::MAX`.
            #[expect(
                clippy::as_conversions,
                clippy::integer_division,
                reason = "no wrapping/truncation occurs",
            )]
            let compression_goal_diff = (multiplied_len / 256) as usize;
            // No underflow occurs; see above.
            let compression_goal_len = uncompressed_block.len() - compression_goal_diff;

            match Codecs::encode(
                encoders,
                uncompressed_block,
                compressor_id,
                compression_goal_len,
                buffer_pool,
                compression_buf,
            ) {
                Ok(compressed_buf)                          => Some(compressed_buf),
                Err(CodecsCompressionError::Unsupported)    => return Err(
                    WriteTableError::UnsupportedCompressor(compressor_id),
                ),
                Err(CodecsCompressionError::BufferAlloc)    => return Err(
                    WriteTableError::BufferAllocErr,
                ),
                Err(CodecsCompressionError::Incompressible) => None,
                Err(CodecsCompressionError::Custom(err))    => return Err(
                    WriteTableError::Compression(
                        compressor_id,
                        // This clone is expensive, but should not usually happen.
                        uncompressed_block.to_vec(),
                        err,
                    ),
                ),
            }
        } else {
            None
        };

        let compressed_block = compressed_buf.as_ref()
            .map(ByteBuffer::as_slice)
            .unwrap_or(uncompressed_block);
        let compressor = compressor.map(|id| id.0.get()).unwrap_or(0);

        let mut digest = crc32c::crc32c(compressed_block);
        digest = crc32c::crc32c_append(digest, &[compressor]);

        // Write the block: the compressed contents, followed by the table block trailer.
        table_file.write_all(compressed_block).map_err(WriteTableError::WriteTable)?;
        table_file.write_all(&[compressor]).map_err(WriteTableError::WriteTable)?;
        table_file.write_all(&digest.to_le_bytes()).map_err(WriteTableError::WriteTable)?;

        #[expect(clippy::expect_used, reason = "could theoretically panic, but won't")]
        let block_size = u64::try_from(uncompressed_block.len())
            .expect("A single slice should not be exabytes in length");

        // We're done with the compressed data.
        if let Some(new_buf) = compressed_buf {
            compression_buf.return_buffer(new_buf);
        }

        let block_handle = BlockHandle {
            offset: *offset_in_file,
            size:   TableBlockSize(block_size),
        };

        {
            #![expect(clippy::as_conversions, reason = "constant (5) is far less than `u64::MAX`")]
            offset_in_file.0 += block_size + BLOCK_FOOTER_LEN as u64;
        };

        Ok(block_handle)
    }
}
