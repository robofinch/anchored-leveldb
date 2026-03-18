use std::{num::NonZeroU8, sync::Arc};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use anchored_vfs::RandomAccess;
use clone_behavior::FastMirroredClone;

use crate::{table_caches::BlockCacheKey, table_format::InternalFilterPolicy};
use crate::{
    all_errors::types::{
        CompressedBlockError, CorruptedTableError, MetaindexIterError, NewTableReaderError,
        ReadTableBlockError, TableFooterCorruption,
    },
    options::{InternalOptions, InternalOptionsPerRead},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::{CodecsDecompressionError, CompressionCodecs, CompressorId},
        pool::{BufferPool, ByteBuffer as _},
    },
    pub_typed_bytes::{BlockHandle, BlockType, FileNumber, FileOffset, FileSize},
    typed_bytes::{InternalKey, LookupKey},
    utils::{get_buffer, unmask_checksum},
};
use super::{
    filter_block::FilterBlockReader,
    index_block::IndexBlockIter,
    metaindex_block::MetaindexBlockIter,
};
use super::{
    data_block::{DataBlockIter, TableEntry},
    footer::{BLOCK_FOOTER_LEN, TableFooter},
};


pub(crate) struct TableReader<File, Policy, Pool: BufferPool> {
    file:             File,
    file_number:      FileNumber,
    file_size:        FileSize,
    metaindex_offset: FileOffset,
    index_handle:     BlockHandle,
    index_block:      Pool::PooledBuffer,
    filter_block:     Option<FilterBlockReader<Policy, Pool::PooledBuffer>>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File, Policy, Pool> TableReader<File, Policy, Pool>
where
    File:   RandomAccess,
    Policy: FilterPolicy,
    Pool:   BufferPool,
{
    /// The database lockfile should be held while this reader exists, in order to prevent
    /// `sstable_file` from being unexpectedly modified.
    pub fn new<Cmp, Codecs>(
        sstable_file: File,
        file_number:  FileNumber,
        file_size:    FileSize,
        opts:         &InternalOptions<File, Cmp, Policy, Codecs, Pool>,
        read_opts:    &InternalOptionsPerRead,
        decoders:     &mut Codecs::Decoders,
    ) -> Result<Self, NewTableReaderError<Cmp::InvalidKeyError, Codecs::DecompressionError>>
    where
        Cmp:    LevelDBComparator,
        Codecs: CompressionCodecs,
        Policy: FastMirroredClone,
    {
        // We need to read the footer and the index block, at the very least.
        // Additionally, if a `Policy` was selected, then we need to read the metaindex block
        // and filter block.

        let footer_offset = file_size.0
            .checked_sub(u64::from(TableFooter::ENCODED_LENGTH_U8))
            .ok_or(NewTableReaderError::FileSizeTooShort(file_size))?;
        let footer_offset = FileOffset(footer_offset);

        let mut table_footer = [0; TableFooter::ENCODED_LENGTH];
        sstable_file
            .read_exact_at(footer_offset.0, &mut table_footer)
            .map_err(NewTableReaderError::map_eof_to_truncated(file_size))?;

        let table_footer = TableFooter::decode_from(&table_footer)
            .map_err(|footer_corruption| match footer_corruption {
                TableFooterCorruption::BadTableMagic(bad_magic)
                    => CorruptedTableError::BadTableMagic(bad_magic),
                TableFooterCorruption::Metaindex(err)
                    => CorruptedTableError::CorruptedBlockHandle(
                        BlockType::Metaindex,
                        footer_offset,
                        err,
                    ),
                TableFooterCorruption::Index(offset, err)
                    => CorruptedTableError::CorruptedBlockHandle(
                        BlockType::Index,
                        // Since `file_size` fits in a `u64` and this sum
                        // is at most `file_size`, this sum does not overflow.
                        FileOffset(footer_offset.0 + u64::from(offset)),
                        err,
                    ),
            })
            .map_err(NewTableReaderError::TableCorruption)?;

        let mut block_reader: TableBlockReader<'_, File, Codecs, Pool> = TableBlockReader {
            file:             &sstable_file,
            file_size,
            decoders,
            buffer_pool:      &opts.buffer_pool,
        };

        let existing_buf = &mut None;

        let filter_block = if let Some(policy) = &opts.policy {
            block_reader.read_filter_block(
                policy,
                table_footer.metaindex,
                read_opts.verify_checksums,
                existing_buf,
            )?
        } else {
            None
        };

        let index_block = block_reader.read_table_block(
            BlockType::Index,
            table_footer.index,
            read_opts.verify_checksums,
            existing_buf,
        )?;

        let _iter = IndexBlockIter::new(index_block.as_slice())
            .map_err(|(offset, block_err)| {
                NewTableReaderError::TableCorruption(CorruptedTableError::CorruptedBlock(
                    BlockType::Index,
                    table_footer.index,
                    offset,
                    block_err,
                ))
            })?;

        Ok(Self {
            file:             sstable_file,
            file_number,
            file_size,
            metaindex_offset: table_footer.metaindex.offset,
            index_handle:     table_footer.index,
            index_block,
            filter_block,
        })
    }

    #[must_use]
    pub(super) fn index_iter(&self) -> IndexBlockIter {
        // Validated on construction, and not mutated since then.
        #[expect(
            clippy::expect_used,
            reason = "assuming `ByteBuffer` is implement sanely, this does not panic",
        )]
        IndexBlockIter::new(self.index_block())
            .expect("`IndexBlockIter::new(TableReader.index_block())` is validated to succeed")
    }

    pub(super) fn set_index_iter(&self, index_iter: &mut IndexBlockIter) {
        // Validated on construction, and not mutated since then.
        #[expect(
            clippy::expect_used,
            reason = "assuming `ByteBuffer` is implement sanely, this does not panic",
        )]
        index_iter.set(self.index_block())
            .expect("`IndexBlockIter::new(TableReader.index_block())` is validated to succeed");
    }

    /// Get the most recent entry in this SSTable with user key `lookup_key.0` with a sequence
    /// number no greater than the `lookup_key`'s sequence number.
    ///
    /// Returns `Ok(None)` if and only if there is no such entry in this table.
    ///
    /// To be precise, among the entries in this SSTable with user key `lookup_key.0` whose
    /// sequence numbers are at most `lookup_key.as_internal_key_tag().sequence_number()` (if any),
    /// the entry with the greatest sequence number is returned.
    ///
    /// (This sets aside the potential effects of corruption, which might cause an incorrect
    /// `Ok(_)` return value.)
    ///
    /// # Implementation Details
    /// The correctness of this function depends on many parts of this codebase:
    /// - [`InternalComparator`](crate::table_format::InternalComparator), which has extensive
    ///   comments detailing why its implementation makes this function correct,
    /// - [`InternalFilterPolicy`], whose implementation is depended on by `InternalComparator`'s
    ///   reasoning,
    /// - [`LookupKey`], which restricts the given sequence number to be strictly less than the
    ///   maximum sequence number (which is a detail depended on by the reasoning of
    ///   `InternalComparator`),
    /// - [`CoarserThan`], which is used to constrain the comparator and policy,
    ///
    /// and so on.
    ///
    /// Most reasoning about the correctness of this function is actually deferred to
    /// `InternalComparator`. Here, we need only ensure that we only return `Ok(None)` in the
    /// following four cases, where `min_bound` denotes `lookup_key.as_internal_key()` and
    /// `user_key` denotes `lookup_key.0`:
    ///
    /// #### Case 1
    /// There is no internal key in the SSTable greater than or equal to `min_bound` with a user key
    /// that compares equal to `user_key`.
    ///
    /// #### Case 2
    /// A filter was generated on all keys in the SSTable greater than or equal to `min_bound`, and
    /// that filter did not match `min_bound`.
    ///
    /// #### Case 3
    /// There exist internal keys `from` and `to` which are adjacent in the SSTable such that
    /// `from < to` and a `filter` did not match `min_bound`, where:
    /// - `min_bound <= separator`,
    /// - `separator` is the output of `self.find_short_separator(from, to, _)`, and
    /// - `filter` is a filter generated on (at least) all keys in the SSTable loosely between
    ///   `min_bound` and `separator`.
    ///
    /// #### Case 4
    /// There exist adjacent internal keys `from` and `to` in the SSTable such that
    /// `from < min_bound < to` and `min_bound <= separator`, where `separator` is the output of
    /// `self.find_short_separator(from, to, _)`.
    #[expect(clippy::type_complexity, reason = "still sufficiently readable")]
    pub fn get<Cmp, Codecs>(
        &self,
        lookup_key:   LookupKey<'_>,
        opts:         &InternalOptions<File, Cmp, Policy, Codecs, Pool>,
        read_opts:    &InternalOptionsPerRead,
        decoders:     &mut Codecs::Decoders,
        existing_buf: &mut Option<Pool::PooledBuffer>,
    ) -> Result<
        Option<TableEntry<Pool::PooledBuffer>>,
        ReadTableBlockError<Cmp::InvalidKeyError, Codecs::DecompressionError>,
    >
    where
        Cmp:        LevelDBComparator,
        Codecs:     CompressionCodecs,
        Policy::Eq: CoarserThan<Cmp::Eq>,
    {
        let mut index_iter = self.index_iter();
        index_iter.try_seek(self.index_block(), &opts.cmp, lookup_key.as_internal_key())
            .map_err(|seek_err| ReadTableBlockError::from_seek_err(
                BlockType::Index,
                self.index_handle,
                index_iter.current_entry_offset(),
                seek_err,
            ))?;

        // If we return `Ok(None)` in the `else` branch, then `min_bound` is past the last entry.
        // That's case 1.
        let current = index_iter
            .current_mapped_err(self.index_block())
            .map_err(ReadTableBlockError::TableCorruption)?;
        let Some(data_handle) = current else {
            return Ok(None);
        };

        // In this branch, `[last_key_in_block] <= separator` and `min_bound <= separator`.
        // If there was a previous block, its keys are all strictly less than `min_bound`.
        // Any table entries which are greater than or equal to `min_bound` are thus in
        // this block and any following blocks.
        //
        // Suppose this is the last block. Then, this block contains all entries greater than
        // or equal to `min_bound` (and possibly more), so if the filter for that block does
        // not match and we return `None`, we are within one of the declared cases where
        // `Ok(None)` might be returned (in particular, case 2).
        //
        // Otherwise, there is a following block, so `separator` was returned from
        // `TableCmp::find_short_separator` applied to `[last_key_in_block]` and an adjacent,
        // strictly-greater `[some_key_in_next_block]`.
        //
        // The block whose filter we check contains all table entries up to and including
        // `separator`, and there are no entries strictly less than `min_bound` which we miss.
        // If the filter for keys including everything between `min_bound` and `separator`,
        // inclusive, does not match `min_bound` and we return `None`, then we are within
        // one of the declared cases where `Ok(None)` might be returned (namely, case 3).
        if let Some(filter_block) = &self.filter_block {
            match filter_block.key_may_match(data_handle, lookup_key.0) {
                Ok(false) => return Ok(None),
                Ok(true) => {},
                Err(filter_err) => {
                    // TODO: log error, optionally hard error, else treat error as though
                    // it's `OK(true)`.

                    return Err(ReadTableBlockError::TableCorruption(
                        CorruptedTableError::CorruptedFilterBlock(
                            filter_block.filter_block_handle(),
                            filter_err,
                        ),
                    ));
                }
            }
        }

        let block_buf = self.read_data_block(
            data_handle,
            opts,
            read_opts,
            decoders,
            existing_buf,
        )?;

        let mut block_iter = DataBlockIter::new(block_buf.as_slice())
            .map_err(|(offset, block_err)| {
                ReadTableBlockError::TableCorruption(CorruptedTableError::CorruptedBlock(
                    BlockType::Data,
                    data_handle,
                    offset,
                    block_err,
                ))
            })?;
        block_iter.try_seek(block_buf.as_slice(), &opts.cmp, lookup_key.as_internal_key())
            .map_err(|seek_err| ReadTableBlockError::from_seek_err(
                BlockType::Data,
                data_handle,
                block_iter.current_entry_offset(),
                seek_err,
            ))?;

        // If `TableEntry::new` returns `None`, then:
        // - `[some_key_in_block] < min_bound` for each key in this block, else we'd have
        //   found and returned the entry corresponding to a GEQ key;
        // - if there is not a next block, `min_bound` is strictly past the last entry
        //   in the `Table`;
        // - if there is a next block,
        //   `last_key_in_block < min_bound <= separator < first_key_in_next_block`.
        //
        // That's case 4, since `last_key_in_block` and `first_key_in_next_block` are adjacent.
        //
        // If this returns `Some`, then it's the first entry **with the same user key** which is
        // greater than or equal to `min_bound` in the first block whose keys are not bounded above
        // by an index strictly less than `min_bound`.
        //
        // TLDR: we satisfy the documentation of this `Table::get` method.
        if block_iter.valid() {
            let entry_offset = block_iter.current_entry_offset();
            let (key, value) = block_iter.into_raw_current();

            let entry = TableEntry::new(block_buf, key, value, &opts.cmp)
                .map_err(|invalid_key| {
                    ReadTableBlockError::TableCorruption(
                        CorruptedTableError::InvalidInternalKey(
                            BlockType::Data,
                            data_handle,
                            entry_offset,
                            invalid_key,
                        ),
                    )
                })?;

            if opts.cmp.cmp_user(
                entry.key().as_internal_key().0,
                lookup_key.0,
            ).is_eq() {
                Ok(Some(entry))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn approximate_offset_of_key<Cmp: LevelDBComparator, Codecs>(
        &self,
        opts: &InternalOptions<File, Cmp, Policy, Codecs, Pool>,
        key:  InternalKey<'_>,
    ) -> FileOffset {
        // If the `key` is greater than the largest key in this table *or* the index block is
        // corrupt, we return `metaindex_offset`. With the way that SSTables are ordinarily
        // written, the `metaindex_offset` is the offset just after the last data block, so at
        // least if the index block isn't corrupt, the answer is reasonable.

        let index_block = self.index_block.as_slice();

        let Ok(mut index_iter) = IndexBlockIter::new(index_block) else {
            return self.metaindex_offset;
        };

        let Ok(()) = index_iter.try_seek(self.index_block(), &opts.cmp, key) else {
            return self.metaindex_offset;
        };

        let Ok(Some(block_handle)) = index_iter.current(self.index_block()) else {
            return self.metaindex_offset;
        };

        block_handle.offset
    }

    /// Used by [`TableIter`].
    pub(super) fn index_block(&self) -> &[u8] {
        self.index_block.as_slice()
    }

    /// Used by [`TableIter`].
    pub(super) const fn index_handle(&self) -> BlockHandle {
        self.index_handle
    }

    /// Read and cache the data block with the given handle,
    /// and return the block contents on success.
    ///
    /// Used by [`TableIter`].
    pub(super) fn read_data_block<Cmp, Codecs>(
        &self,
        handle:       BlockHandle,
        opts:         &InternalOptions<File, Cmp, Policy, Codecs, Pool>,
        read_opts:    &InternalOptionsPerRead,
        decoders:     &mut Codecs::Decoders,
        existing_buf: &mut Option<Pool::PooledBuffer>,
    ) -> Result<
        Arc<Pool::PooledBuffer>,
        ReadTableBlockError<Cmp::InvalidKeyError, Codecs::DecompressionError>,
    >
    where
        Cmp:    LevelDBComparator,
        Codecs: CompressionCodecs,
    {
        let cache_key = BlockCacheKey {
            table_number: self.file_number,
            block_offset: handle.offset,
        };

        opts.block_cache.get_or_insert_with(cache_key, || {
            let mut block_reader: TableBlockReader<'_, File, Codecs, Pool> = TableBlockReader {
                file:        &self.file,
                file_size:   self.file_size,
                decoders,
                buffer_pool: &opts.buffer_pool,
            };

            let data_block = block_reader.read_table_block(
                BlockType::Data,
                handle,
                read_opts.verify_checksums,
                existing_buf,
            )?;

            Ok(Arc::new(data_block))
        })
    }
}

impl<File, Policy, Pool> Debug for TableReader<File, Policy, Pool>
where
    File:   Debug,
    Policy: Debug,
    Pool:   BufferPool<PooledBuffer: Debug>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("TableReader")
            .field("file",             &self.file)
            .field("file_number",      &self.file_number)
            .field("file_size",        &self.file_size)
            .field("metaindex_offset", &self.metaindex_offset)
            .field("index_handle",     &self.index_handle)
            .field("index_block",      &self.index_block)
            .field("filter_block",     &self.filter_block)
            .finish()
    }
}

/// A short-lived reader for any table block.
#[derive(Debug)]
struct TableBlockReader<'a, File, Codecs: CompressionCodecs, Pool> {
    pub file:        &'a File,
    pub file_size:   FileSize,
    pub decoders:    &'a mut Codecs::Decoders,
    pub buffer_pool: &'a Pool,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File, Codecs, Pool> TableBlockReader<'_, File, Codecs, Pool>
where
    File:   RandomAccess,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// Attempts to read the block associated with the `block_handle` from `self.file`.
    ///
    /// Returns `Ok(block_contents)` if the entire block was successfully read.
    pub fn read_table_block<InvalidKey>(
        &mut self,
        block_type:      BlockType,
        block_handle:    BlockHandle,
        verify_checksum: bool,
        existing_buf:    &mut Option<Pool::PooledBuffer>,
    ) -> Result<
        Pool::PooledBuffer,
        ReadTableBlockError<InvalidKey, Codecs::DecompressionError>,
    > {
        macro_rules! compressed_block_err {
            ($($err_tokens:tt)*) => {
                ReadTableBlockError::TableCorruption(
                    CorruptedTableError::CorruptedCompressedBlock(
                        block_type,
                        block_handle,
                        CompressedBlockError::$($err_tokens)*
                    ),
                )
            };
        }

        let block_size = usize::try_from(block_handle.size.0)
            .map_err(|_overflow| ReadTableBlockError::BlockUsizeOverflow(block_handle))?;
        let block_size_with_footer = block_size
            .checked_add(BLOCK_FOOTER_LEN)
            .ok_or(ReadTableBlockError::BlockUsizeOverflow(block_handle))?;

        // Note that the returned buffer has length exactly `block_size_with_footer`.
        let mut compressed_buf = get_buffer(
            self.buffer_pool,
            existing_buf,
            block_size_with_footer,
        )?;

        if let Err(io_err) = self.file.read_exact_at(
            block_handle.offset.0,
            compressed_buf.as_mut_slice(),
        ) {
            *existing_buf = Some(compressed_buf);
            return Err(ReadTableBlockError::map_eof_to_truncated(self.file_size)(io_err));
        }

        // Note that `compressed_buf.len() == block_size_with_footer >= block_size`.
        // Therefore, this should not panic.
        let (compressed_block_data, footer) = compressed_buf
            .as_slice()
            .split_at(block_size);

        // `compressed_buf.len() - block_size == block_size_with_footer - block_size`,
        // which is `BLOCK_FOOTER_LEN`.
        #[expect(clippy::unwrap_used, reason = "guaranteed to be the correct length")]
        let footer: &[u8; BLOCK_FOOTER_LEN] = footer.try_into().unwrap();

        let compressor_id = footer[0];
        #[expect(clippy::unwrap_used, reason = "`BLOCK_FOOTER_LEN == 5 >= 4`")]
        let masked_checksum = u32::from_le_bytes(*footer.last_chunk::<4>().unwrap());

        if verify_checksum {
            let unmasked_checksum = unmask_checksum(masked_checksum);
            // Compute the checksum of everything except the checksum that was appended.
            // That is, the checksum of the block and the appended `compressor_id`, but not
            // the `masked_checksum`.
            let checksum_of_block = crc32c::crc32c(compressed_block_data);
            let actual_checksum = crc32c::crc32c_append(checksum_of_block, &[compressor_id]);

            if unmasked_checksum != actual_checksum {
                *existing_buf = Some(compressed_buf);
                return Err(compressed_block_err!(
                    ChecksumMismatch(unmasked_checksum, actual_checksum),
                ));
            }
        }

        if let Some(compressor_id) = NonZeroU8::new(compressor_id).map(CompressorId) {
            let decompression_result = Codecs::decode(
                self.decoders,
                compressed_block_data,
                compressor_id,
                self.buffer_pool,
                &mut None,
            );

            let decompression_result = decompression_result.map_err(|err| {
                match err {
                    CodecsDecompressionError::Unsupported => compressed_block_err!(
                        UnsupportedDecompressor(
                            compressor_id,
                            // This clone is expensive, but should not happen normally anyway.
                            compressed_block_data.to_vec(),
                        ),
                    ),
                    CodecsDecompressionError::BufferAlloc => ReadTableBlockError::BufferAllocErr,
                    CodecsDecompressionError::Custom(err) => compressed_block_err!(
                        Decompression(
                            compressor_id,
                            // This clone is expensive, but should not happen normally anyway.
                            compressed_block_data.to_vec(),
                            err,
                        ),
                    ),
                }
            });

            *existing_buf = Some(compressed_buf);

            decompression_result
        } else {
            // `compressor_id` was `0` (no compression).
            // Does not panic, since `compressed_buf.capacity() >= compressed_buf.len()`
            // which is (before this call) `block_size_with_footer`, which is greater than
            // `block_size`.
            compressed_buf.set_len(block_size);
            Ok(compressed_buf)
        }
    }

    /// Attempts to read the filter block in `self.file` associated with the given `policy`.
    ///
    /// Returns `Ok(None)` if there is no such filter block - which is valid -
    /// and `Ok(Some(_))` if the filter block was found and successfully read.
    ///
    /// The `metaindex_handle` should be the handle of the metaindex block of this SSTable file.
    ///
    /// Note that the checksum for the filter block is always validated, since there's otherwise
    /// little chance of detecting corruption in the filter block.
    #[expect(clippy::type_complexity, reason = "still sufficiently readable")]
    pub fn read_filter_block<InvalidKey, Policy>(
        &mut self,
        policy:                    &InternalFilterPolicy<Policy>,
        metaindex_handle:          BlockHandle,
        verify_metaindex_checksum: bool,
        existing_buf:              &mut Option<Pool::PooledBuffer>,
    ) -> Result<
        Option<FilterBlockReader<Policy, Pool::PooledBuffer>>,
        ReadTableBlockError<InvalidKey, Codecs::DecompressionError>,
    >
    where
        Policy: FilterPolicy + FastMirroredClone,
    {
        if metaindex_handle.size.0 <= 4 {
            // If the metaindex block is contains only the `num_restarts` value (or less),
            // which is a `u32` (size: 4 bytes), then either that block is corrupt
            // (either it lacks the full four bytes or has a nonzero `num_restarts`,
            // despite having no restarts), or it's empty and there's no filter.
            // It isn't mandatory for us to report every possible corruption error, so we can
            // just stop here and say there's no filter.
            return Ok(None);
        }
        let metaindex_block = self.read_table_block(
            BlockType::Metaindex,
            metaindex_handle,
            verify_metaindex_checksum,
            // The metaindex block is likely to be *much* smaller than most blocks, so there's no
            // point in trying to reuse a buffer for it.
            &mut None,
        )?;
        let metaindex_block = metaindex_block.as_slice();

        let mut metaindex_iter = MetaindexBlockIter::new(metaindex_block)
            .map_err(|(offset, block_err)| {
                ReadTableBlockError::TableCorruption(CorruptedTableError::CorruptedBlock(
                    BlockType::Metaindex,
                    metaindex_handle,
                    offset,
                    block_err,
                ))
            })?;

        let filter_block_handle = metaindex_iter.get_filter_handle(policy)
            .map_err(|metaindex_err| {
                match metaindex_err {
                    MetaindexIterError::Block(block_err) => ReadTableBlockError::TableCorruption(
                        CorruptedTableError::CorruptedBlock(
                            BlockType::Metaindex,
                            metaindex_handle,
                            metaindex_iter.current_entry_offset(),
                            block_err,
                        ),
                    ),
                    MetaindexIterError::Handle(handle_err) => ReadTableBlockError::TableCorruption(
                        CorruptedTableError::CorruptedFilterBlockHandle(
                            metaindex_iter.current_value_offset(),
                            handle_err,
                        ),
                    ),
                }
            })?;

        let Some(filter_block_handle) = filter_block_handle else {
            return Ok(None);
        };

        let filter_block = self.read_table_block(
            BlockType::Filter,
            filter_block_handle,
            // Always verify filter checksums.
            true,
            existing_buf,
        )?;
        let policy = policy.fast_mirrored_clone();

        let filter_reader = FilterBlockReader::new(policy, filter_block, filter_block_handle)
            .map_err(|filter_err| {
                ReadTableBlockError::TableCorruption(
                    CorruptedTableError::CorruptedFilterBlock(
                        filter_block_handle,
                        filter_err,
                    ),
                )
            })?;

        Ok(Some(filter_reader))
    }
}
