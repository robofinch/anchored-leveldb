use crate::{table_format::InternalFilterPolicy, typed_bytes::UserKey};
use crate::{
    all_errors::types::{CorruptedFilterBlockError, FilterBuildError},
    pub_traits::{cmp_and_policy::FilterPolicy, pool::ByteBuffer},
    pub_typed_bytes::{BlockHandle, FileOffset, MinU32Usize, TableBlockOffset},
};


/// Length of the footer of a filter block, in bytes.
const FOOTER_LEN: usize = const {
    let footer_len: usize = 5;

    assert!(
        footer_len == size_of::<u32>() + size_of::<u8>(),
        "Show why it's the magic value 5",
    );

    footer_len
};


/// A `FilterBlockBuilder` is used to generate the filters for all of the normal blocks of an
/// SSTable, producing a single filter block as output.
///
/// For each block in the SSTable, `self.start_block(_)` must be called, followed by
/// `self.add_key(_)` for each key in the block. These operations must be done in the order that the
/// blocks are in the table, and in the order that keys are in each block. After all blocks have
/// been processed, `self.finish()` should be called. No `FilterBlockBuilder` methods should
/// be called on `self` after `self.finish()` is called, except `self.reset()`.
///
/// As a special exception, `self.start_block(0)` may always be elided.
///
/// No harm is done if the process is entirely aborted and the `FilterBlockBuilder` is dropped.
/// However, the build operations must be done in the correct order, or else a corrupt filter
/// block might be produced.
///
/// # Errors
/// If constructing a filter for an SSTable fails, then no filter block should be added to that
/// SSTable. (In practice, such an error could occur if e.g. 4 GiB of filters are generated for a
/// single SSTable, if hundreds of millions of entries are added to a single table, and so on.
/// Not likely, but not inconceivable.)
#[derive(Debug)]
pub(super) struct FilterBlockBuilder<Policy> {
    policy:                  InternalFilterPolicy<Policy>,
    filter_chunk_size_log2:  u8,
    flattened_filters:       Vec<u8>,
    /// Each offset is the start of a filter in `flattened_filters`.
    filter_offsets:          Vec<MinU32Usize>,
    /// This is reset each time `self.start_block()` is called.
    flattened_user_key_data: Vec<u8>,
    /// Each index is the start of a key in `flattened_user_key_data`.
    ///
    /// This is reset each time `self.start_block()` is called.
    user_key_offsets:        Vec<usize>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Policy> FilterBlockBuilder<Policy> {
    #[inline]
    #[must_use]
    pub const fn new(policy: InternalFilterPolicy<Policy>, filter_chunk_size_log2: u8) -> Self {
        Self {
            policy,
            filter_chunk_size_log2,
            flattened_filters:       Vec::new(),
            filter_offsets:          Vec::new(),
            flattened_user_key_data: Vec::new(),
            user_key_offsets:        Vec::new(),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.flattened_filters.clear();
        self.filter_offsets.clear();
        self.flattened_user_key_data.clear();
        self.user_key_offsets.clear();
    }

    /// Returns an estimate for the length of the slice returned by `self.finish()`, or zero if
    /// `self.finish()` would certainly return `None` (or panic/abort due to OOM).
    #[must_use]
    pub fn estimated_finished_length(&self) -> usize {
        // Since `self.flattened_filters: Vec<u8>`, its length is at most
        // `isize::MAX / sizeof::<u8> == isize::MAX`.
        // Since `self.filter_offsets: Vec<MinU32Usize>`, its length is at most
        // `isize::MAX / sizeof::<MinU32Usize>`, so the product is at most `isize::MAX`.
        // Since `isize::MAX + isize::MAX` does not overflow `usize`, it follows that
        // evaluating `data_len` does not overflow. However, adding `FOOTER_LEN` could overflow.
        let data_len = self.flattened_filters.len()
            + self.filter_offsets.len() * size_of::<MinU32Usize>();
        FOOTER_LEN.checked_add(data_len).unwrap_or(0)
    }

    #[inline]
    #[must_use]
    pub const fn policy(&self) -> &InternalFilterPolicy<Policy> {
        &self.policy
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Policy: FilterPolicy> FilterBlockBuilder<Policy> {
    /// The provided `block_offset` must be greater than the offset of any previously-started
    /// block.
    pub fn start_block(
        &mut self,
        block_offset: FileOffset,
    ) -> Result<(), FilterBuildError<Policy::FilterError>> {
        let filter_index = block_offset.0 >> self.filter_chunk_size_log2;

        // The below is essentially:
        // ```rust
        // while filter_index > self.filter_offsets.len() {
        //     self.generate_filter()?;
        // }
        // ```
        // except accounting for usize/u64.

        loop {
            #[expect(clippy::expect_used, reason = "could theoretically panic, but won't")]
            let filter_offsets_len = u64::try_from(self.filter_offsets.len())
                .expect("A single Vec should never have a length measured in exabytes");

            // Notice that for `block_offset == filter_offset == 0`, this immediately `break`s
            // without doing anything, thus why the first block does not need to have anything
            // called to start it.
            if filter_index <= filter_offsets_len {
                break;
            }

            // The first loop creates the filter for previous block(s), and the rest of the loops
            // are generating empty iterators for any 2kB chunks which do not contain the
            // `block_offset` of any block.
            self.generate_filter()?;
        }

        Ok(())
    }

    /// The provided `user_key` must be greater than any key previously added in the current block.
    ///
    /// See [`FilterBlockBuilder`] for more.
    pub fn add_key(&mut self, user_key: UserKey<'_>) {
        self.user_key_offsets.push(self.flattened_user_key_data.len());
        self.flattened_user_key_data.extend(user_key.inner());
    }

    /// Finish writing the filter block.
    ///
    /// After calling this method, no [`FilterBlockBuilder`] methods should be used, aside from
    /// `self.reset()`. See the type-level documentation for more.
    ///
    /// # Errors
    /// If constructing a filter for an SSTable fails and `None` is returned, then no filter block
    /// should be added to that SSTable. (In practice, such an error could occur if e.g. 4 GiB of
    /// filters are generated for a single SSTable, if hundreds of millions of entries are added to
    /// a single table, and so on. Not likely, but not inconceivable.)
    pub fn finish(&mut self) -> Result<&[u8], FilterBuildError<Policy::FilterError>> {
        if !self.user_key_offsets.is_empty() {
            self.generate_filter()?;
        }

        // Add the array of filter offsets.
        let start_of_offsets = self.flattened_filters.len();
        let start_of_offsets = u32::try_from(start_of_offsets)
            .map_err(|_overflow| FilterBuildError::FilterLenOverflowsU32(start_of_offsets))?;

        // Don't care about overflow. Either this wraps, in which case `reserve_exact` reserves
        // `0`, `1`, or `2` bytes instead of near `usize::MAX` (after which the below code OOM's),
        // or it panics (in which case the below code would've OOM'd), or it does not overflow.
        // (Also, the sum can only overflow if the pointer size is `16` bits.)
        self.flattened_filters.reserve_exact(
            self.filter_offsets.len() * size_of::<u32>() + FOOTER_LEN,
        );

        for offset in &self.filter_offsets {
            self.flattened_filters.extend(u32::from(*offset).to_le_bytes());
        }

        // Add the footer
        self.flattened_filters.extend(start_of_offsets.to_le_bytes());
        self.flattened_filters.push(self.filter_chunk_size_log2);

        Ok(&self.flattened_filters)
    }
}

impl<Policy: FilterPolicy> FilterBlockBuilder<Policy> {
    /// Returns `Ok(())` if and only if a filter was successfully generated.
    fn generate_filter(&mut self) -> Result<(), FilterBuildError<Policy::FilterError>> {
        let filter_offset = self.flattened_filters.len();
        let filter_offset = MinU32Usize::from_usize(filter_offset)
            .ok_or(FilterBuildError::FilterLenOverflowsU32(filter_offset))?;

        self.filter_offsets.push(filter_offset);

        if self.flattened_user_key_data.is_empty() {
            return Ok(());
        }

        self.policy.create_filter(
            &self.flattened_user_key_data,
            &self.user_key_offsets,
            &mut self.flattened_filters,
        ).map_err(FilterBuildError::UserError)?;

        self.flattened_user_key_data.clear();
        self.user_key_offsets.clear();
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct FilterBlockReader<Policy, PooledBuffer> {
    policy:              InternalFilterPolicy<Policy>,
    filter_block:        PooledBuffer,
    filter_block_handle: BlockHandle,
    start_of_offsets:    MinU32Usize,
    filter_base_log2:    u8,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Policy, PooledBuffer: ByteBuffer> FilterBlockReader<Policy, PooledBuffer> {
    pub fn new(
        policy:              InternalFilterPolicy<Policy>,
        filter_block:        PooledBuffer,
        filter_block_handle: BlockHandle,
    ) -> Result<Self, CorruptedFilterBlockError> {
        let footer: [u8; 5] = *filter_block.as_slice()
            .last_chunk()
            .ok_or(CorruptedFilterBlockError::TruncatedFilterBlock)?;

        #[expect(
            clippy::unwrap_used,
            reason = "Converting a length-4 slice to a length-`size_of::<u32>` array succeeds",
        )]
        let (start_of_offsets, filter_base_log2) = (
            footer[..4].try_into().unwrap(),
            footer[4],
        );

        let start_of_offsets = u32::from_le_bytes(start_of_offsets);

        let start_of_offsets = MinU32Usize::from_u32(start_of_offsets)
            .ok_or(CorruptedFilterBlockError::InvalidFilterOffsetsOffset(start_of_offsets))?;

        Ok(Self {
            policy,
            filter_block,
            filter_block_handle,
            start_of_offsets,
            filter_base_log2,
        })
    }

    /// Returns whether the `key` matches the filter for the data block with the given handle
    /// within the table associated with the filter block being read.
    pub fn key_may_match(
        &self,
        data_block_handle: BlockHandle,
        key:               UserKey<'_>,
    ) -> Result<bool, CorruptedFilterBlockError>
    where
        Policy: FilterPolicy,
    {
        let filter_index = data_block_handle.offset.0 >> self.filter_base_log2;

        let Some((start_offset, start_u32, end_u32)) = self.get_filter_offsets(filter_index) else {
            // This block has no filter.
            return Err(CorruptedFilterBlockError::FiltersTooShort(data_block_handle));
        };

        let (Ok(start), Ok(end)) = (usize::try_from(start_u32), usize::try_from(end_u32)) else {
            // One or both offsets are out of bounds.
            return Err(CorruptedFilterBlockError::InvalidFilterOffsets(
                start_offset,
                start_u32,
                end_u32,
            ));
        };

        let Some(filter) = self.filter_block.as_slice().get(start..end) else {
            // Either one or both of the offsets is out-of-bounds,
            // or they are out-of-order.
            return Err(CorruptedFilterBlockError::InvalidFilterOffsets(
                start_offset,
                start_u32,
                end_u32,
            ));
        };

        if filter.is_empty() {
            // Empty filters, of any `FilterPolicy`, are required to not match any keys.
            Ok(false)
        } else {
            Ok(self.policy.key_may_match(key, filter))
        }
    }

    #[inline]
    #[must_use]
    pub const fn filter_block_handle(&self) -> BlockHandle {
        self.filter_block_handle
    }

    /// # Errors
    /// Returns `None` if the filter block is too short, and does not have a filter at the
    /// given index.
    #[must_use]
    fn get_filter_offsets(&self, filter_index: u64) -> Option<(TableBlockOffset, u32, u32)> {
        let filter_index = usize::try_from(filter_index).ok()?;

        // The offset of the (offset of the) start of the filter.
        let start_offset = usize::from(self.start_of_offsets)
            .checked_add(filter_index.checked_mul(size_of::<u32>())?)?;

        // The (offset of the) start of the filter,
        // followed by the (offset of the) end of the filter.
        let filter_offset_data = self.filter_block.as_slice().get(start_offset..)?;

        // The (offset of the) start of the filter.
        let (&start, filter_offset_data) = filter_offset_data.split_first_chunk::<4>()?;
        // The (offset of the) end of the filter.
        // NOTE: If `filter_index` is the index of the last filter in this block, then this
        // value may be the `u32` value of `self.start_of_offsets`. If `filter_index` is equal to
        // the number of filters in this block, then since the filter block footer is only 5 bytes
        // in length (rather than 8 or more bytes), this `.first_chunk()` call will try to get the
        // first 4 bytes of of the 1 remaining byte, resulting in `None` being returned. (If
        // `filter_index` is even greater, then `split_first_chunk` returns `None`.)
        let &end = filter_offset_data.first_chunk::<4>()?;

        Some((
            TableBlockOffset(start_offset),
            u32::from_le_bytes(start),
            u32::from_le_bytes(end),
        ))
    }
}
