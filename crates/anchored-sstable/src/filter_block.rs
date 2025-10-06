use std::mem;
use std::borrow::Borrow;

use crate::internal_utils::U32_BYTES;
use crate::filters::{FILTER_KEY_LENGTH_LIMIT, FILTER_NUM_KEYS_LIMIT, TableFilterPolicy};


/// Existing implementations of LevelDB currently hardcode that one filter is created per
/// 2048 bytes of data (which is 2^11).
const DEFAULT_FILTER_BASE_LOG2: u8 = 11;
/// Length of the footer of a filter block, in bytes.
const FOOTER_LEN: usize = 5;


/// A `FilterBlockBuilder` is used to generate the filters for all of the normal blocks of a
/// [`Table`], producing a single filter block as output.
///
/// For each block in the [`Table`], `self.start_block(_)` must be called, followed by
/// `self.add_key(_)` for each key in the block. These operations must be done in the order that the
/// blocks are in the table, and in the order that keys are in each block. After all blocks have
/// been processed, `self.finish()` should be called. No `FilterBlockBuilder` methods should
/// be called on `self` after `self.finish()` is called, except `self.reuse_as_new(_)` and
/// `self.swap_buffer(_)`.
///
/// `self.swap_buffer(_)` is intended to be called after `self.finish()` and before
/// `self.reuse_as_new(_)`; it can completely overwrite the buffer that this builder uses for
/// filters, and thus enable the creation of an invalid filter block. Thus, after calling
/// `self.swap_buffer(_)`, no other `FilterBlockBuilder` methods should be called except
/// `self.reuse_as_new(_)`, unless it is certain that the swapped buffer is valid (e.g. swapping
/// a valid buffer out and back in).
///
/// No harm is done if the process is entirely aborted and the `FilterBlockBuilder` is dropped.
/// However, the build operations must be done in the correct order, or else an invalid filter
/// block might be produced, and panics may occur.
///
/// # Limits
/// The total size of key data added to each block, accounting for the effects of applying
/// `Policy::append_key_data` to each key, must be strictly less than 4 gigabytes;
/// see [`FILTER_KEY_LENGTH_LIMIT`]. At most 2^24 keys (around 16.7 million) may be added to a
/// single block; see [`FILTER_NUM_KEYS_LIMIT`]. Additionally, the total length of generated
/// filters, across all blocks in the table, must not reach 4 gigabytes.
///
/// If 16.7 million keys were added to a single block using a [`BloomPolicy`] with the maximum bits
/// per key, the result would be around 86 megabytes of filters. 47 such blocks could be
/// added to a single table before reaching 4 gigabytes of filters, amounting to over 788 million
/// entries added to a single table. If each entry were at least sixteen bytes in size, as is
/// always the case with LevelDB internal entries, the total entry data would exceed 11 gigabytes.
///
/// If one key were added per block with any [`BloomPolicy`], then if `N` keys were added to the
/// table, `9N` bytes of filters would be generated. Over 477 million entries, each exceeding
/// the maximum block size setting (by default 4096 bytes) in order to force each entry to have its
/// own block, would need to be added to a single table in order to reach 4 gigabytes of filters.
/// This would again amount to gigabytes of entry data added to a single table.
///
/// Do not get close to these limits.
///
/// [`BloomPolicy`]: crate::filters::BloomPolicy
/// [`Table`]: crate::table::Table
// TODO: in the TableBuilder implementation, document limits like this, and provide functions
// that would allow catching such errors.
#[derive(Debug)]
pub struct FilterBlockBuilder<Policy> {
    policy:             Policy,
    flattened_filters:  Vec<u8>,
    /// Each offset is the start of a filter in `flattened_filters`.
    filter_offsets:     Vec<u32>,
    /// This is reset each time `self.start_block()` is called.
    flattened_key_data: Vec<u8>,
    /// Each index is the start of a key in `flattened_key_data`
    ///
    /// This is reset each time `self.start_block()` is called.
    key_indices:        Vec<usize>,
}

impl<Policy> FilterBlockBuilder<Policy> {
    #[inline]
    #[must_use]
    pub const fn new(policy: Policy) -> Self {
        Self {
            policy,
            flattened_filters:  Vec::new(),
            filter_offsets:     Vec::new(),
            flattened_key_data: Vec::new(),
            key_indices:        Vec::new(),
        }
    }

    /// Create a mostly-new `FilterBlockBuilder`, but reuse `self`'s buffer capacities.
    #[inline]
    pub fn reuse_as_new(&mut self) {
        self.flattened_filters.clear();
        self.filter_offsets.clear();
        self.flattened_key_data.clear();
        self.key_indices.clear();
    }

    /// Returns the exact length which the buffer returned by `self.finish()` would have if that
    /// function were called now.
    #[must_use]
    pub fn finished_length(&self) -> usize {
        self.flattened_filters.len() + self.filter_offsets.len() * U32_BYTES + FOOTER_LEN
    }

    #[inline]
    #[must_use]
    pub const fn policy(&self) -> &Policy {
        &self.policy
    }

    /// `self.swap_buffer(_)` is intended to be called after `self.finish()` and before
    /// `self.reuse_as_new(_)`; it can completely overwrite the buffer that this builder uses for
    /// filters, and thus enable the creation of an invalid filter block. Thus, after calling
    /// `self.swap_buffer(_)`, no other `FilterBlockBuilder` methods should be called except
    /// `self.reuse_as_new(_)`, unless it is certain that the swapped buffer is valid (e.g.
    /// swapping a valid buffer out and back in).
    #[inline]
    pub const fn swap_buffer(&mut self, buffer: &mut Vec<u8>) {
        mem::swap(&mut self.flattened_filters, buffer);
    }
}

impl<Policy: TableFilterPolicy> FilterBlockBuilder<Policy> {
    /// The provided `block_offset` must be greater than the offset of any previously-started
    /// block.
    ///
    /// Additionally, the total size of key data added to each block must be at most one megabyte;
    /// see [`FILTER_KEYS_LENGTH_LIMIT`].
    ///
    /// See [`FilterBlockBuilder`] for more.
    ///
    /// [`FILTER_KEYS_LENGTH_LIMIT`]: FILTER_KEYS_LENGTH_LIMIT
    pub fn start_block(&mut self, block_offset: usize) {
        let filter_index = block_offset >> DEFAULT_FILTER_BASE_LOG2;

        while filter_index > self.filter_offsets.len() {
            // The first loop creates the filter for previous block(s), and the rest of the loops
            // are generating empty iterators for any 2kB chunks which do not contain the
            // `block_offset` of any `Block`.
            self.generate_filter();
        }
    }

    /// The provided `key` must be greater than any key previously added in the current block.
    ///
    /// See [`FilterBlockBuilder`] for more.
    ///
    /// # Panics
    /// Panics if adding the key would result in more than [`FILTER_KEY_LENGTH_LIMIT`] bytes
    /// of key data associated with the current block, or if the number of keys added to the current
    /// block would exceed [`FILTER_NUM_KEYS_LIMIT`].
    ///
    /// Note that the key data is not necessarily equivalent to concatenating the keys together;
    /// see [`TableFilterPolicy::append_key_data`].
    pub fn add_key(&mut self, key: &[u8]) {
        self.key_indices.push(self.flattened_key_data.len());
        self.policy.append_key_data(key, &mut self.flattened_key_data);

        let filter_key_length_limit = usize::try_from(FILTER_KEY_LENGTH_LIMIT)
            .unwrap_or(usize::MAX);
        assert!(
            self.flattened_key_data.len() <= filter_key_length_limit,
            "Attempted to add more than FILTER_KEY_LENGTH_LIMIT bytes (4 gigabytes) \
             of key data to a single block",
        );
        let filter_num_keys_limit = usize::try_from(FILTER_NUM_KEYS_LIMIT)
            .unwrap_or(usize::MAX);
        assert!(
            self.key_indices.len() <= filter_num_keys_limit,
            "Attempted to add more than FILTER_NUM_KEYS_LIMIT (16.7 million) \
             keys to a single block",
        );
    }

    /// Finish writing the filter block.
    ///
    /// After calling this method, no [`FilterBlockBuilder`] methods should be used, aside from
    /// `self.reuse_as_new(_)` and `self.swap_buffer(_)`. See the type-level documentation for more.
    ///
    /// # Panics
    /// Panics if the length of generated filters is 4 gigabytes or more.
    #[must_use]
    pub fn finish(&mut self) -> &mut Vec<u8> {
        if !self.key_indices.is_empty() {
            self.generate_filter();
        }

        // Add the array of filter offsets
        #[expect(clippy::unwrap_used, reason = "panic is documented, and immensely unlikely")]
        let start_of_offsets = u32::try_from(self.flattened_filters.len()).unwrap();

        self.flattened_filters.reserve_exact(
            self.filter_offsets.len() * U32_BYTES + FOOTER_LEN,
        );

        for offset in &self.filter_offsets {
            self.flattened_filters.extend(offset.to_le_bytes());
        }

        // Add the footer
        self.flattened_filters.extend(start_of_offsets.to_le_bytes());
        self.flattened_filters.push(DEFAULT_FILTER_BASE_LOG2);

        &mut self.flattened_filters
    }
}

impl<Policy: TableFilterPolicy> FilterBlockBuilder<Policy> {
    /// # Panics
    /// Panics if the length of generated filters somehow manages to exceed 4 gigabytes.
    ///
    /// Do not let a table become long enough that this is remotely possible.
    fn generate_filter(&mut self) {
        #[expect(clippy::unwrap_used, reason = "panic is documented, and immensely unlikely")]
        self.filter_offsets.push(u32::try_from(self.flattened_filters.len()).unwrap());
        if self.flattened_key_data.is_empty() {
            return;
        }

        self.policy.create_filter(
            &self.flattened_key_data,
            &self.key_indices,
            &mut self.flattened_filters,
        );

        self.flattened_key_data.clear();
        self.key_indices.clear();
    }
}

#[derive(Debug)]
pub struct FilterBlockReader<Policy, BlockContents> {
    policy:           Policy,
    filter_block:     BlockContents,

    start_of_offsets: usize,
    filter_base_log2: u8,
}

impl<Policy, BlockContents: Borrow<Vec<u8>>> FilterBlockReader<Policy, BlockContents> {
    /// # Panics
    /// Panics if `filter_block` is not at least 5 bytes long (as all valid filter blocks are).
    ///
    /// Other methods of `FilterBlockReader` may also panic if `filter_block` is not a valid filter
    /// block.
    #[must_use]
    pub fn new(policy: Policy, filter_block: BlockContents) -> Self {
        #![expect(clippy::indexing_slicing, reason = "panics are covered by `assert!` and docs")]

        let borrowed = filter_block.borrow();
        let len = borrowed.len();

        assert!(
            len >= FOOTER_LEN,
            "expected a valid filter block, which must have at least 5 bytes",
        );

        let filter_base_log2 = borrowed[len - 1];

        let start_of_offsets = &borrowed[len - 5..len - 1];
        #[expect(clippy::unwrap_used, reason = "the slice is the correct length, 4 bytes")]
        let start_of_offsets = u32::from_le_bytes(start_of_offsets.try_into().unwrap());

        #[expect(
            clippy::as_conversions,
            reason = "should be an offset inside `filter_block`, \
                      and thus be less than `usize::MAX`",
        )]
        let start_of_offsets = start_of_offsets as usize;

        Self {
            policy,
            filter_block,
            start_of_offsets,
            filter_base_log2,
        }
    }

    /// Returns whether the `key` matches the filter for the block at offset `block_offset`
    /// within the table associated with the filter block being read.
    #[must_use]
    pub fn key_may_match(&self, block_offset: u64, key: &[u8]) -> bool
    where
        Policy: TableFilterPolicy,
    {
        let filter_index = block_offset >> self.filter_base_log2;

        let Ok(filter_index) = usize::try_from(filter_index) else {
            // This branch should never realistically be taken.
            return true;
        };

        if filter_index < self.num_filters() {
            let (start, up_to) = self.get_offset(filter_index);

            #[expect(
                clippy::as_conversions,
                clippy::indexing_slicing,
                reason = "they should be valid offsets",
            )]
            let filter = &self.filter_block.borrow()[start as usize..up_to as usize];

            if filter.is_empty() {
                // Empty filters do not match any keys
                false
            } else {
                self.policy.key_may_match(key, filter)
            }
        } else {
            // This branch shouldn't occur, but if an error happened,
            // then it _could_ be a match, we don't know.
            true
        }
    }

    #[must_use]
    fn num_filters(&self) -> usize {
        #![expect(
            clippy::integer_division,
            reason = "this is intentional (and should have remainder 0)",
        )]

        // The start of the offset array is the end of all the filters, and the data after
        // the offset array is 5 bytes. This gives us the size of the offset array in bytes.
        let offset_array_size = self.filter_block.borrow().len()
            - self.start_of_offsets
            - FOOTER_LEN;

        offset_array_size / U32_BYTES
    }

    /// # Panics
    /// May panic if `filter_index >= self.num_filters()`
    #[must_use]
    fn get_offset(&self, filter_index: usize) -> (u32, u32) {
        #![expect(
            clippy::indexing_slicing,
            reason = "FilterBlockReader::new and this method both document the panic condition",
        )]
        #![expect(clippy::unwrap_used, reason = "the slices are both the length of a u32 in bytes")]

        let start_offset = self.start_of_offsets + U32_BYTES * filter_index;
        let up_to_offset = start_offset + U32_BYTES;

        let start = &self.filter_block.borrow()[start_offset..start_offset + U32_BYTES];
        let up_to = &self.filter_block.borrow()[up_to_offset..up_to_offset + U32_BYTES];

        (
            u32::from_le_bytes(start.try_into().unwrap()),
            u32::from_le_bytes(up_to.try_into().unwrap()),
        )
    }
}
