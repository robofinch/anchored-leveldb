use std::num::NonZeroUsize;

use crate::pub_typed_bytes::MinU32Usize;
use crate::{
    typed_bytes::{EncodedInternalKey, MaybeUserValue},
    utils::{common_prefix_len, WriteVarint as _},
};


/// Utilities for building a valid block of an SSTable.
///
/// Each block is semantically associated with some `Cmp` type which implements
/// <code>[LevelDBComparator]<\[u8\]></code>. In order to allow costs from monomorphization to be
/// reduced (and allow reusing the same builder for blocks with different comparators), this builder
/// does not have a `Cmp` generic.
///
/// You must ensure that entries are added in sorted order for the produced block, else the block
/// may be considered corrupted.
///
/// [LevelDBComparator]: crate::pub_traits::cmp_and_policy::LevelDBComparator
#[derive(Debug)]
pub(super) struct BlockBuilder {
    block_buffer:     Vec<u8>,
    last_key:         Vec<u8>,
    num_entries:      usize,
    restarts:         Vec<u32>,
    /// Counter for making `restart` entries once every `self.restart_interval` entries.
    restart_counter:  usize,
    restart_interval: NonZeroUsize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl BlockBuilder {
    /// Initialize a `BlockBuilder`.
    ///
    /// The produced blocks will have exactly one `restart` entry every `block_restart_interval`
    /// entries (aside from the tail). These restart entries are used by iterators seeking
    /// through the block, including moving backwards. (Forwards step-by-step iteration does not
    /// require `restart`s, but many operations do require them).
    #[inline]
    #[must_use]
    pub const fn new(block_restart_interval: NonZeroUsize) -> Self {
        Self {
            block_buffer:     Vec::new(),
            last_key:         Vec::new(),
            num_entries:      0,
            restarts:         Vec::new(),
            restart_counter:  0,
            restart_interval: block_restart_interval,
        }
    }

    /// Allow the `BlockBuilder` to be reused for making more blocks.
    ///
    /// This keeps only the capacity of buffers and the `block_restart_interval` setting,
    /// discarding all entries and anything done by `self.finish_block_contents()`.
    ///
    /// As `self.finish_block_contents()` mangles the block buffer, this method must
    /// be called before adding more entries or using other methods of `self`.
    pub fn reset(&mut self) {
        self.block_buffer.clear();
        self.last_key.clear();
        self.num_entries = 0;
        self.restarts.clear();
        self.restart_counter = 0;
    }

    /// The number of entries that have been added to the block being constructed.
    #[must_use]
    pub const fn num_entries(&self) -> usize {
        self.num_entries
    }

    /// The key most-recently added to the block, or the empty slice if no entry has been added yet.
    #[must_use]
    pub fn last_key(&self) -> &[u8] {
        self.last_key.as_slice()
    }

    /// Returns the exact length of the slice which would be returned by
    /// `self.finish_block_contents()` it it were called now.
    #[must_use]
    pub fn finished_length(&self) -> usize {
        self.block_buffer.len() + size_of::<u32>() * (self.restarts.len() + 1)
    }

    /// With respect to the comparator which will be used with the block being built,
    /// the newly added key must be strictly greater than any key previously added to the block.
    ///
    /// (The current block began being built when this `BlockBuilder` was created or when it had
    /// `reset()` called on it.)
    ///
    /// Failing to uphold this requirement may result in an invalid/corrupt block being created.
    ///
    /// # Errors
    /// Returns an error if the number and/or size of the entries already in the block is too
    /// large for the given entry to be added.
    ///
    /// The current block should be flushed, and the entry can be successfully written to an
    /// empty block.
    pub fn add_entry(
        &mut self,
        key:   EncodedInternalKey<'_>,
        value: MaybeUserValue<'_>,
    ) -> Result<(), AddEntryError> {
        // Note that when `self.add_entry(_, _)` is first called after `self` was created or
        // `reset()`, `self.restart_counter` is 0, thus ensuring that the first entry is
        // always a restart.
        let shared_len = if self.restart_counter % self.restart_interval == 0 {
            // If the block is too large for this restart to be added (either due to the length
            // being too large, or `self.restarts.len()` already being `u32::MAX`), start the
            // next block.
            let next_restart = u32::try_from(self.block_buffer.len())
                .map_err(|_err| AddEntryError)?;
            if u32::try_from(self.restarts.len()).is_ok_and(|restarts| restarts < u32::MAX) {
                self.restarts.push(next_restart);
            } else {
                return Err(AddEntryError);
            }

            // Aside from when it's reset to 0, the counter ranges in `1..=self.restart_interval`
            // instead of `0..self.restart_interval`, and that's fine.
            self.restart_counter = 1;
            MinU32Usize::ZERO
        } else {
            self.restart_counter += 1;

            #[expect(clippy::expect_used, reason = "could only fail due to a bug")]
            MinU32Usize::from_usize(
                common_prefix_len(&self.last_key, key.inner()),
            ).expect(
                "common len is at most `EncodedInternalKey::inner(key).len()`, whose length \
                 fits in a `MinU32Usize`",
            )
        };

        #[expect(
            clippy::expect_used,
            clippy::indexing_slicing,
            reason = "it is guaranteed that `shared_len <= key.len()`",
        )]
        let (non_shared_key, non_shared_len) = (
            &key.inner()[usize::from(shared_len)..],
            key.len()
                .checked_sub(shared_len)
                .expect("`common_prefix_len(_, key.inner())` has length at most `key.len()`"),
        );

        self.block_buffer.write_varint32(u32::from(shared_len));
        self.block_buffer.write_varint32(u32::from(non_shared_len));
        self.block_buffer.write_varint32(u32::from(value.len()));

        self.block_buffer.extend(non_shared_key);
        self.block_buffer.extend(value.inner());

        // Update key
        self.last_key.truncate(usize::from(shared_len));
        self.last_key.extend(non_shared_key);

        self.num_entries += 1;
        Ok(())
    }

    /// After calling `self.finish_block_contents()`, `self.reset()` must be called before using
    /// any other methods of `self`.
    #[must_use]
    pub fn finish_block_contents(&mut self) -> &[u8] {
        self.block_buffer.reserve(size_of::<u32>() * (self.restarts.len() + 1));

        // Append `restart`s
        for restart in &self.restarts {
            self.block_buffer.extend(restart.to_le_bytes());
        }

        // Append `num_restarts`
        #[expect(clippy::expect_used, reason = "could only fail due to a bug")]
        let num_restarts = u32::try_from(self.restarts.len())
            .expect("`BlockBuilder::add_entry` ensures that there are at most `u32::MAX` restarts");
        self.block_buffer.extend(num_restarts.to_le_bytes());
        &self.block_buffer
    }
}

/// Returned if the block being built by a [`BlockBuilder`] is too full to have a certain
/// entry added to it.
///
/// (Note that *any* entry can be successfully added to an empty block.)
pub(super) struct AddEntryError;
