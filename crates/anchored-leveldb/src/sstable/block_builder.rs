use std::num::NonZeroU32;

use crate::all_errors::types::AddBlockEntryError;
use crate::{
    pub_typed_bytes::{MinU32Usize, ShortSlice},
    utils::{common_prefix_len, WriteVarint as _},
};


/// Utilities for building a valid block of an SSTable.
///
/// Each block is semantically associated with some comparator. In order to allow costs from
/// monomorphization to be reduced (and allow reusing the same builder for blocks with different
/// comparators), this builder does not have a `Cmp` generic.
///
/// You must ensure that entries are added in sorted order for the produced block, else the block
/// may be considered corrupted.
#[derive(Debug)]
pub(super) struct BlockBuilder {
    block_buffer:     Vec<u8>,
    /// # Correctness Invariant
    /// Must have length at most `u32::MAX`.
    last_key:         Vec<u8>,
    num_entries:      usize,
    /// # Correctness Invariant
    /// Must have length at most `u32::MAX`.
    restarts:         Vec<u32>,
    /// Counter for making `restart` entries once every `self.restart_interval` entries.
    restart_counter:  u32,
    restart_interval: NonZeroU32,
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
    pub const fn new(block_restart_interval: NonZeroU32) -> Self {
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
    pub fn reset(&mut self) {
        self.block_buffer.clear();
        self.last_key.clear();
        self.num_entries = 0;
        self.restarts.clear();
        self.restart_counter = 0;
    }

    /// Allow the `BlockBuilder` to be reused for making more blocks.
    ///
    /// This keeps only the capacity of buffers, discarding all entries and anything done by
    /// `self.finish_block_contents()`.
    ///
    /// The `block_restart_interval` setting is changed to the provided value.
    /// The produced blocks will have exactly one `restart` entry every `block_restart_interval`
    /// entries (aside from the tail). These restart entries are used by iterators seeking
    /// through the block, including moving backwards. (Forwards step-by-step iteration does not
    /// require `restart`s, but many operations do require them).
    pub fn reset_with_restart_interval(&mut self, block_restart_interval: NonZeroU32) {
        self.block_buffer.clear();
        self.last_key.clear();
        self.num_entries = 0;
        self.restarts.clear();
        self.restart_counter = 0;
        self.restart_interval = block_restart_interval;
    }

    /// The number of entries that have been added to the block being constructed.
    #[must_use]
    pub const fn num_entries(&self) -> usize {
        self.num_entries
    }

    /// The key most-recently added to the block, or the empty slice if no entry has been added yet.
    #[must_use]
    pub fn last_key(&self) -> ShortSlice<'_> {
        // Correctness: guaranteed by invariant of `self.last_key`.
        #[expect(clippy::expect_used, reason = "panic is impossible (unless there's a bug)")]
        ShortSlice::new(&self.last_key)
            .expect("impossible for `BlockBuilder.last_key` to exceed `u32::MAX` in length")
    }

    /// Returns the exact length of the slice which would be returned by
    /// `self.finish_block_contents()` it it were called now.
    #[must_use]
    pub fn finished_length(&self) -> usize {
        self.block_buffer.len() + size_of::<u32>() * (self.restarts.len() + 1)
    }

    /// Checks whether `self.add_entry(key, value); self.add_entry(following_key, following_value)`
    /// would certainly succeed (if `self` isn't mutated between then and this check).
    ///
    /// The checks are performed solely based on the lengths of `key` and `value` and are slightly
    /// pessimistic.
    ///
    /// # Correctness
    /// It is assumed that the first insertion is guaranteed to succeed. Only the second insertion
    /// is checked.
    #[must_use]
    pub fn could_add_following_entry(&self, key_len: MinU32Usize, value_len: MinU32Usize) -> bool {
        let first_is_a_restart = self.restart_counter % self.restart_interval == 0;

        let (restart_counter, restarts_len) = if first_is_a_restart {
            (1, self.restarts.len() + 1)
        } else {
            (self.restart_counter + 1, self.restarts.len())
        };

        if restart_counter % self.restart_interval != 0 {
            // The following insertion is not a restart, and cannot fail.
            return true;
        }

        // Any buffer length strictly greater than `usize::MAX` is impossible, and thus does
        // not need to be considered. We can saturate.
        let worst_case_following_buf_len = self.block_buffer.len()
            // 3 worst-case varint32 values
            .saturating_add(15)
            // worst-case non-shared key
            .saturating_add(usize::from(key_len))
            // value
            .saturating_add(usize::from(value_len));


        if u32::try_from(worst_case_following_buf_len).is_err() {
            return false;
        }
        if !u32::try_from(restarts_len).is_ok_and(|restarts| restarts < u32::MAX) {
            return false;
        }

        true
    }

    /// Add the first entry to a block.
    ///
    /// Unlike `self.add_entry(..)`, the block cannot end up being too large for the given entry
    /// to be added.
    ///
    /// # Panics
    /// Panics if this is not the first entry in the block.
    pub fn add_first_entry(&mut self, key: ShortSlice<'_>, value: ShortSlice<'_>) {
        assert_eq!(
            self.num_entries,
            0,
            "`BlockBuilder::add_first_entry` should be called only when the block is empty",
        );

        self.num_entries = 1;
        self.restart_counter = 1;

        // Correctness invariant: `1 < u32::MAX`.
        self.restarts.push(0);
        self.block_buffer.write_varint32(0);
        self.block_buffer.write_varint32(u32::from(key.len()));
        self.block_buffer.write_varint32(u32::from(value.len()));

        self.block_buffer.extend(key.inner());
        self.block_buffer.extend(value.inner());

        // Correctness invariant: `key.len()` fits in a `MinU32Usize`.
        self.last_key.extend(key.inner());
    }

    /// With respect to the comparator which will be used with the block being built,
    /// the newly added key must be strictly greater than any key previously added to the block.
    ///
    /// (The current block began being built when this `BlockBuilder` was created or when it had
    /// `reset()` or `reset_with_restart_interval(_)` called on it.)
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
        key:   ShortSlice<'_>,
        value: ShortSlice<'_>,
    ) -> Result<(), AddBlockEntryError> {
        // Note that when `self.add_entry(_, _)` is first called after `self` was created or
        // reset, `self.restart_counter` is 0, thus ensuring that the first entry is
        // always a restart.
        let shared_len = if self.restart_counter % self.restart_interval == 0 {
            // If the block is too large for this restart to be added (either due to the length
            // being too large, or `self.restarts.len()` already being `u32::MAX`), start the
            // next block.
            let next_restart = u32::try_from(self.block_buffer.len())
                .map_err(|_err| AddBlockEntryError)?;
            if u32::try_from(self.restarts.len()).is_ok_and(|restarts| restarts < u32::MAX) {
                // Correctness invariant: `self.restarts.len() < u32::MAX`, so adding `1` does not
                // exceed `u32::MAX`.
                self.restarts.push(next_restart);
            } else {
                return Err(AddBlockEntryError);
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

        // Update key. Note that `non_shared_key.len() = key.inner().len() - shared_len`
        // and thus, after these two calls, `self.last_key.len() == key.inner().len()`,
        // which fits in a `MinU32Usize` as desired.
        self.last_key.truncate(usize::from(shared_len));
        self.last_key.extend(non_shared_key);

        self.num_entries += 1;
        Ok(())
    }

    /// After calling `self.finish_block_contents()`, `self.reset()` or
    /// `self.reset_with_restart_interval(_)` must be called before using any other methods of
    /// `self`. Otherwise, a corrupted block may be produced.
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
            .expect("correctness invariant: `BlockBuilder.restarts.len() <= u32::MAX`");
        self.block_buffer.extend(num_restarts.to_le_bytes());
        &self.block_buffer
    }
}
