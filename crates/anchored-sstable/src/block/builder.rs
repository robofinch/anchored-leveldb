use std::marker::PhantomData;

use integer_encoding::VarIntWriter as _;
use seekable_iterator::Comparator;

use crate::internal_utils::{common_prefix_len, U32_BYTES};
use super::Block;


/// Utilities for building a valid [`Block`].
///
/// Every `BlockBuilder` is associated with some `Cmp` type which implements
/// <code>[Comparator]<\[u8\]></code>. The `Block` entries should be ordered by their keys in the
/// order indicated by `Cmp`, but `BlockBuilder` does not validate that callers insert entries in
/// the correct order; callers must ensure that entries are inserted in the order of `Cmp`, without
/// duplicate keys, or else the produced `Block`s might be invalid.
///
/// [Comparator]: seekable_iterator::Comparator
#[derive(Debug)]
pub struct BlockBuilder<Cmp>(BlockBuilderImpl, PhantomData<Cmp>);

impl<Cmp> BlockBuilder<Cmp> {
    /// Initialize a `BlockBuilder`.
    ///
    /// The [`Block`]s made by this builder will have exactly one `restart` entry every
    /// `block_restart_interval` entries. These restart entries are used by iterators seeking
    /// through the `Block`, including moving backwards. (Forwards step-by-step iteration does not
    /// require `restart`s, but many operations do require them).
    ///
    /// # Panics
    /// Panics if `block_restart_interval == 0`.
    #[inline]
    #[must_use]
    pub const fn new(block_restart_interval: usize) -> Self {
        assert!(block_restart_interval > 0, "interval must be at least 1");
        Self(BlockBuilderImpl::new(block_restart_interval), PhantomData)
    }

    #[inline]
    #[must_use]
    pub const fn num_entries(&self) -> usize {
        self.0.num_entries()
    }

    #[inline]
    #[must_use]
    pub fn last_key(&self) -> &[u8] {
        self.0.last_key()
    }

    /// Returns the exact length of the slice or block which would be returned by
    /// `self.finish_block_contents()` or another `finish` method it it were called now.
    #[inline]
    #[must_use]
    pub fn finished_length(&self) -> usize {
        self.0.finished_length()
    }

    /// Allow the `BlockBuilder` to be reused for making more `Block`s.
    ///
    /// This keeps only the capacity of buffers and the `block_restart_interval` setting,
    /// discarding all entries and anything done by `self.finish_with_cmp()` or `self.finish()`.
    ///
    /// As `self.finish_with_cmp()` and `self.finish()` mangle the block buffer, this method must
    /// be called before adding more entries or using other methods of `self`.
    #[inline]
    pub fn reset(&mut self) {
        self.0.reset();
    }
}

impl<Cmp: Comparator<[u8]>> BlockBuilder<Cmp> {
    /// With respect to the `Cmp` comparator which will be used to finish the block being built,
    /// the newly added key must be strictly greater than any key previously added to the block.
    ///
    /// (The current block began being built when this `BlockBuilder` was created or when it had
    /// `reset()` called on it.)
    ///
    /// Failing to uphold this requirement may result in an invalid `Block` being created.
    ///
    /// # Panics
    /// Panics if the buffer's length exceeds `u32::MAX`.
    ///
    /// By checking `self.finished_length()` and calling one of the `finish` methods, this problem
    /// is easily avoidable.
    #[inline]
    pub fn add_entry(&mut self, key: &[u8], value: &[u8]) {
        self.0.add_entry(key, value);
    }

    /// After calling one of the `finish` methods, `self.reset()` must be called before using
    /// any other methods of `self`.
    #[inline]
    #[must_use]
    pub fn finish_block_contents(&mut self) -> &[u8] {
        self.0.finish_block_contents()
    }

    /// After calling one of the `finish` methods, `self.reset()` must be called before using
    /// any other methods of `self`.
    ///
    /// # Panics
    /// May panic if the buffer's length exceeds `u32::MAX`.
    ///
    /// By checking `self.finished_length()` and calling one of the `finish` methods, this problem
    /// is easily avoidable.
    #[inline]
    #[must_use]
    pub fn finish(&mut self) -> Block<&[u8], Cmp>
    where
        Cmp: Default,
    {
        self.finish_with_cmp(Cmp::default())
    }

    /// After calling one of the `finish` methods, `self.reset()` must be called before using
    /// any other methods of `self`.
    ///
    /// # Panics
    /// May panic if the buffer's length exceeds `u32::MAX`.
    ///
    /// By checking `self.finished_length()` and calling one of the `finish` methods, this problem
    /// is easily avoidable.
    #[inline]
    #[must_use]
    pub fn finish_with_cmp(&mut self, cmp: Cmp) -> Block<&[u8], Cmp> {
        Block::new(self.finish_block_contents(), cmp)
    }
}

/// Utilities for building a valid [`Block`].
///
/// Every `BlockBuilderImpl` is semantically associated with some `Cmp` type which implements
/// <code>[Comparator]<\[u8\]></code>. In order to allow costs from monomorphization to be reduced,
/// the implementations of these methods are kept separate from the `Cmp` generic.
///
/// [Comparator]: seekable_iterator::Comparator
#[derive(Debug)]
struct BlockBuilderImpl {
    block_buffer:     Vec<u8>,
    last_key:         Vec<u8>,
    num_entries:      usize,
    restarts:         Vec<u32>,
    /// Counter for making `restart` entries once every `self.restart_interval` entries.
    restart_counter:  usize,
    restart_interval: usize,
}

impl BlockBuilderImpl {
    #[inline]
    #[must_use]
    const fn new(block_restart_interval: usize) -> Self {
        Self {
            block_buffer:     Vec::new(),
            last_key:         Vec::new(),
            num_entries:      0,
            restarts:         Vec::new(),
            restart_counter:  0,
            restart_interval: block_restart_interval,
        }
    }

    #[must_use]
    const fn num_entries(&self) -> usize {
        self.num_entries
    }

    #[must_use]
    fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    /// Returns the exact length of the slice which would be returned by
    /// `self.finish_block_contents()` it it were called now.
    #[must_use]
    fn finished_length(&self) -> usize {
        self.block_buffer.len() + U32_BYTES * (self.restarts.len() + 1)
    }

    /// # Panics
    /// Panics if the buffer's length exceeds `u32::MAX`.
    // TODO: return errors instead of panicking. No sane user should be writing data like that,
    // but it's still a DOS condition.
    fn add_entry(&mut self, key: &[u8], value: &[u8]) {
        // Note that when `self.add_entry(_, _)` is first called after `self` was created or
        // `reset()`,
        // `self.restart_counter` is 0, thus ensuring that the first entry is always a restart.
        let shared = if self.restart_counter % self.restart_interval == 0 {
            #[expect(
                clippy::unwrap_used,
                reason = "error is incredibly unlikely, and is documented",
            )]
            self.restarts.push(u32::try_from(self.block_buffer.len()).unwrap());
            // Aside from when it's reset to 0, the counter ranges in `1..=self.restart_interval`
            // instead of `0..self.restart_interval`, and that's fine.
            self.restart_counter = 1;
            0
        } else {
            self.restart_counter += 1;

            common_prefix_len(&self.last_key, key)
        };

        #[expect(clippy::indexing_slicing, reason = "it is guaranteed that `shared <= key.len()`")]
        let non_shared_key = &key[shared..];
        let non_shared = key.len() - shared;

        {
            #![expect(
                clippy::unwrap_used,
                reason = "performing IO on a Vec like `self.block_buffer` \
                        can only fail due to allocation errors",
            )]
            self.block_buffer.write_varint(shared).unwrap();
            self.block_buffer.write_varint(non_shared).unwrap();
            self.block_buffer.write_varint(value.len()).unwrap();
        };

        self.block_buffer.extend(non_shared_key);
        self.block_buffer.extend(value);

        // Update key
        self.last_key.truncate(shared);
        self.last_key.extend_from_slice(non_shared_key);

        self.num_entries += 1;
    }

    fn reset(&mut self) {
        self.block_buffer.clear();
        self.last_key.clear();
        self.num_entries = 0;
        self.restarts.clear();
        self.restart_counter = 0;
    }

    /// After calling `self.finish_block_contents()`, `self.reset()` must be called before using
    /// any other methods of `self`.
    ///
    /// # Panics
    /// Panics if the number of restarts exceeds `u32::MAX`. This requires that the
    /// block buffer exceed `u32::MAX` bytes in length.
    #[must_use]
    fn finish_block_contents(&mut self) -> &[u8] {
        self.block_buffer.reserve(U32_BYTES * (self.restarts.len() + 1));

        // Append `restart`s
        for restart in &self.restarts {
            self.block_buffer.extend(restart.to_le_bytes());
        }

        // Append `num_restarts`
        #[expect(
            clippy::unwrap_used,
            reason = "error is incredibly unlikely, and is documented",
        )]
        let num_restarts = u32::try_from(self.restarts.len()).unwrap();
        self.block_buffer.extend(num_restarts.to_le_bytes());
        &self.block_buffer
    }
}
