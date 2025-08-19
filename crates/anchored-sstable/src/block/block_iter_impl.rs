use std::array;
use std::cmp::Ordering;

use integer_encoding::VarInt as _;
use seekable_iterator::Comparator;

use crate::utils::U32_BYTES;


#[derive(Default, Debug)]
pub struct BlockIterImplPieces {
    key_buffer: Vec<u8>,
}

impl BlockIterImplPieces {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// `BlockIterImpl` implements the algorithms for iterating through a [`Block`], but does not
/// itself store the associated block.
///
/// After a `Block`'s contents are passed to [`BlockIterImpl::new`] or
/// [`BlockIterImpl::reuse_as_new`], all methods of the `BlockIterImpl` value **must** be provided
/// references to the same `Block` contents, until [`BlockIterImpl::reuse_as_new`] is called.
/// Only then may the `Block` used be changed.
///
/// Similarly, for methods which take a `Cmp` comparator, it is required for logical correctness
/// that the block's keys were sorted in the comparator's order.
///
/// # Panics
/// Currently, all `BlockIterImpl` methods may assume that the provided block references refer
/// to a valid `Block`. The lack of validation can lead to panics or logical errors; that is,
/// database corruption leads to panics or, potentially, other severe errors.
///
/// This is not ideal, and will be changed.
#[derive(Debug)]
pub struct BlockIterImpl {
    /// After creation, this is constant, and is the upper bound of entry locations, and the lower
    /// bound of restart pointer data.
    restarts_offset:      usize,

    /// Must be either `self.restarts_offset` or the offset of a valid entry.
    ///
    /// Will be advanced to by `self.advance_entry()`.
    next_entry_offset:    usize,

    /// If `self.valid()`, this must be the offset of the current valid entry (the one referred
    /// to be `self.key`, `self.value_offset`, and so on).
    ///
    /// If `!self.valid()`, this may be anything at all (but in practice is either `0` or the
    /// offset of some previously-accessed entry).
    current_entry_offset: usize,

    /// If `!self.valid()`, this value must be zero.
    ///
    /// If `self.valid()`, `self.current_entry_offset` should be
    /// in `current_restart_offset..next_restart_offset` or
    /// in `current_restart_offset..` if `self.current_restart_idx == self.num_restarts() - 1`.
    ///
    /// Note that the lower restart bound is always present for nonempty blocks, as the very first
    /// entry is guaranteed to be a restart (if there is a first entry). And if `self.valid()`,
    /// it is required that `self.current_restart_idx < self.num_restarts()`.
    ///
    /// However, the restriction on this value is lifted for `self.advance_entry()` and
    /// `self.advance_until()`. All other methods may assume that `self.current_restart_idx`
    /// acts as described above.
    current_restart_idx:  usize,

    /// The key of the current entry, or empty.
    ///
    /// Note that keys may be empty.
    key:                  Vec<u8>,
    /// The offset of the current entry's value, or `0`.
    ///
    /// Must either be `0` or the offset of a valid entry value. The minimum possible offset
    /// of a valid entry value is `3`, so `0` is distinguishable.
    value_offset:         usize,
}

// Initialization
impl BlockIterImpl {
    /// # Panics
    /// Currently, all `BlockIterImpl` methods may assume that the provided block references refer
    /// to a valid `Block`. The lack of validation can lead to panics or logical errors; that is,
    /// database corruption leads to panics or, potentially, other severe errors.
    ///
    /// This is not ideal, and will be changed.
    #[inline]
    #[must_use]
    pub fn new(block: &[u8]) -> Self {
        let restarts_offset = Self::restarts_offset(block);

        Self {
            restarts_offset,

            next_entry_offset:    0,
            current_entry_offset: 0,
            current_restart_idx:  0,

            key:                  Vec::new(),
            value_offset:         0,
        }
    }

    #[inline]
    #[must_use]
    pub fn from_pieces(block: &[u8], mut pieces: BlockIterImplPieces) -> Self {
        let restarts_offset = Self::restarts_offset(block);
        pieces.key_buffer.clear();

        Self {
            restarts_offset,

            next_entry_offset:    0,
            current_entry_offset: 0,
            current_restart_idx:  0,

            key:                  pieces.key_buffer,
            value_offset:         0,
        }
    }

    #[inline]
    #[must_use]
    pub fn into_pieces(self) -> BlockIterImplPieces {
        BlockIterImplPieces {
            key_buffer: self.key,
        }
    }

    pub fn reuse_as_new(&mut self, block: &[u8]) {
        self.restarts_offset      = Self::restarts_offset(block);
        self.next_entry_offset    = 0;
        self.current_entry_offset = 0;
        self.current_restart_idx  = 0;
        self.key.clear();
        self.value_offset         = 0;
    }
}

// Implementation details
impl BlockIterImpl {
    /// Get the number of restart entries in this block.
    ///
    /// Is `0` if and only if there are zero entries in the block.
    ///
    /// # Panics
    /// Panics if `block` is not at least 4 bytes long. This can only occur for invalid blocks.
    #[must_use]
    fn num_restarts(block: &[u8]) -> u32 {
        debug_assert!(block.len() > U32_BYTES, "the `num_restarts` u32 is mandatory");

        #[expect(clippy::indexing_slicing, reason = "this function declares the panic")]
        let num_restarts = &block[block.len() - U32_BYTES..];
        #[expect(clippy::unwrap_used, reason = "the slice is the correct length")]
        u32::from_le_bytes(num_restarts.try_into().unwrap())
    }

    /// Get the number of restart entries in this block.
    ///
    /// Is `0` if and only if there are zero entries in the block.
    ///
    /// # Panics
    /// Panics if `block` is not at least 4 bytes long. This can only occur for invalid blocks.
    #[must_use]
    fn num_restarts_usize(block: &[u8]) -> usize {
        #![expect(
            clippy::as_conversions,
            reason = "if `block` is valid, then there are well more than `num_restarts` \
                        bytes in `block`, and thus `num_restarts` is less than `usize::MAX`",
        )]
        Self::num_restarts(block) as usize
    }

    #[must_use]
    fn restarts_offset(block: &[u8]) -> usize {
        #[expect(
            clippy::as_conversions,
            reason = "if `block` is valid, then there are well more than `num_restarts` \
                    bytes in `block`, and thus `num_restarts` is less than `usize::MAX`",
        )]
        let num_restarts = Self::num_restarts(block) as usize;
        block.len() - U32_BYTES * (1 + num_restarts)
    }

    /// Updates `self.current_restart_idx`, `self.current_entry_offset`, `self.key`, and
    /// `self.value` to the indicated restart entry, and sets `self.next_entry_offset` to either
    /// the following entry or `self.restarts_offset`.
    ///
    /// The previous state (before this call is made) can be any valid state.
    ///
    /// # Panics
    /// May panic if `restart_idx >= self.num_restarts()`.
    fn seek_to_restart_entry(&mut self, block: &[u8], restart_idx: usize) {
        let restart_entry_offset = self.get_restart_usize(block, restart_idx);

        self.next_entry_offset    = restart_entry_offset;
        self.current_restart_idx  = restart_idx;

        // If this is a valid `Block`, then the restart offset points to a valid entry
        self.advance_entry(block);
    }

    /// Get the offset of the indicated restart entry.
    ///
    /// # Panics
    /// May panic if `restart_idx >= self.num_restarts()`.
    #[must_use]
    fn get_restart(&self, block: &[u8], restart_idx: usize) -> u32 {
        debug_assert!(
            restart_idx < Self::num_restarts_usize(block),
            "`restart_idx` {restart_idx} is out of bounds. Is `self.num_restarts()` zero?",
        );

        let restart = self.restarts_offset + U32_BYTES * restart_idx;
        #[expect(clippy::indexing_slicing, reason = "only panics for invalid `Block`s")]
        let restart = &block[restart..restart + U32_BYTES];
        #[expect(clippy::unwrap_used, reason = "the slice is the correct length")]
        u32::from_le_bytes(restart.try_into().unwrap())
    }

    /// Get the offset of the indicated restart entry.
    ///
    /// # Panics
    /// May panic if `restart_idx >= self.num_restarts()`.
    #[inline]
    #[must_use]
    fn get_restart_usize(&self, block: &[u8], restart_idx: usize) -> usize {
        #![expect(
            clippy::as_conversions,
            reason = "if the `Block` is valid, then the `Block` is at least `self.get_restart()` \
                      bytes long, and we know its length is less than `usize::MAX`",
        )]
        self.get_restart(block, restart_idx) as usize
    }

    /// This function assumes that `self.next_entry_offset` points to a valid entry in a valid
    /// `Block`.
    ///
    /// `self.current_entry_offset`, `self.key`, and `self.value_offset` are advanced to that
    /// entry, and `self.next_entry_offset` is moved to either the following entry or to
    /// `self.restarts_offset`.
    ///
    /// `self.current_restart_idx` is not adjusted. It is permissible for `self.current_restart_idx`
    /// to be inaccurate before or after this call is made.
    ///
    /// # Returns
    /// Returns true if the now-current entry (the one advanced to) might be a restart entry.
    /// There are no false negatives, but there may be false positives.
    ///
    /// # Panics
    /// May panic if the above assumption is not met.
    fn advance_entry(&mut self, block: &[u8]) -> bool {
        self.current_entry_offset = self.next_entry_offset;

        let mut parsed_len = 0;

        let [shared, non_shared, value_size] = array::from_fn(|_| {
            #[expect(clippy::indexing_slicing, reason = "only panics for invalid `Block`s")]
            let decode_attempt = usize::decode_var(
                &block[self.next_entry_offset + parsed_len..],
            );

            let Some((decoded_varint, varint_len)) = decode_attempt else {
                {
                    #![expect(
                        clippy::panic,
                        reason = "this cannot occur for valid `Block`s. \
                                Might as well give better debug info for invalid `Block`s",
                    )]
                    panic!(
                        "advance_entry(): couldn't parse entry header at/after key {:?}",
                        self.key,
                    );
                }
            };

            parsed_len += varint_len;
            decoded_varint
        });

        let header_len = parsed_len;

        let key_offset = self.next_entry_offset + header_len;
        let value_offset = key_offset + non_shared;

        self.key.truncate(shared);

        #[expect(clippy::indexing_slicing, reason = "`Block` is assumed valid")]
        self.key.extend_from_slice(&block[key_offset..key_offset + non_shared]);

        self.value_offset      = value_offset;
        self.next_entry_offset = value_offset + value_size;

        // For every restart entry, `shared` is `0`. This might be, but need not be,
        // a restart entry.
        shared == 0
    }

    /// While `self.next_entry_offset < offset`, advance forwards with `self.advance_entry()`.
    ///
    /// If `self.next_entry_offset >= offset`, nothing happens.
    ///
    /// Otherwise, `self.current_entry_offset`, `self.key`, and `self.value_offset` are set to
    /// the greatest entry which is strictly before `offset`, and `self.next_entry_offset` is set
    /// to either the offset of the following entry or to `self.restarts_offset`.
    ///
    /// Does not adjust `self.current_restart_idx`.
    ///
    /// # Panics
    /// May panic if `offset > self.restarts_offset` or `self.next_entry_offset` is not the offset
    /// of a valid entry.
    fn advance_until(&mut self, block: &[u8], offset: usize) {
        while self.next_entry_offset < offset {
            self.advance_entry(block);
        }
    }

    /// This function assumes that `self.valid()`.
    ///
    /// # Panics
    /// May panic if the assumption is not met.
    #[inline]
    #[must_use]
    fn unchecked_current<'a, 'b>(&'a self, block: &'b [u8]) -> (&'a [u8], &'b [u8]) {
        #[expect(
            clippy::indexing_slicing,
            reason = "the offsets and the `Block` are assumed valid",
        )]
        (
            &self.key,
            &block[self.value_offset..self.next_entry_offset],
        )
    }
}

// Modified `CursorLendingIterator` methods
impl BlockIterImpl {
    /// This function should return true if and only if `self.current_entry_offset` points to a
    /// valid entry, `self.key` and `self.value_offset` store or refer to the key and value of that
    /// entry, `self.current_restart_idx` is the index of the greatest restart at or before the
    /// current entry, and `self.next_entry_offset` is either the following entry or
    /// `self.restarts_offset`.
    ///
    /// This function should return false if and only if `self.next_entry_offset`,
    /// `self.current_restart_idx`, and `self.value_offset` are 0 and `self.key` is empty.
    ///
    /// `self.current_entry_offset` may be anything if `!self.valid()`.
    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.value_offset != 0
    }

    pub fn next<'a, 'b>(&'a mut self, block: &'b [u8]) -> Option<(&'a [u8], &'b [u8])> {
        // If the block is empty, this essentially does nothing and returns false.
        // If the block is nonempty, then `self.current_entry_offset` was a valid entry: the last
        // one in the block. We wrap around to being `!valid()` as the phantom element, and next
        // time, `next()` will return the first element.
        if self.next_entry_offset >= self.restarts_offset {
            self.reset();
            return None;
        }

        // We checked that `self.next_entry_offset` is a valid entry (assuming the block is valid).
        let maybe_restart_entry = self.advance_entry(block);

        // We know that `self.current_restart_idx` was accurate before `self.next()` was called.
        // We moved exactly one entry forwards, and thus might need to move to the next restart.
        if maybe_restart_entry {
            let next_restart_idx = self.current_restart_idx + 1;
            if next_restart_idx < Self::num_restarts_usize(block)
                && self.current_entry_offset == self.get_restart_usize(block, next_restart_idx)
            {
                self.current_restart_idx = next_restart_idx;
            }
        }

        Some(self.unchecked_current(block))
    }

    #[inline]
    #[must_use]
    pub fn current<'a, 'b>(&'a self, block: &'b [u8]) -> Option<(&'a [u8], &'b [u8])> {
        self.valid().then(|| self.unchecked_current(block))
    }

    pub fn prev<'a, 'b>(&'a mut self, block: &'b [u8]) -> Option<(&'a [u8], &'b [u8])> {
        // If current is a restart, find the last key in the previous restart.
        // Else, move forwards in the current restart until self.key is reached,
        // and do not advance to it.
        // The difference in those two cases is essentially just which restart we seek to before
        // moving forwards, unless there is no previous restart.

        if !self.valid() {
            self.seek_to_last(block);
            return self.current(block);
        }

        // We know that `self.current_entry_offset` and `self.current_restart_idx` are
        // accurate and valid, since `self.valid()`.

        let current_entry_offset = self.current_entry_offset;
        let current_restart_offset = self.get_restart_usize(block, self.current_restart_idx);
        let current_is_a_restart = current_entry_offset == current_restart_offset;

        if current_is_a_restart {
            if let Some(previous_restart_idx) = self.current_restart_idx.checked_sub(1) {
                // Move to the first entry of the previous restart.
                // This also changes `self.current_restart_idx` to the previous index.
                self.seek_to_restart_entry(block, previous_restart_idx);
            } else {
                // We are at the very first restart. We _know_ that the very first entry is a
                // restart, and yada-yada-yada ~invariants of `Block`~, we know that we're at the
                // the first entry. Therefore, `prev()` should move us to the phantom element
                // before the first entry (and after the last entry).
                self.reset();
                return None;
            }
        } else {
            // This is more-or-less `seek_to_restart_entry` inlined, with the unnecessary stuff
            // removed.

            self.next_entry_offset = current_restart_offset;
            // If we have a valid `Block`, then the restart offset points to a valid entry
            self.advance_entry(block);
            // After the above call, everything is as it should be, except
            // `self.current_restart_idx` has not been changed in this function.
        }

        self.advance_until(block, current_entry_offset);

        // Note that we need not adjust `self.current_restart_idx`.
        // If `current_is_a_restart` was taken, the restart was moved one back, and we advance
        // up to just before the following restart; so it doesn't need updating.
        // Otherwise, we started advancing from the restart entry of `self.current_restart_idx`,
        // and stopped before `current_entry_offset`, and are at least 2 entries away from the
        // next restart.

        Some(self.unchecked_current(block))
    }
}

// Modified `Seekable<[u8], Cmp>` methods which don't require `Cmp`
impl BlockIterImpl {
    /// Reset the iterator to its initial position, before the first entry and after the last
    /// entry (if there are any entries in the collection).
    ///
    /// The iterator will then not be `valid()`.
    #[inline]
    pub fn reset(&mut self) {
        // Sets `self` to be `!valid()`, changing all the necessary values to preserve described
        // invariants.

        self.next_entry_offset = 0;
        // `current_entry_offset` need not be reset, it'll get overwritten before it's read
        self.current_restart_idx = 0;
        self.key.clear();
        self.value_offset = 0;
    }

    /// Move the iterator to the smallest key in the collection.
    ///
    /// If the collection is empty, the iterator is `!valid()`.
    pub fn seek_to_first(&mut self, block: &[u8]) {
        self.reset();
        self.next(block);
    }

    /// Move the iterator to the greatest key in the collection.
    ///
    /// If the collection is empty, the iterator is `!valid()`.
    pub fn seek_to_last(&mut self, block: &[u8]) {
        // Find and seek to the last restart point if there is one, then find the last key

        if let Some(last_restart_idx) = Self::num_restarts_usize(block).checked_sub(1) {
            // Move to the first entry of the previous restart.
            // This also changes `self.current_restart_idx` to the last index,
            // we don't need to adjust it again.
            self.seek_to_restart_entry(block, last_restart_idx);

            // If `self.next_entry_offset` is an entry, we advance further, stopping when
            // `self.next_entry_offset` is set to `self.restarts_offset`. If it already was,
            // nothing happens.
            self.advance_until(block, self.restarts_offset);
        } else {
            // The block is empty. Do nothing. It's necessarily the case that `!self.valid()`,
            // and nothing's changed since the creation of this iter.
        }
    }
}

// Modified `Seekable<[u8], Cmp>` methods which require `Cmp`
impl BlockIterImpl {
    /// Move the iterator to the smallest key which is greater or equal than the provided
    /// `min_bound`.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the collection).
    ///
    /// # Correctness
    /// It is required for logical correctness that the block's keys were sorted in the given
    /// comparator's order, and that no two keys compare equal to each other.
    /// The latter constraint holds true of any valid `Block`.
    pub fn seek<Cmp: Comparator<[u8]>>(&mut self, block: &[u8], cmp: &Cmp, key: &[u8]) {
        // Begin with binary search.
        let mut left: u32 = 0;
        // `right` is the maximum restart index.
        let Some(mut right) = Self::num_restarts(block).checked_sub(1) else {
            // There are no restarts iff the block is empty.
            return;
        };

        // There's only one restart. We'll need to do linear search, starting from the start of the
        // whole block.
        if left == right {
            self.reset();
        }

        while left < right {
            // Note that `left <= middle < right` unless `left >= right`,
            // since `middle = left + (right - left)/2` and the division rounds down.
            let middle = left.midpoint(right);
            #[expect(
                clippy::as_conversions,
                reason = "`middle` < `self.num_restarts()` < `usize::MAX`",
            )]
            self.seek_to_restart_entry(block, middle as usize);

            match cmp.cmp(&self.key, key) {
                // Too small -> look more rightwards
                Ordering::Less => {
                    left = middle;
                }
                // Too great -> look more leftwards.
                Ordering::Greater => {
                    right = middle - 1;
                }
                // Equal -> we got really lucky! we're done.
                Ordering::Equal => {
                    return;
                }
            }
        }

        // Note: if we get here, either `!self.valid()` (from `self.reset()` above)
        // or `self.current_entry_offset` refers to an entry whose key was compared above and found
        // to be not equal. Thus, in the below linear search, we only check `self.next()`.
        // Moreover, the left restart is always loosely before `key` (and if we get here it's
        // strictly before `key`), so the first entry that compares greater or equal is the one.

        // If `left == right` at the start, then there's only one restart, 0, which
        // `self.current_entry_offset` must be set to.
        // Otherwise, we went through the while loop.
        // On the last iteration, we necessarily had `left == middle == right - 1`,
        // so `seek_to_restart_entry` moved us to the restart entry of `left` a.k.a `middle`.
        #[expect(clippy::as_conversions, reason = "`left <= self.num_restarts() < usize::MAX`")]
        {
            debug_assert_eq!(left as usize, self.current_restart_idx, "see comment");
        };

        // If we manage to search until the end of the list, then there's no element
        // greater than or equal to `key`, so we correctly become `!valid()`.
        while self.next(block).is_some() {
            if cmp.cmp(&self.key, key) >= Ordering::Equal {
                return;
            }
        }
    }

    ///  Move the iterator to the greatest key which is strictly less than the provided
    /// `strict_upper_bound`.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the collection).
    ///
    /// # Speed
    /// This operation uses `self.prev()`, and is thus somewhat inefficient. If possible,
    /// this method should be avoided in favor of `self.seek()`.
    ///
    /// # Correctness
    /// It is required for logical correctness that the block's keys were sorted in the given
    /// comparator's order, and that no two keys compare equal to each other.
    /// The latter constraint holds true of any valid `Block`.
    pub fn seek_before<Cmp: Comparator<[u8]>>(
        &mut self,
        block:              &[u8],
        cmp:                &Cmp,
        strict_upper_bound: &[u8],
    ) {
        self.seek(block, cmp, strict_upper_bound);
        // Doing this more efficiently would require the ability to peek at the next key without
        // destroying the current key, which is a hassle, and would require a second buffer.
        self.prev(block);
    }
}
