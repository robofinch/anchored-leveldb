use std::{cmp::Ordering, ops::Range};

use crate::utils::ReadVarint as _;
use crate::{
    all_errors::types::{BlockSeekError, CorruptedBlockError},
    pub_typed_bytes::{MinU32Usize, TableBlockOffset},
};


// TODO: the current design of block iters makes `iter.current()` fallible even at the highest
// levels. It'd be ideal to do more validation immediately. Plus, it'd be nice to have errors be
// reported directly to a handler instead of percolating up the call stack.

/// A circular (rather than fused) iterator through a block of an SSTable.
///
/// In particular, `BlockIter` stores the state and implements the algorithms for iterating through
/// a block of an SSTable, but does not itself store the associated block.
///
/// After a block's contents are passed to [`BlockIter::new`] or [`BlockIter::set`], all methods of
/// the `BlockIter` value **must** be provided references to the same block contents, until
/// [`BlockIter::set`] or [`BlockIter::clear`] is called. Only when calling [`BlockIter::set`]
/// may the block used be changed. Note that the iterator resulting from [`BlockIter::new_empty`]
/// or [`BlockIter::clear`] must not have any block provided to it until [`BlockIter::set`] is
/// called.
///
/// For methods which take a `Cmp` comparator, it is required for logical correctness
/// that the block's keys were sorted in the comparator's order.
///
/// # Errors
/// After a corruption error, the `BlockIter` is in an unpredictable corrupt state, so further
/// calls to any methods other than [`BlockIter::set`] or [`BlockIter::clear`] may result in
/// spurious corruption errors or other strange results.
///
/// # Panics
/// All `BlockIter` methods may assume that block contents are provided correctly, as described
/// above. (However, the block contents may be corrupt; that will result in errors being returned,
/// rather than panics.)
///
/// # Format
/// A block is a list of `entries`, followed by a list of `restart`s, terminated by `num_restarts`.
///
/// An `entry` consists of three varint32 values (`shared_len`, `non_shared_len`, and `value_len`),
/// a `key` byte slice, and a `value` byte slice.
///
/// - `shared_len` denotes how many bytes the entry's key shares with the previous one.
/// - `non_shared_len` is the length of the `key` stored in the entry.
/// - The length of the semantic key -- which is *not* necessarily stored in a single slice of
///   the block -- is `shared_len + non_shared_len`.
/// - `value_len` is the length of `value`.
/// - a `restart` is a fixed u32 pointing to the beginning of an `entry`. The key of a restart
///   entry must have `shared` set to `0` (though the latter does not imply being a restart entry).
///   The very first entry _must_ be a restart. There must not be multiple restarts pointing at the
///   same entry.
/// - `num_restarts` is a fixed u32 indicating the number of restarts.
///
/// The keys of `entries` must be in some sorted order handled consistently. The list of `restarts`
/// must likewise be sorted such that a restart is sorted earlier iff the restart entry it refers
/// to is sorted earlier.
///
/// The keys should all compare distinct from each other; otherwise, seeking can become
/// unpredictable and slightly logically incorrect.
///
/// Note that all these guarantees are satisfied by Google's C++ implementation of LevelDB.
#[derive(Debug)]
pub(super) struct BlockIter {
    /// After creation, this is constant, and is the upper bound of entry locations, and the lower
    /// bound of restart pointer data.
    ///
    /// # Corruption-proof Guarantee
    /// If the block is set, `self.restarts_offset` is validated to not be corrupt.
    restarts_offset:      usize,
    /// Must be either `self.restarts_offset` or the offset of a (possibly-corrupt) entry.
    ///
    /// Will be advanced to by `self.advance_entry()`.
    ///
    /// # Corruption-proof Guarantee
    /// If the block is set, then `self.next_entry_offset <= self.restarts_offset`.
    next_entry_offset:    usize,
    /// If `self.valid()`, this must be the offset of the current (possibly-corrupt) entry, which
    /// is the one referenced by `self.key`, `self.value_offset`, and so on.
    ///
    /// If `!self.valid()`, this may be anything at all (but in practice is either `0` or the
    /// offset of some previously-accessed entry).
    ///
    /// # Corruption-proof Guarantee
    /// If the block is set and `self.valid()`,
    /// then `self.current_entry_offset < self.restarts_offset`.
    current_entry_offset: usize,
    /// If `!self.valid()`, this value must be zero.
    ///
    /// If `self.valid()`, `self.current_entry_offset` should be
    /// in `current_restart_offset..next_restart_offset` or
    /// in `current_restart_offset..` if `self.current_restart_idx == self.num_restarts(_) - 1`.
    /// ("Should", rather than "must", due to the possibility of corruption.)
    ///
    /// Note that the lower restart bound is always present for nonempty blocks, as the very first
    /// entry is guaranteed to be a restart (if there is a first entry). And if `self.valid()`,
    /// it is required that `self.current_restart_idx < self.num_restarts(_)`.
    ///
    /// However, the restriction on this value is lifted for `self.advance_entry()` and
    /// `self.advance_until()`. All other methods may assume that `self.current_restart_idx`
    /// acts as described above.
    ///
    /// # Corruption-proof Guarantee
    /// If the block is set and `self.valid()`,
    /// then `self.current_restart_idx < self.num_restarts(_)`.
    ///
    /// (However, this cannot be relied on by `unsafe` code, since safe functions on this iterator
    /// document that they may be given out-of-bounds restart indices, on pain of panics rather
    /// than UB.)
    current_restart_idx:  usize,
    /// The key of the current entry, or empty.
    ///
    /// Note that keys may be empty.
    key:                  Vec<u8>,
    /// The offset of the current entry's value, or `0`.
    ///
    /// Must either be `0` or the offset of a (possibly-corrupt) entry value. The minimum possible
    /// offset of a valid entry value is `3`, so `0` is distinguishable.
    ///
    /// # Corruption-proof Guarantee
    /// If the block is set and `self.valid()`, then `self.value_offset <= self.next_entry_offset`.
    /// Additionally, `self.value_offset + u32::MAX` should either overflow or be greater than or
    /// equal to `self.next_entry_offset`; that is, the length of the current value must not
    /// exceed `u32::MAX`.
    value_offset:         usize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl BlockIter {
    #[inline]
    #[must_use]
    pub const fn new_empty() -> Self {
        // Corruption-proof
        Self {
            restarts_offset:      0,
            next_entry_offset:    0,
            current_entry_offset: 0,
            current_restart_idx:  0,

            key:                  Vec::new(),
            value_offset:         0,
        }
    }

    #[inline]
    pub fn new(block: &[u8]) -> Result<Self, (TableBlockOffset, CorruptedBlockError)> {
        let restarts_offset = Self::restarts_offset_checked(block)?;

        Ok(Self {
            restarts_offset,

            next_entry_offset:    0,
            current_entry_offset: 0,
            current_restart_idx:  0,

            key:                  Vec::new(),
            value_offset:         0,
        })
    }

    pub fn set(&mut self, block: &[u8]) -> Result<(), (TableBlockOffset, CorruptedBlockError)> {
        self.restarts_offset      = Self::restarts_offset_checked(block)?;
        self.next_entry_offset    = 0;
        self.current_entry_offset = 0;
        self.current_restart_idx  = 0;
        self.key.clear();
        self.value_offset         = 0;
        Ok(())
    }

    #[inline]
    pub fn clear(&mut self) {
        self.restarts_offset      = 0;
        self.next_entry_offset    = 0;
        self.current_entry_offset = 0;
        self.current_restart_idx  = 0;
        self.key.clear();
        self.value_offset         = 0;
    }
}

impl BlockIter {
    /// Get the number of restart entries in this block.
    ///
    /// Is `0` if and only if there are zero entries in the block.
    ///
    /// # Errors
    /// Returns an error if `block` is not at least 4 bytes long, or if the number of restarts
    /// is impossibly large.
    fn num_restarts_checked(
        block: &[u8],
    ) -> Result<MinU32Usize, (TableBlockOffset, CorruptedBlockError)> {
        let num_restarts = block.last_chunk::<4>()
            .ok_or((
                TableBlockOffset(0),
                CorruptedBlockError::MissingNumRestarts,
            ))?;

        // Since `block.last_chunk::<4>()` returned `Some`, the length is at least 4.
        let num_restarts_offset = TableBlockOffset(block.len() - 4);

        // The size of `block` is at least 4 times the number of restarts. If the number of
        // restarts overflows a `usize`, that should imply that `block` has length exceeding
        // `usize::MAX`... by contradiction, the number of restarts is corrupt (impossibly large)
        // in that case.
        let num_restarts = MinU32Usize::from_u32(u32::from_le_bytes(*num_restarts))
            .ok_or((num_restarts_offset, CorruptedBlockError::NumRestartsTooLarge))?;

        // Confirm that `Self::restarts_offset(block)` succeeds.
        let restarts_offset = usize::from(num_restarts)
            .checked_add(1)
            .and_then(|sum| sum.checked_mul(size_of::<u32>()))
            .ok_or((num_restarts_offset, CorruptedBlockError::NumRestartsTooLarge))?;

        if restarts_offset > block.len() {
            Err((num_restarts_offset, CorruptedBlockError::NumRestartsTooLarge))
        } else {
            Ok(num_restarts)
        }
    }

    #[expect(clippy::unused_self, reason = "used as proof that `block` is valid")]
    #[must_use]
    fn num_restarts(&self, block: &[u8]) -> MinU32Usize {
        // Since `Self::num_restarts_checked` must succeed in order to set the block of `self`,
        // and since we can assume that `self`'s block is `block`, it follows that this `expect`
        // should not panic.
        #[expect(
            clippy::expect_used,
            reason = "only panics if the caller has a bug and provides the wrong `block`",
        )]
        block.last_chunk()
            .and_then(|num_restarts| MinU32Usize::from_u32(u32::from_le_bytes(*num_restarts)))
            .expect("`BlockIter` should be provided with a `block` that it is set to")
    }

    fn restarts_offset_checked(
        block: &[u8],
    ) -> Result<usize, (TableBlockOffset, CorruptedBlockError)> {
        let num_restarts = Self::num_restarts_checked(block)?;
        // NOTE: as per the checks in `Self::num_restarts`, this arithmetic does not
        // underflow or overflow.
        Ok(block.len() - size_of::<u32>() * (1 + usize::from(num_restarts)))
    }

    /// Updates `self.current_restart_idx`, `self.current_entry_offset`, `self.key`, and
    /// `self.value` to the indicated restart entry, and sets `self.next_entry_offset` to either
    /// the following entry or `self.restarts_offset`.
    ///
    /// The previous state (before this call is made) can be any valid (though possibly-corrupt)
    /// state.
    ///
    /// # Errors
    /// May return an error if the desired restart is corrupt.
    ///
    /// # Panics
    /// May panic if `restart_idx >= self.num_restarts(block)`.
    fn seek_to_restart_entry_panicky(
        &mut self,
        block:       &[u8],
        restart_idx: usize,
    ) -> Result<(), CorruptedBlockError> {
        let restart_entry_offset = self.get_restart_usize_panicky(block, restart_idx)?;

        if restart_entry_offset >= self.restarts_offset {
            return Err(CorruptedBlockError::RestartOutOfBounds);
        }

        // We validated that `current_restart_offset < self.restarts_offset`, so the guarantee
        // of `self.next_entry_offset` is satisfied, and `self.advance_entry(block)` should
        // not return a spurious corruption error.
        self.next_entry_offset    = restart_entry_offset;
        // The caller asserts that `restart_idx < self.num_restarts(_)`. Also, we've adjusted
        // `current_restart_idx` to the correct value.
        self.current_restart_idx  = restart_idx;
        self.advance_entry(block)?;

        Ok(())
    }

    /// Get the offset of the indicated restart entry.
    ///
    /// # Panics
    /// May panic if `restart_idx >= self.num_restarts(block)`.
    #[must_use]
    fn get_restart_panicky(&self, block: &[u8], restart_idx: usize) -> u32 {
        // Note: if `self` is set to the given `block` (as we can assume it to be),
        // then `Self::num_restarts_checked(block)` succeeded. Therefore, any in-bounds restart
        // index (that is, an index less than `self.num_restarts(block)`) has restart data in-bounds
        // of the `block` slice. Therefore, the indexing slicing shouldn't panic (provided that
        // `self` is set to `block` -- a condition documented on `BlockIter` --
        // and `restart_idx < self.num_restarts()`).

        debug_assert!(
            restart_idx < usize::from(self.num_restarts(block)),
            "`restart_idx` {restart_idx} is out of bounds. Is `self.num_restarts()` zero?",
        );

        let restart = self.restarts_offset + size_of::<u32>() * restart_idx;
        #[expect(
            clippy::indexing_slicing,
            reason = "Only panics if caller violates documented condition",
        )]
        let restart = &block[restart..restart + size_of::<u32>()];

        #[expect(clippy::unwrap_used, reason = "the slice is the correct length, size_of::<u32>()")]
        u32::from_le_bytes(restart.try_into().unwrap())
    }

    /// Get the offset of the indicated restart entry.
    ///
    /// # Errors
    /// May return an error if the desired restart is corrupt.
    ///
    /// # Panics
    /// May panic if `restart_idx >= self.num_restarts()`.
    #[inline]
    fn get_restart_usize_panicky(
        &self,
        block: &[u8],
        restart_idx: usize,
    ) -> Result<usize, CorruptedBlockError> {
        // The panic condition is passed up to the caller of this function.
        let restart = self.get_restart_panicky(block, restart_idx);

        // If the restart does not fit in a `usize` even though `block.len()` does, then clearly
        // it's out-of-bounds.
        usize::try_from(restart).map_err(|_overflow| CorruptedBlockError::RestartOutOfBounds)
    }

    /// This function assumes that `self.next_entry_offset` points to a (possibly-corrupt) entry in
    /// the `block`.
    ///
    /// `self.current_entry_offset`, `self.key`, and `self.value_offset` are advanced to that
    /// entry, and `self.next_entry_offset` is moved to either the following entry or to
    /// `self.restarts_offset`.
    ///
    /// # Current Restart Index
    /// `self.current_restart_idx` is not adjusted. It is permissible for `self.current_restart_idx`
    /// to be inaccurate before or after this call is made.
    ///
    /// # Returns
    /// Returns `true` if the now-current entry (the one advanced to) might be a restart entry.
    /// There are no false negatives, but there may be false positives.
    ///
    /// # Requirements
    /// `self.next_entry_offset` must be strictly less than `self.restarts_offset`, else
    /// guarantees of `self` may be violated.
    fn advance_entry(&mut self, block: &[u8]) -> Result<bool, CorruptedBlockError> {
        // The requirement that `self.current_entry_offset < self.restarts_offset` is forwarded
        // to the caller.
        self.current_entry_offset = self.next_entry_offset;

        #[expect(clippy::expect_used, reason = "succeeds, by invariants of `Self`")]
        let mut remaining_data = block.get(self.next_entry_offset..self.restarts_offset)
            .expect("`self.next_entry_offset <= self.restarts_offset < block.len()`");

        let (shared_len,     shared_len_len)     = remaining_data.read_varint32()?;
        let (non_shared_len, non_shared_len_len) = remaining_data.read_varint32()?;
        let (value_len,      value_len_len)      = remaining_data.read_varint32()?;

        // Should not overflow, assuming that `read_varint32` is implemented correctly.
        let header_len = shared_len_len + non_shared_len_len + value_len_len;

        // Since `self.next_entry_offset < block.len() <= isize::MAX` and
        // `header_len <= block.len()` as well, it follows that this does not overflow.
        let key_offset = self.next_entry_offset + header_len;

        // If the key's end offset exceeds `usize::MAX`, then clearly it's out-of-bounds
        // for the `entries` segment of `block`.
        let key_end_offset = usize::try_from(non_shared_len).ok()
            .and_then(|len| len.checked_add(key_offset))
            .ok_or(CorruptedBlockError::TruncatedKey)?;

        if key_end_offset > self.restarts_offset {
            return Err(CorruptedBlockError::TruncatedKey);
        }

        #[expect(
            clippy::indexing_slicing,
            reason = "`key_offset <= key_end_offset <= self.restarts_offset < block.len()`",
        )]
        let non_shared_key_data = &block[key_offset..key_end_offset];

        let value_offset = key_end_offset;

        // If the values's end offset exceeds `usize::MAX`, then clearly it's out-of-bounds
        // for the `entries` segment of `block`.
        let value_end_offset = usize::try_from(value_len).ok()
            .and_then(|len| len.checked_add(value_offset))
            .ok_or(CorruptedBlockError::TruncatedValue)?;

        if value_end_offset > self.restarts_offset {
            return Err(CorruptedBlockError::TruncatedValue);
        }

        // If `shared_len` exceeds `usize::MAX`, clearly it's too large.
        let Ok(shared_len) = usize::try_from(shared_len) else {
            return Err(CorruptedBlockError::OversharedKey);
        };
        if self.key.len() < shared_len {
            return Err(CorruptedBlockError::OversharedKey);
        }
        self.key.truncate(shared_len);

        self.key.extend(non_shared_key_data);

        // Since `value_end_offset` is `value_len + value_offset` without overflow,
        // and we validate that `value_end_offset <= self.restarts_offset`,
        // the guarantees of these two fields are satisfied.
        // Note use that `value_len` is a `u32`, so `self.value_offset + u32::MAX` cannot possibly
        // exceed `self.next_entry_offset`.
        self.value_offset      = value_offset;
        self.next_entry_offset = value_end_offset;

        // For every restart entry, `shared` is `0`. This might be, but need not be,
        // a restart entry.
        Ok(shared_len == 0)
    }

    /// While `self.next_entry_offset < offset`, advance forwards with `self.advance_entry()`.
    ///
    /// If `self.next_entry_offset >= offset`, nothing happens.
    ///
    /// Otherwise, `self.current_entry_offset`, `self.key`, and `self.value_offset` are set to
    /// the greatest entry which is strictly before `offset`, and `self.next_entry_offset` is set
    /// to either the offset of the following entry or to `self.restarts_offset`.
    ///
    /// # Current Restart Index
    /// Does not adjust `self.current_restart_idx`.  It is permissible for
    /// `self.current_restart_idx` to be inaccurate before or after this call is made.
    ///
    /// # Requirements
    /// `offset` must be at most `self.restarts_offset`, else guarantees of `self` may be violated.
    fn advance_until(&mut self, block: &[u8], offset: usize) -> Result<(), CorruptedBlockError> {
        while self.next_entry_offset < offset {
            // We could only have `self.next_entry_offset == self.restarts_offset` if
            // `self.next_entry_offset == self.restarts_offset < offset`, which the caller asserts
            // is not the case.
            self.advance_entry(block)?;
        }

        Ok(())
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl BlockIter {
    /// Relevant for reporting the location where an error occurred.
    #[inline]
    #[must_use]
    pub const fn current_entry_offset(&self) -> TableBlockOffset {
        TableBlockOffset(self.current_entry_offset)
    }

    /// Relevant for reporting the location where an error occurred.
    #[inline]
    #[must_use]
    pub const fn current_value_offset(&self) -> TableBlockOffset {
        TableBlockOffset(self.value_offset)
    }

    /// # Internal Documentation
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

    /// Move the iterator one position forwards, and return the entry at that position.
    /// Returns `None` if the iterator was at the last entry.
    ///
    /// The returned value is guaranteed to have length at most `u32::MAX`.
    ///
    /// Note that this iterator is conceptually circular rather than fused.
    pub fn next<'a, 'b>(
        &'a mut self,
        block: &'b [u8],
    ) -> Result<Option<BlockEntry<'a, 'b>>, CorruptedBlockError> {
        // If the block is empty, this essentially does nothing and returns false.
        // If the block is nonempty, then `self.current_entry_offset` was a valid entry: the last
        // one in the block. We wrap around to being `!valid()` as the phantom element, and next
        // time, `next()` will return the first element.
        if self.next_entry_offset == self.restarts_offset {
            self.reset();
            return Ok(None);
        }

        // We checked that `self.next_entry_offset` is not equal to `self.restarts_offset`.
        // Since it is guaranteed to be at most `self.restarts_offset`, we thus have that it is
        // strictly less.
        let maybe_restart_entry = self.advance_entry(block)?;

        // We know that `self.current_restart_idx` was accurate before `self.next()` was called.
        // We moved exactly one entry forwards, and thus might need to move to the next restart.
        if maybe_restart_entry {
            // Since `self.current_restart_idx < self.num_restarts(_)`, adding 1 does not overflow.
            let next_restart_idx = self.current_restart_idx + 1;
            if next_restart_idx < usize::from(self.num_restarts(block)) {
                let next_restart_usize = self.get_restart_usize_panicky(block, next_restart_idx)?;
                if self.current_entry_offset == next_restart_usize {
                    // We ensure that `next_restart_idx < self.num_restarts(_)`.
                    self.current_restart_idx = next_restart_idx;
                }
            }
        }

        // Since `value_offset` is set to a nonzero value by `self.advance_entry` (if it
        // returns successfully), it follows that `self.valid()`, so we can call this without
        // a panic.
        Ok(Some(self.current_panicky(block)))
    }

    /// Return the entry at the iterator's current position in the block.
    ///
    /// The returned value is guaranteed to have length at most `u32::MAX`.
    ///
    /// `None` is conceptually a phantom entry before the first actual entry and after the last
    /// actual entry (if the block is nonempty).
    #[inline]
    #[must_use]
    pub fn current<'a, 'b>(&'a self, block: &'b [u8]) -> Option<BlockEntry<'a, 'b>> {
        self.valid().then(|| self.current_panicky(block))
    }

    /// This function assumes that `self.valid()`.
    ///
    /// The returned value is guaranteed to have length at most `u32::MAX`.
    ///
    /// # Panics
    /// May panic if `!self.valid()`.
    #[inline]
    #[must_use]
    pub fn current_panicky<'a, 'b>(&'a self, block: &'b [u8]) -> BlockEntry<'a, 'b> {
        #[expect(
            clippy::indexing_slicing,
            reason = "`self` should be `valid()` and set to `block`",
        )]
        BlockEntry {
            key:  &self.key,
            // Since we can assume that `self` is `valid()` and set to `block`, it follows that
            // `self.value_offset <= self.next_entry_offset <= self.restarts_offset < block.len()`.
            value: &block[self.value_offset..self.next_entry_offset],
        }
    }

    /// Consume this iterator, and convert it into the current `key` buffer and `value` range.
    ///
    /// If `self.valid()` is currently `true` and `self` is set to some block `block`, then
    /// `self.current()` would return a `Some(_)` entry consisting of `&key` and `&block[value]`.
    ///
    /// Additionally, in that case, the returned value range is guaranteed to have length at most
    /// `u32::MAX`.
    #[inline]
    #[must_use]
    pub fn into_raw_current(self) -> (Vec<u8>, Range<usize>) {
        (self.key, self.value_offset..self.next_entry_offset)
    }

    /// Move the iterator one position back, and return the entry at that position.
    /// Returns `None` if the iterator was at the first entry.
    ///
    /// The returned value is guaranteed to have length at most `u32::MAX`.
    ///
    /// Note that this iterator is conceptually circular rather than fused.
    ///
    /// # Speed
    /// This operation is slower than `self.next()`. If possible, this method should not be used.
    pub fn prev<'a, 'b>(
        &'a mut self,
        block: &'b [u8],
    ) -> Result<Option<BlockEntry<'a, 'b>>, CorruptedBlockError> {
        // If current is a restart, find the last key in the previous restart.
        // Else, move forwards in the current restart until self.key is reached,
        // and do not advance to it.
        // The difference in those two cases is essentially just which restart we seek to before
        // moving forwards, unless there is no previous restart.

        if !self.valid() {
            self.seek_to_last(block)?;
            return Ok(self.current(block));
        }

        // We know that `self.current_entry_offset` and `self.current_restart_idx` are
        // accurate and valid, since `self.valid()`.

        let current_entry_offset = self.current_entry_offset;
        // Since `self.current_restart_idx < self.num_restarts(_)`, this does not panic.
        let current_restart_offset = self.get_restart_usize_panicky(
            block,
            self.current_restart_idx,
        )?;
        let current_is_a_restart = current_entry_offset == current_restart_offset;

        if current_is_a_restart {
            if let Some(previous_restart_idx) = self.current_restart_idx.checked_sub(1) {
                // Move to the first entry of the previous restart.
                // This also changes `self.current_restart_idx` to the previous index.
                // This does not panic, since
                // `previous_restart_idx < self.current_restart_idx < self.num_restarts(_)`.
                self.seek_to_restart_entry_panicky(block, previous_restart_idx)?;
            } else {
                // We are at the very first restart. We _know_ that the very first entry is a
                // restart, and yada-yada-yada ~invariants of the block format~, we know that we're
                // at the the first entry. Therefore, `prev()` should move us to the phantom element
                // before the first entry (and after the last entry).
                self.reset();
                return Ok(None);
            }
        } else {
            // This is more-or-less `seek_to_restart_entry_panicky` inlined,
            // with the unnecessary stuff removed.
            if current_restart_offset >= self.restarts_offset {
                return Err(CorruptedBlockError::RestartOutOfBounds);
            }

            // We validated that `current_restart_offset < self.restarts_offset`, so the guarantee
            // of `self.next_entry_offset` and the requirement of `advance_entry` are each
            // satisfied.
            self.next_entry_offset = current_restart_offset;
            self.advance_entry(block)?;
            // After the above call, everything is as it should be, except
            // `self.current_restart_idx` has not been changed in this function.
        }

        self.advance_until(block, current_entry_offset)?;

        // Note that we need not adjust `self.current_restart_idx`.
        // If `current_is_a_restart` was taken, the restart was moved one back, and we advance
        // up to just before the following restart; so it doesn't need updating.
        // Otherwise, we started advancing from the restart entry of `self.current_restart_idx`,
        // and stopped before `current_entry_offset`, and are at least 2 entries away from the
        // next restart.

        // After successfully calling `self.advance_entry` (or `self.advance_until`), the iterator
        // is `valid()`, so we can call `current_panicky` without panicking.
        Ok(Some(self.current_panicky(block)))
    }

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
    #[expect(dead_code, reason = "this part of the interface is unused")]
    pub fn seek_to_first(&mut self, block: &[u8]) -> Result<(), CorruptedBlockError> {
        self.reset();
        self.next(block)?;
        Ok(())
    }

    /// Move the iterator to the greatest key in the collection.
    ///
    /// If the collection is empty, the iterator is `!valid()`.
    pub fn seek_to_last(&mut self, block: &[u8]) -> Result<(), CorruptedBlockError> {
        // Find and seek to the last restart point if there is one, then find the last key

        if let Some(last_restart_idx) = usize::from(self.num_restarts(block)).checked_sub(1) {
            // Move to the first entry of the previous restart.
            // This also changes `self.current_restart_idx` to the last index,
            // we don't need to adjust it again.
            self.seek_to_restart_entry_panicky(block, last_restart_idx)?;

            // If `self.next_entry_offset` is an entry, we advance further, stopping when
            // `self.next_entry_offset` is set to `self.restarts_offset`. If it already was,
            // nothing happens.
            self.advance_until(block, self.restarts_offset)?;
        } else {
            // The block is empty. Do nothing. It's necessarily the case that `!self.valid()`,
            // and nothing's changed since the creation of this iter.
        }

        Ok(())
    }

    /// Move the iterator to the smallest key which is greater or equal than the target
    /// `lower_bound` key indicated by the `by` comparator callback, which should indicate how its
    /// argument compares to the target `lower_bound` key.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the collection).
    ///
    /// # Correctness
    /// It is required for logical correctness that the block's keys were sorted in the
    /// comparator's expected order, and that no two keys compare equal to each other.
    /// The latter constraint holds true of any non-corrupt block.
    ///
    /// # Error
    /// If a [`BlockSeekError::Cmp`] error is returned, then calling [`self.current_entry_offset()`]
    /// before otherwise mutating `self` returns the offset of the entry whose key is invalid.
    ///
    /// [`self.current_entry_offset()`]: BlockIter::current_entry_offset
    pub fn try_seek_by<F, E>(&mut self, block: &[u8], mut by: F) -> Result<(), BlockSeekError<E>>
    where
        F: FnMut(&[u8]) -> Result<Ordering, E>,
    {
        // Begin with binary search. This code is largely based on `<[_]>::binary_search_by`.
        'binary_search: {
            let mut size = usize::from(self.num_restarts(block));
            if size == 0 {
                // The block is, necessarily, empty (since the first entry should always be
                // a restart).
                return Ok(());
            } else if size == 1 {
                // There's only one restart. We'll need to do linear search, starting from the
                // beginning of the whole block.
                self.reset();
                break 'binary_search;
            } else {
                // Continue below.
            }
            // `base` is kept less-than-or-equal-to the target `lower_bound`.
            let mut base = 0_usize;

            // From `std`:
            // This loop intentionally doesn't have an early exit if the comparison
            // returns `Equal`. We want the number of loop iterations to depend *only*
            // on the size of the input slice so that the CPU can reliably predict
            // the loop count.
            //
            // Alas, errors mean that there are possibly early returns, though they're cold.
            while size > 1 {
                #[expect(clippy::integer_division, reason = "rounding down is intentional")]
                let half = size / 2;
                let mid = base + half;

                let mid_cmp_lower_bound = {
                    // Should not panic, since `mid <= size / 2 + size / 4 + ... < size`,
                    // and `size == self.num_restarts(_)`.
                    self.seek_to_restart_entry_panicky(block, mid)
                        .map_err(BlockSeekError::Block)?;

                    // Note: `self.current_entry_offset` is the offset of the entry whose
                    // key might be found invalid here.
                    by(&self.key).map_err(BlockSeekError::Cmp)?
                };

                // Binary search interacts poorly with branch prediction, so force
                // the compiler to use conditional moves if supported by the target
                // architecture.
                // TODO: once MSRV is high enough, use this.
                // base = select_unpredictable(mid_cmp_lower_bound == Ordering::Greater, base, mid);
                base = if mid_cmp_lower_bound.is_gt() { base } else { mid };

                // This is imprecise in the case where `size` is odd and the
                // comparison returns `Greater`: the mid element still gets included
                // by `size` even though it's known to be larger than the element
                // being searched for.
                //
                // This is fine though: we gain more performance by keeping the
                // loop iteration count invariant (and thus predictable) than we
                // lose from considering one additional element.
                size -= half;
            }

            // We've called `seek_to_restart_entry_panicky` at least once by the time we get here,
            // so `self.valid()`. In particular, we are at the entry corresponding to the restart
            // at index `base`.
            // Note: `self.current_entry_offset` is the offset of the entry whose
            // key might be found invalid here.
            let base_cmp_lower_bound = by(&self.key).map_err(BlockSeekError::Cmp)?;

            if base_cmp_lower_bound.is_eq() {
                // We got lucky, no linear search needed.
                return Ok(());
            }
        }

        // Note: if we get here, either `!self.valid()` (from `self.reset()` above)
        // or `base` refers to a reset whose entry's key was compared above and found
        // to be not equal. Since `base_key <= lower_bound` at all times, this implies
        // `base_key < lower_bound`. Since the phantom entry is before all entries, in either case,
        // the entry that we're currently at is strictly before `lower_bound`.
        //
        // Thus, in the below linear search, we only check `self.next()`, and the first entry
        // that compares greater than or equal to `lower_bound` is the target.

        // If we manage to search until the end of the list, then there's no element
        // greater than or equal to `key`, so we correctly become `!valid()`.
        while self.next(block).map_err(BlockSeekError::Block)?.is_some() {
            // Note: `self.current_entry_offset` is the offset of the entry whose
            // key might be found invalid here.
            if by(&self.key).map_err(BlockSeekError::Cmp)?.is_ge() {
                return Ok(());
            }
        }

        Ok(())
    }

    /// Move the iterator to the greatest key which is strictly less than the target
    /// `strict_upper_bound` key indicated by the `by` comparator callback, which should indicate
    /// how its argument compares to the target `strict_upper_bound` key.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the collection).
    ///
    /// # Speed
    /// This operation uses `self.prev()`, and is thus somewhat inefficient. If possible,
    /// this method should be avoided in favor of `self.try_seek_by()`.
    ///
    /// # Correctness
    /// It is required for logical correctness that the block's keys were sorted in the
    /// comparator's expected order, and that no two keys compare equal to each other.
    /// The latter constraint holds true of any non-corrupt block.
    pub fn try_seek_before_by<F, E>(&mut self, block: &[u8], by: F) -> Result<(), BlockSeekError<E>>
    where
        F: FnMut(&[u8]) -> Result<Ordering, E>,
    {
        self.try_seek_by(block, by)?;
        // Doing this more efficiently would require the ability to peek at the next key without
        // destroying the current key, which is a hassle, and would require a second buffer.
        self.prev(block).map_err(BlockSeekError::Block)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BlockEntry<'key, 'value> {
    pub key:   &'key [u8],
    pub value: &'value [u8],
}
