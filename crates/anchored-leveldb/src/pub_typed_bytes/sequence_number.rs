use crate::all_errors::types::OutOfSequenceNumbers;


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    pub const ZERO: Self = Self(0);
    pub(crate) const MAX_USABLE_SEQUENCE_NUMBER: Self = Self((1 << 56) - 2);
    /// Should not be used as the sequence number of a write entry, in order to ensure that
    /// separators between internal keys work properly.
    ///
    /// Intended for separators between internal keys and for searching through internal keys.
    pub(crate) const MAX_SEQUENCE_NUMBER: Self = Self((1 << 56) - 1);

    /// `sequence_number` must be strictly less than `1 << 56`.
    #[inline]
    #[must_use]
    pub const fn new(sequence_number: u64) -> Option<Self> {
        if sequence_number <= Self::MAX_SEQUENCE_NUMBER.0 {
            Some(Self(sequence_number))
        } else {
            None
        }
    }

    /// Get the inner value of this sequence number, guaranteed to be strictly less than `1 << 56`.
    #[inline]
    #[must_use]
    pub const fn inner(self) -> u64 {
        self.0
    }

    /// Returns `SequenceNumber(sequence_number)` if the result would be a valid sequence number
    /// which could be used normally as the sequence number of a write entry.
    #[inline]
    #[must_use]
    pub(crate) const fn new_usable(sequence_number: u64) -> Option<Self> {
        if sequence_number <= Self::MAX_USABLE_SEQUENCE_NUMBER.0 {
            Some(Self(sequence_number))
        } else {
            None
        }
    }

    /// Attempts to return `SequenceNumber(last_sequence.inner() + additional)`, checking that
    /// overflow does not occur and that the result is a valid sequence number usable for
    /// write entries.
    ///
    /// If this returns `Ok`, then every sequence number from `last_sequence` up to
    /// the returned sequence number, inclusive, are guaranteed to be valid and usable sequence
    /// numbers.
    #[inline]
    pub(crate) fn checked_add(self, additional: u64) -> Result<Self, OutOfSequenceNumbers> {
        let new_sequence_number = self.0.checked_add(additional).ok_or(OutOfSequenceNumbers)?;

        if new_sequence_number <= Self::MAX_USABLE_SEQUENCE_NUMBER.0 {
            Ok(Self(new_sequence_number))
        } else {
            Err(OutOfSequenceNumbers)
        }
    }

    /// Attempts to return `SequenceNumber(last_sequence.inner() + u64::from(additional))`,
    /// checking that overflow does not occur and that the result is a valid sequence
    /// number usable for write entries.
    ///
    /// If this returns `Ok`, then every sequence number from `last_sequence` up to the returned
    /// sequence number, inclusive, are guaranteed to be valid and usable sequence numbers.
    #[inline]
    pub(crate) fn checked_add_u32(self, additional: u32) -> Result<Self, OutOfSequenceNumbers> {
        self.checked_add(u64::from(additional))
    }

    /// Attempts to decrease `self` by 1. Returns `None` if `self.inner()` is `0`.
    ///
    /// The result, if `Some`, is a valid and usable sequence number.
    #[inline]
    pub(crate) fn checked_decrement(self) -> Option<Self> {
        // We can assume that `self` is valid.
        self.0.checked_sub(1).map(Self)
    }
}
