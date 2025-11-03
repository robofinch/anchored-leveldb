use std::fmt::{Debug, Formatter, Result as FmtResult};

use crate::public_format::EntryType;
use crate::format::{InternalKey, SequenceNumber, UserKey};


#[derive(Debug)]
pub(crate) struct CompactionPointer {
    user_key:        Vec<u8>,
    sequence_number: SequenceNumber,
    entry_type:      EntryType,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl CompactionPointer {
    #[inline]
    #[must_use]
    pub fn new(internal_key: InternalKey<'_>) -> Self {
        Self {
            user_key:        internal_key.user_key.0.to_owned(),
            sequence_number: internal_key.sequence_number,
            entry_type:      internal_key.entry_type,
        }
    }

    #[inline]
    #[must_use]
    pub fn internal_key(&self) -> InternalKey<'_> {
        InternalKey {
            user_key:        UserKey(&self.user_key),
            sequence_number: self.sequence_number,
            entry_type:      self.entry_type,
        }
    }
}

/// An optional [`InternalKey`] value.
pub(crate) struct OptionalCompactionPointer {
    /// Invariant: if `self.valid` is true, then the other three fields store an [`InternalKey`]
    /// which was previously provided to [`OptionalCompactionPointer::set`].
    valid:           bool,
    user_key:        Vec<u8>,
    sequence_number: SequenceNumber,
    entry_type:      EntryType,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl OptionalCompactionPointer {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            valid:           false,
            user_key:        Vec::new(),
            sequence_number: SequenceNumber::ZERO,
            entry_type:      EntryType::MIN_TYPE,
        }
    }

    #[inline]
    pub const fn clear(&mut self) {
        self.valid = false;
    }

    #[inline]
    pub fn set(&mut self, key: InternalKey<'_>) {
        self.user_key.clear();
        self.user_key.extend(key.user_key.0);
        self.sequence_number = key.sequence_number;
        self.entry_type      = key.entry_type;
        self.valid           = true;
    }

    #[inline]
    #[must_use]
    pub fn internal_key(&self) -> Option<InternalKey<'_>> {
        self.valid.then_some(InternalKey {
            user_key:        UserKey(&self.user_key),
            sequence_number: self.sequence_number,
            entry_type:      self.entry_type,
        })
    }

    #[inline]
    #[must_use]
    pub fn compaction_pointer(self) -> Option<CompactionPointer> {
        self.valid.then_some(CompactionPointer {
            user_key:        self.user_key,
            sequence_number: self.sequence_number,
            entry_type:      self.entry_type,
        })
    }
}

impl Default for OptionalCompactionPointer {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for OptionalCompactionPointer {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Debug::fmt(&self.internal_key(), f)
    }
}
