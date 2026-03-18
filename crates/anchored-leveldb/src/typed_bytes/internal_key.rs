use crate::all_errors::types::InvalidInternalKey;
use crate::pub_typed_bytes::{EntryType, MinU32Usize, SequenceNumber, ShortSlice};
use super::user::{MaybeUserValue, UserKey};


#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalKey<'a>(pub UserKey<'a>, pub InternalKeyTag);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl InternalKey<'_> {
    #[inline]
    pub fn append_encoded(self, output: &mut Vec<u8>) {
        // This should not overflow, by the invariant of `UserKey`.
        // (That is, user keys are at most `u32::MAX - 8` bytes in length, AND
        // at most `usize::MAX - 8` bytes in length.)
        output.reserve(usize::from(self.0.len()) + 8);
        output.extend(self.0.inner());
        output.extend(self.1.raw_inner().to_le_bytes().as_slice());
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct InternalKeyTag(u64);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl InternalKeyTag {
    #[inline]
    #[must_use]
    pub fn new(sequence_number: SequenceNumber, entry_type: EntryType) -> Self {
        Self((sequence_number.inner() << 8) | u64::from(u8::from(entry_type)))
    }

    #[inline]
    #[must_use]
    pub fn new_raw(data: u64) -> Option<Self> {
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "truncation to least-significant byte is intentional",
        )]
        let entry_type = data as u8;
        if EntryType::try_from(entry_type).is_ok() {
            Some(Self(data))
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    pub const fn raw_inner(self) -> u64 {
        self.0
    }

    #[inline]
    #[must_use]
    pub const fn sequence_number(self) -> SequenceNumber {
        #[expect(clippy::expect_used, reason = "regardless of `self.0`'s value, this succeeds")]
        SequenceNumber::new(self.0 >> 8).expect("(u64::MAX >> 8) < (1 << 56)")
    }

    #[inline]
    #[must_use]
    pub fn entry_type(self) -> EntryType {
        #[expect(
            clippy::expect_used,
            reason = "condition for this call to succeed is checked in constructor",
        )]
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "truncation to least-significant byte is intentional",
        )]
        EntryType::try_from(self.0 as u8)
            .expect("least-significant byte of a key tag should be valid entry type")
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LookupKey<'a>(pub UserKey<'a>, pub CmpSequenceTag);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> LookupKey<'a> {
    #[inline]
    #[must_use]
    pub const fn as_internal_key(self) -> InternalKey<'a> {
        InternalKey(self.0, self.1.as_internal_key_tag())
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct CmpSequenceTag(u64);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl CmpSequenceTag {
    #[inline]
    #[must_use]
    pub fn new(sequence_number: SequenceNumber) -> Option<Self> {
        if sequence_number < SequenceNumber::MAX_SEQUENCE_NUMBER {
            Some(Self((sequence_number.inner() << 8) | u64::from(u8::from(EntryType::MAX_TYPE))))
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    pub const fn as_internal_key_tag(self) -> InternalKeyTag {
        InternalKeyTag(self.0)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalEntry<'a>(pub InternalKey<'a>, pub MaybeUserValue<'a>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> InternalEntry<'a> {
    #[inline]
    #[must_use]
    pub const fn user_key(self) -> UserKey<'a> {
        self.0.0
    }
}

/// A user key followed by an 8-byte suffix from a little-endian [`InternalKeyTag`].
///
/// The user key *should* be comparable.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct EncodedInternalKey<'a>(ShortSlice<'a>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> EncodedInternalKey<'a> {
    pub fn validate<V, E>(
        unvalidated:       UnvalidatedInternalKey<'a>,
        validate_user_key: V,
    ) -> Result<Self, InvalidInternalKey<E>>
    where
        V: FnOnce(UserKey<'_>) -> Result<(), E>,
    {
        if let Some(user_key_len) = unvalidated.0.len().checked_sub(8) {
            // Since the key tag is in little-endian order, the first byte of the 8-byte suffix
            // is the entry type (which is the least-significant byte).
            #[expect(clippy::indexing_slicing, reason = "unvalidated.0.len() > user_key_len")]
            let entry_type = unvalidated.0[user_key_len];
            if EntryType::try_from(entry_type).is_err() {
                return Err(InvalidInternalKey::UnknownEntryType(entry_type));
            }

            #[expect(clippy::indexing_slicing, reason = "unvalidated.0.len() >= user_key_len")]
            let user_key = UserKey::new(&unvalidated.0[..user_key_len])
                .ok_or(InvalidInternalKey::TooLong)?;

            validate_user_key(user_key).map_err(|err| {
                InvalidInternalKey::InvalidUserKey(Box::from(user_key.inner()), err)
            })?;

            // Since `user_key.len() <= u32::MAX - 8`, it follows that
            // `unvalidated.len() == user_key_len + 8 <= u32::MAX`.
            Ok(Self(ShortSlice::new_unchecked(unvalidated.0)))
        } else {
            Err(InvalidInternalKey::Truncated)
        }
    }

    /// `EncodedInternalKey::validate(UnvalidatedInternalKey(validated_encoded_key))` **must**
    /// return `Ok(_)`; otherwise, downstream panics or other errors may occur.
    #[inline]
    #[must_use]
    pub fn new_unchecked(validated_encoded_key: &'a [u8]) -> Self {
        Self(ShortSlice::new_unchecked(validated_encoded_key))
    }

    #[inline]
    #[must_use]
    pub const fn short(self) -> ShortSlice<'a> {
        self.0
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> &'a [u8] {
        self.0.inner()
    }

    #[inline]
    #[must_use]
    pub fn len(self) -> MinU32Usize {
        self.0.len()
    }

    #[expect(clippy::expect_used, reason = "this type is validated on construction")]
    #[inline]
    #[must_use]
    pub fn as_internal_key(self) -> InternalKey<'a> {
        let (user_key, key_tag) = self.0.inner()
            .split_last_chunk()
            .expect("EncodedInternalKey is validated");

        let user_key = UserKey::new(user_key).expect("EncodedInternalKey is validated");
        let key_tag = u64::from_le_bytes(*key_tag);
        let key_tag = InternalKeyTag::new_raw(key_tag).expect("EncodedInternalKey is validated");

        InternalKey(user_key, key_tag)
    }
}

/// *Should* be an [`EncodedInternalKey`], but might not be.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct UnvalidatedInternalKey<'a>(pub &'a [u8]);

/// *Should* be an [`EncodedInternalKey`] (forming an [`InternalEntry`]), but might not be.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UnvalidatedInternalEntry<'a>(
    pub UnvalidatedInternalKey<'a>,
    pub MaybeUserValue<'a>,
);
