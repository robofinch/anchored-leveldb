use crate::all_errors::types::InvalidInternalKey;
use crate::pub_typed_bytes::{EntryType, MinU32Usize, SequenceNumber, ShortSlice};
use super::user::{MaybeUserValue, OwnedUserKey, UserKey, UserValue};


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

    #[inline]
    #[must_use]
    pub fn to_owned(self) -> OwnedInternalKey {
        OwnedInternalKey(self.0.to_owned(), self.1)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedInternalKey(pub OwnedUserKey, pub InternalKeyTag);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl OwnedInternalKey {
    #[inline]
    #[must_use]
    pub fn borrow(&self) -> InternalKey<'_> {
        InternalKey(self.0.borrow(), self.1)
    }

    pub fn set_optional(dst: &mut Option<Self>, src: Option<InternalKey<'_>>) {
        if let Some(src_key) = src {
            if let Some(dst_key) = dst {
                src_key.0.clone_into(&mut dst_key.0);
                dst_key.1 = src_key.1;
            } else {
                *dst = Some(src_key.to_owned());
            }
        } else {
            *dst = None;
        }
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

    #[inline]
    #[must_use]
    pub const fn not_deleted_user_value(self) -> UserValue<'a> {
        UserValue(self.1.0)
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct InternalKeyTag(u64);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl InternalKeyTag {
    /// Note that internal keys' key tags are sorted in descending order; this key tag has the
    /// minimum numeric value for a key tag, which makes it the last tag in descending order.
    pub const MIN_KEY_TAG: Self = Self::new(
        SequenceNumber::ZERO,
        EntryType::MIN_TYPE,
    );
    /// Note that internal keys' key tags are sorted in descending order; this key tag has the
    /// maximum numeric value for a key tag, which makes it the first tag in descending order.
    pub const MAX_KEY_TAG: Self = Self::new(
        SequenceNumber::MAX_SEQUENCE_NUMBER,
        EntryType::MAX_TYPE,
    );

    #[inline]
    #[must_use]
    pub const fn new(sequence_number: SequenceNumber, entry_type: EntryType) -> Self {
        // TODO(const-hack): let entry_type = u64::from(u8::from(entry_type));
        #[expect(clippy::as_conversions, reason = "const-hack for `From::from`")]
        let entry_type = (entry_type as u8) as u64;
        Self((sequence_number.inner() << 8) | entry_type)
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

/// A user key followed by an 8-byte suffix from a little-endian [`InternalKeyTag`].
///
/// The user key *should* be comparable. Otherwise, downstream panics may occur.
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
            #[expect(clippy::expect_used, reason = "cannot panic; length is validated above")]
            let validated = ShortSlice::new(unvalidated.0)
                .expect("`unvalidated` was validated to fit in a `ShortSlice`");
            Ok(Self(validated))
        } else {
            Err(InvalidInternalKey::Truncated)
        }
    }

    /// # Panics or Errors
    /// `EncodedInternalKey::validate(UnvalidatedInternalKey(validated_encoded_key, _))` **must**
    /// return `Ok(_)`; otherwise, panics or other errors may occur, either in this function
    /// or by downstream users of the returned value.
    #[inline]
    #[must_use]
    pub const fn new_unchecked(validated_encoded_key: &'a [u8]) -> Self {
        #[expect(clippy::expect_used, reason = "caller is warned about the panic")]
        let validated = ShortSlice::new(validated_encoded_key)
            .expect("`EncodedInternalKey::new_unchecked` called on an invalid key");
        Self(validated)
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
    pub const fn len(self) -> MinU32Usize {
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct EncodedInternalEntry<'a>(pub EncodedInternalKey<'a>, pub MaybeUserValue<'a>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> EncodedInternalEntry<'a> {
    pub fn validate<V, E>(
        unvalidated:       UnvalidatedInternalEntry<'a>,
        validate_user_key: V,
    ) -> Result<Self, InvalidInternalKey<E>>
    where
        V: FnOnce(UserKey<'_>) -> Result<(), E>,
    {
        let internal_key = EncodedInternalKey::validate(
            unvalidated.0,
            validate_user_key,
        )?;

        Ok(Self(internal_key, unvalidated.1))
    }

    /// # Correctness
    /// Must only be called on entries which have already been previously validated.
    ///
    /// Otherwise, panics or other errors may occur (either here or downstream).
    #[inline]
    #[must_use]
    pub const fn new_unchecked(validated_entry: UnvalidatedInternalEntry<'a>) -> Self {
        Self(
            EncodedInternalKey::new_unchecked(validated_entry.0.0),
            validated_entry.1,
        )
    }

    #[inline]
    #[must_use]
    pub fn user_key(self) -> UserKey<'a> {
        self.0.as_internal_key().0
    }

    /// The returned value is meaningless if this entry is a `Deletion` entry.
    #[inline]
    #[must_use]
    pub const fn not_deleted_user_value(self) -> UserValue<'a> {
        UserValue(self.1.0)
    }

    #[inline]
    #[must_use]
    pub fn as_internal_entry(self) -> InternalEntry<'a> {
        InternalEntry(self.0.as_internal_key(), self.1)
    }
}

/// *Should* be an [`EncodedInternalKey`] (forming an [`InternalEntry`]), but might not be.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UnvalidatedInternalEntry<'a>(
    pub UnvalidatedInternalKey<'a>,
    pub MaybeUserValue<'a>,
);
