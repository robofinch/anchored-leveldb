use crate::pub_typed_bytes::MinU32Usize;


/// Has length at most `u32::MAX - 8`.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct UserKey<'a>(&'a [u8]);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> UserKey<'a> {
    #[inline]
    #[must_use]
    pub fn new(user_key: &'a [u8]) -> Option<Self> {
        // If `u32::MAX - 8` doesn't fit in a `usize`, then `user_key` cannot possibly
        // be too long.
        if usize::try_from(u32::MAX - 8).is_ok_and(|max_len| user_key.len() > max_len) {
            None
        } else {
            Some(Self(user_key))
        }
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> &'a [u8] {
        self.0
    }

    #[inline]
    #[must_use]
    pub fn len(self) -> MinU32Usize {
        #[expect(clippy::expect_used, reason = "verified at construction")]
        MinU32Usize::from_usize(self.0.len()).expect("`UserKey.0.len() <= u32::MAX`")
    }

    #[inline]
    #[must_use]
    pub fn to_owned(self) -> OwnedUserKey {
        OwnedUserKey(self.0.to_owned())
    }
}

/// Has length at most `u32::MAX - 8`.
#[derive(Debug, Clone)]
pub(crate) struct OwnedUserKey(Vec<u8>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl OwnedUserKey {
    #[inline]
    #[must_use]
    pub const fn inner(&self) -> &Vec<u8> {
        &self.0
    }

    #[inline]
    #[must_use]
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    #[inline]
    #[must_use]
    pub fn len(self) -> MinU32Usize {
        #[expect(clippy::expect_used, reason = "verified at construction")]
        MinU32Usize::from_usize(self.0.len()).expect("`OwnedUserKey.0.len() <= u32::MAX`")
    }

    #[inline]
    #[must_use]
    pub fn borrow(&self) -> UserKey<'_> {
        UserKey(&self.0)
    }
}

/// Has length at most `u32::MAX`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UserValue<'a>(&'a [u8]);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> UserValue<'a> {
    #[inline]
    #[must_use]
    pub fn new(user_value: &'a [u8]) -> Option<Self> {
        // If `u32::MAX` doesn't fit in a `usize`, then `user_value` cannot possibly be too long.
        if usize::try_from(u32::MAX).is_ok_and(|max_len| user_value.len() > max_len) {
            None
        } else {
            Some(Self(user_value))
        }
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> &'a [u8] {
        self.0
    }

    #[inline]
    #[must_use]
    pub fn len(self) -> MinU32Usize {
        #[expect(clippy::expect_used, reason = "verified at construction")]
        MinU32Usize::from_usize(self.0.len()).expect("`UserValue.0.len() <= u32::MAX`")
    }
}

/// Either a [`UserValue`] or some irrelevant byte slice (likely the empty slice).
///
/// Has length at most `u32::MAX`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MaybeUserValue<'a>(&'a [u8]);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> MaybeUserValue<'a> {
    pub const EMPTY: Self = Self(&[]);

    #[inline]
    #[must_use]
    pub fn new(value: &'a [u8]) -> Option<Self> {
        // If `u32::MAX` doesn't fit in a `usize`, then `value` cannot possibly be too long.
        if usize::try_from(u32::MAX).is_ok_and(|max_len| value.len() > max_len) {
            None
        } else {
            Some(Self(value))
        }
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> &'a [u8] {
        self.0
    }

    #[inline]
    #[must_use]
    pub fn len(self) -> MinU32Usize {
        #[expect(clippy::expect_used, reason = "verified at construction")]
        MinU32Usize::from_usize(self.0.len()).expect("`MaybeUserValue.0.len() <= u32::MAX`")
    }
}
