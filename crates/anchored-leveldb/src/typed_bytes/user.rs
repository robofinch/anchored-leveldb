use crate::pub_typed_bytes::{MinU32Usize, ShortSlice};


/// Has length at most `u32::MAX - 8` and at most `usize::MAX - 8`.
///
/// Should be comparable with the user comparator; otherwise, panics or other errors may occur.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct UserKey<'a>(ShortSlice<'a>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> UserKey<'a> {
    /// # Downstream Panics
    /// `user_key` should be comparable with the user comparator; otherwise, downstream panics or
    /// other errors may occur when it is assumed comparator.
    #[inline]
    #[must_use]
    pub const fn new(user_key: &'a [u8]) -> Option<Self> {
        if cfg!(target_pointer_width = "16") {
            if user_key.len() > usize::MAX - 8 {
                return None;
            }
        } else {
            #[expect(
                clippy::as_conversions,
                reason = "when `target_pointer_width >= 16`, no truncation occurs",
            )]
            let max_len = (u32::MAX - 8) as usize;
            if user_key.len() > max_len {
                return None;
            }
        }

        // We validate that `user_key.len() <= u32::MAX - 8 <= u32::MAX`
        // and `user_key.len() <= usize::MAX - 8`.
        #[expect(clippy::expect_used, reason = "cannot panic; length is validated above")]
        Some(Self(ShortSlice::new(user_key).expect("`user_key.len() <= usize::MAX - 8`")))
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

    #[inline]
    #[must_use]
    pub fn to_owned(self) -> OwnedUserKey {
        OwnedUserKey(self.0.inner().to_owned())
    }

    #[inline]
    #[must_use]
    pub fn to_owned_with_buf(self, mut buffer: Vec<u8>) -> OwnedUserKey {
        self.0.inner().clone_into(&mut buffer);
        OwnedUserKey(buffer)
    }

    #[inline]
    pub fn clone_into(self, owned: &mut OwnedUserKey) {
        self.0.inner().clone_into(&mut owned.0);
    }
}

/// Has length at most `u32::MAX - 8` and at most `usize::MAX - 8`.
#[derive(Debug, Clone)]
pub(crate) struct OwnedUserKey(Vec<u8>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl OwnedUserKey {
    #[inline]
    #[must_use]
    pub fn new(user_key: Vec<u8>) -> Option<Self> {
        if UserKey::new(&user_key).is_some() {
            Some(Self(user_key))
        } else {
            None
        }
    }

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
        #[expect(clippy::expect_used, reason = "verified at construction")]
        UserKey(ShortSlice::new(&self.0).expect("`OwnedUserKey.0.len() <= u32::MAX`"))
    }
}

/// Has length at most `u32::MAX`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UserValue<'a>(pub ShortSlice<'a>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> UserValue<'a> {
    #[inline]
    #[must_use]
    pub fn new(user_value: &'a [u8]) -> Option<Self> {
        ShortSlice::new(user_value).map(Self)
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

    #[inline]
    #[must_use]
    pub fn to_owned_with_buf(self, mut buffer: Vec<u8>) -> OwnedUserValue {
        self.0.inner().clone_into(&mut buffer);
        OwnedUserValue(buffer)
    }

    #[inline]
    pub fn clone_into(self, owned: &mut OwnedUserValue) {
        self.0.inner().clone_into(&mut owned.0);
    }
}

/// Has length at most `u32::MAX`.
#[derive(Debug, Clone)]
pub(crate) struct OwnedUserValue(Vec<u8>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl OwnedUserValue {
    #[inline]
    #[must_use]
    pub fn new(user_value: Vec<u8>) -> Option<Self> {
        if UserValue::new(&user_value).is_some() {
            Some(Self(user_value))
        } else {
            None
        }
    }

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
        MinU32Usize::from_usize(self.0.len()).expect("`OwnedUserValue.0.len() <= u32::MAX`")
    }

    #[inline]
    #[must_use]
    pub fn borrow(&self) -> UserValue<'_> {
        #[expect(clippy::expect_used, reason = "verified at construction")]
        UserValue(ShortSlice::new(&self.0).expect("`OwnedUserValue.0.len() <= u32::MAX`"))
    }
}

/// Either a [`UserValue`] or some irrelevant byte slice (likely the empty slice).
///
/// Has length at most `u32::MAX`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MaybeUserValue<'a>(pub ShortSlice<'a>);
