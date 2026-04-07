use std::fmt::{Debug, Formatter, Result as FmtResult};

use crate::pub_typed_bytes::{EntryType, SequenceNumber};
use super::{
    internal_key::{InternalKey, InternalKeyTag, OwnedInternalKey},
    user::{OwnedUserKey, UserKey},
};

/// An optional [`OwnedInternalKey`] value.
pub(crate) struct OptionalCompactionPointer {
    /// Either a comparable `OwnedUserKey`, or a random value.
    user_key: Vec<u8>,
    key_tag:  InternalKeyTag,
    /// Should be `true` if and only if `user_key` is a validated user key.
    valid:    bool,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl OptionalCompactionPointer {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            user_key: Vec::new(),
            key_tag:  InternalKeyTag::new(SequenceNumber::ZERO, EntryType::MIN_TYPE),
            valid:    false,
        }
    }

    #[inline]
    pub const fn clear(&mut self) {
        self.valid = false;
    }

    #[inline]
    pub fn set(&mut self, key: InternalKey<'_>) {
        self.user_key.clear();
        self.user_key.extend(key.0.inner());
        self.key_tag = key.1;
        self.valid = true;
    }

    #[inline]
    #[must_use]
    pub fn internal_key(&self) -> Option<InternalKey<'_>> {
        if self.valid {
            Some(InternalKey(
                // Correctness: since `self.valid` is `true`, `self.user_key` was set to a valid
                // `UserKey` above in `self.set(_)`.
                #[expect(clippy::expect_used, reason = "panic is impossible")]
                UserKey::new(&self.user_key).expect("set to a valid `UserKey` in `self.set(_)`"),
                self.key_tag,
            ))
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    pub fn owned_internal_key(self) -> Option<OwnedInternalKey> {
        if self.valid {
            #[expect(clippy::expect_used, reason = "panic is impossible")]
            let user_key = OwnedUserKey::new(self.user_key)
                .expect("set to a valid `UserKey` in `self.set(_)`");

            Some(OwnedInternalKey(user_key, self.key_tag))
        } else {
            None
        }
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
        f.debug_tuple("OptionalCompactionPointer")
            .field(&self.internal_key())
            .finish()
    }
}
