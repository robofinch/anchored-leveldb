#[cfg(target_pointer_width = "16")]
mod inner {
    /// A type which is either `u32` or `usize`, whichever is smaller.
    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    #[repr(transparent)]
    pub struct MinU32Usize(usize);

    impl MinU32Usize {
        pub const ZERO: Self = Self(0);

        #[inline]
        #[must_use]
        pub fn from_u32(value: u32) -> Option<Self> {
            usize::try_from(value).ok().map(Self)
        }

        #[allow(clippy::missing_const_for_fn, reason = "match API across pointer sizes")]
        #[inline]
        #[must_use]
        pub fn from_usize(value: usize) -> Option<Self> {
            Some(Self(value))
        }

        #[inline]
        #[must_use]
        pub fn checked_sub(self, other: Self) -> Option<Self> {
            // The result, if `Some`, is less than or equal to `self.0`, and thus fits in
            // a `MinU32Usize`.
            Some(Self(self.0.checked_sub(other.0)?))
        }
    }

    impl From<MinU32Usize> for u32 {
        #[inline]
        fn from(value: MinU32Usize) -> Self {
            #![allow(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "pointer size is known to ensure that no overflow occurs",
            )]
            value.0 as Self
        }
    }

    impl From<MinU32Usize> for usize {
        #[inline]
        fn from(value: MinU32Usize) -> Self {
            value.0
        }
    }
}

#[cfg(not(target_pointer_width = "16"))]
mod inner {
    /// A type which is either `u32` or `usize`, whichever is smaller.
    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    #[repr(transparent)]
    pub struct MinU32Usize(u32);

    impl MinU32Usize {
        pub const ZERO: Self = Self(0);

        #[allow(clippy::missing_const_for_fn, reason = "match API across pointer sizes")]
        #[inline]
        #[must_use]
        pub fn from_u32(value: u32) -> Option<Self> {
            Some(Self(value))
        }

        #[inline]
        #[must_use]
        pub fn from_usize(value: usize) -> Option<Self> {
            u32::try_from(value).ok().map(Self)
        }

        #[inline]
        #[must_use]
        pub fn checked_sub(self, other: Self) -> Option<Self> {
            // The result, if `Some`, is less than or equal to `self.0`, and thus fits in
            // a `MinU32Usize`.
            Some(Self(self.0.checked_sub(other.0)?))
        }
    }

    impl From<MinU32Usize> for u32 {
        #[inline]
        fn from(value: MinU32Usize) -> Self {
            value.0
        }
    }

    impl From<MinU32Usize> for usize {
        #[inline]
        fn from(value: MinU32Usize) -> Self {
            #![allow(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "pointer size is known to ensure that no overflow occurs",
            )]
            value.0 as Self
        }
    }
}


pub use self::inner::MinU32Usize;
