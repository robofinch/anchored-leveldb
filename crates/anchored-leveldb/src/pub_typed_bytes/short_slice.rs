use crate::pub_typed_bytes::MinU32Usize;


/// A byte slice with length at most `u32::MAX`.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct ShortSlice<'a>(&'a [u8]);

impl<'a> ShortSlice<'a> {
    pub const EMPTY: Self = Self(&[]);

    /// Return a new `ShortSlice` if the input's length is at most `u32::MAX`.
    #[inline]
    #[must_use]
    pub fn new(slice: &'a [u8]) -> Option<Self> {
        // If `u32::MAX` doesn't fit in a `usize`, then `slice` cannot possibly be too long.
        if usize::try_from(u32::MAX).is_ok_and(|max_len| slice.len() > max_len) {
            None
        } else {
            Some(Self(slice))
        }
    }

    /// `slice` **must** have length at most `u32::MAX`; otherwise, downstream panics or other
    /// errors may occur.
    #[inline]
    #[must_use]
    pub const fn new_unchecked(slice: &'a [u8]) -> Self {
        Self(slice)
    }

    /// Get the inner slice, whose length is at most `u32::MAX`.
    #[inline]
    #[must_use]
    pub const fn inner(self) -> &'a [u8] {
        self.0
    }

    /// Get the length of the slice as a [`MinU32Usize`].
    #[inline]
    #[must_use]
    pub const fn len(self) -> MinU32Usize {
        #![expect(clippy::missing_panics_doc, reason = "false positive")]
        #[expect(clippy::expect_used, reason = "verified at construction")]
        MinU32Usize::from_usize(self.0.len()).expect("`ShortSlice.0.len()` must be `<= u32::MAX`")
    }
}
