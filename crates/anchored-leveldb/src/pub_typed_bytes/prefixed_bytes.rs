use crate::{all_errors::types::PrefixedBytesParseError, utils::decode_varint32};
use super::short_slice::ShortSlice;


#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct PrefixedBytes<'a>(&'a [u8]);

impl<'a> PrefixedBytes<'a> {
    #[inline]
    pub fn new(prefixed_bytes: &'a [u8]) -> Result<Self, PrefixedBytesParseError> {
        let (slice_len, slice_len_len) = decode_varint32(prefixed_bytes)?;

        let Ok(slice_len) = usize::try_from(slice_len) else {
            // The `prefixed_bytes` slice cannot possibly have length greater than `usize::MAX`.
            return Err(PrefixedBytesParseError::TruncatedSlice);
        };

        let Some(total_len) = slice_len_len.checked_add(slice_len) else {
            // The `prefixed_bytes` slice cannot possibly have length greater than `usize::MAX`.
            return Err(PrefixedBytesParseError::TruncatedSlice);
        };

        let Some(prefixed_bytes) = prefixed_bytes.get(..total_len) else {
            // The slice isn't long enough.
            return Err(PrefixedBytesParseError::TruncatedSlice);
        };

        Ok(Self(prefixed_bytes))
    }

    #[inline]
    #[must_use]
    pub const fn prefixed_inner(self) -> &'a [u8] {
        self.0
    }

    #[expect(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::missing_panics_doc,
        reason = "validated on construction",
    )]
    #[must_use]
    pub fn unprefixed_inner(self) -> ShortSlice<'a> {
        let (_, slice_len_len) = decode_varint32(self.prefixed_inner())
            .expect("PrefixedBytes struct should begin with a varint32 prefix");

        // The unprefixed data has a length indicated by a varint32, and thus its length cannot
        // exceed `u32::MAX`.
        ShortSlice::new_unchecked(&self.0[slice_len_len..])
    }
}

pub(crate) trait ReadPrefixedBytes<'a> {
    fn read_prefixed_bytes(&mut self) -> Result<PrefixedBytes<'a>, PrefixedBytesParseError>;
}

impl<'a> ReadPrefixedBytes<'a> for &'a [u8] {
    fn read_prefixed_bytes(&mut self) -> Result<PrefixedBytes<'a>, PrefixedBytesParseError> {
        let prefixed_bytes = PrefixedBytes::new(self)?;
        #[expect(
            clippy::indexing_slicing,
            reason = "a parsed PrefixedBytes is a prefix of the input",
        )]
        {
            *self = &self[prefixed_bytes.prefixed_inner().len()..];
        };
        Ok(prefixed_bytes)
    }
}
