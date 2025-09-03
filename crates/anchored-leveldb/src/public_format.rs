use integer_encoding::VarInt as _;


#[derive(Debug, Clone, Copy)]
pub enum WriteEntry<'a> {
    Value {
        key:   LengthPrefixedBytes<'a>,
        value: LengthPrefixedBytes<'a>,
    },
    Deletion {
        key:   LengthPrefixedBytes<'a>,
    }
}

impl WriteEntry<'_> {
    #[inline]
    #[must_use]
    pub fn entry_type(&self) -> EntryType {
        match self {
            Self::Value { .. }    => EntryType::Value,
            Self::Deletion { .. } => EntryType::Deletion,
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum EntryType {
    Deletion = 0,
    Value    = 1,
}

impl EntryType {
    pub(crate) const MIN_TYPE: Self = Self::Deletion;
    pub(crate) const MAX_TYPE: Self = Self::Value;
}

impl From<EntryType> for u8 {
    #[inline]
    fn from(entry_type: EntryType) -> Self {
        entry_type as u8
    }
}

impl TryFrom<u8> for EntryType {
    type Error = ();

    #[inline]
    fn try_from(entry_type: u8) -> Result<Self, Self::Error> {
        match entry_type {
            0 => Ok(Self::Deletion),
            1 => Ok(Self::Value),
            _ => Err(()),
        }
    }
}

/// A `LengthPrefixedBytes` value is a reference to a byte slice formed from the concatenation of:
/// - `data_len`, a varint32 used as a length prefix,
/// - `data`, a byte slice of the length indicated by the varint32.
///
/// Values are verified on construction, so consumers of `LengthPrefixedBytes` values can
/// assume that they are valid.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct LengthPrefixedBytes<'a>(&'a [u8]);

impl<'a> LengthPrefixedBytes<'a> {
    /// Attempts to parse data from the start of `src` into a `LengthPrefixedBytes` value.
    ///
    /// If possible, a `data_len` varint32 is parsed from the start of `src`. If there are
    /// at least `data_len` bytes in `src` following the varint32, then the first `data_len`
    /// bytes are used to form a `data` slice, and a `LengthPrefixedBytes` value wrapping
    /// the varint32 and `data` slice is returned.
    ///
    /// This may fail if `src` does not begin with a valid varint32, or if `src` is not long enough
    /// to have `data_len` bytes following the parsed `data_len` varint32.
    pub fn parse(src: &'a [u8]) -> Result<Self, ()> {
        // TODO: do not rely on integer_encoding, I don't like how it ignores some errors
        // and necessitates an extra check to see whether what it tells me is true.
        let (bytes_len, varint_len) = u32::decode_var(src).ok_or(())?;

        let bytes_len_usize = usize::try_from(bytes_len).map_err(|_| ())?;
        let output_len = varint_len.checked_add(bytes_len_usize).ok_or(())?;

        if output_len <= src.len() {
            Ok(Self(&src[..output_len]))
        } else {
            Err(())
        }
    }

    /// Get the full slice referenced by this `LengthPrefixedBytes` value, consisting of a
    /// `data_len` varint32 used as a length prefix followed by a `data` slice of length
    /// `data_len`.
    #[inline]
    #[must_use]
    pub fn prefixed_data(&self) -> &[u8] {
        self.0
    }

    /// Get only the `data` slice referenced by this `LengthPrefixedBytes` value, excluding
    /// the `data_len` length prefix.
    #[inline]
    #[must_use]
    pub fn data(&self) -> &[u8] {
        let prefix_len = u32::decode_var(self.0).unwrap().1;
        &self.0[prefix_len..]
    }
}
