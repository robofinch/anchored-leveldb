use std::num::NonZeroU16;

use crate::binary_block_log::BINARY_LOG_HEADER_SIZE;


/// The size of blocks in the binary log format used by `MANIFEST-_` manifest files and `_.log`
/// write-ahead log files.
///
/// It is required to be at least `8` and at most `65,536 - 8`.
///
/// The default value is `1 << 15` (the largest power of two meeting these conditions).
///
/// Note that *every* reader and writer of a given LevelDB database need to use **the exact same
/// value** for this block size, and all LevelDB databases produced by Google's LevelDB library
/// use `1 << 15`. Mojang's LevelDB fork makes this value configurable, though appears to
/// exclusively use `1 << 15` as well.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct BinaryLogBlockSize(NonZeroU16);

impl BinaryLogBlockSize {
    /// Not made public, because this is a const-hack.
    #[allow(clippy::unwrap_used, reason = "validated at compile time")]
    pub(crate) const DEFAULT: Self = Self::new(1 << 15).unwrap();

    #[inline]
    #[must_use]
    pub const fn new(value: u16) -> Option<Self> {
        #![expect(clippy::missing_panics_doc, reason = "false positive")]

        if BINARY_LOG_HEADER_SIZE < value && value <= u16::MAX - BINARY_LOG_HEADER_SIZE {
            #[expect(clippy::unwrap_used, reason = "`something < value` implies `0 < value`")]
            Some(Self(NonZeroU16::new(value).unwrap()))
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> NonZeroU16 {
        self.0
    }

    /// Internal shorthand.
    #[inline]
    #[must_use]
    pub(crate) const fn as_u16(self) -> u16 {
        self.0.get()
    }

    /// Internal shorthand.
    #[expect(clippy::as_conversions, reason = "const-hack")]
    #[inline]
    #[must_use]
    pub(crate) const fn as_u64(self) -> u64 {
        self.0.get() as u64
    }

    /// Internal shorthand.
    #[expect(clippy::as_conversions, reason = "const-hack")]
    #[inline]
    #[must_use]
    pub(crate) const fn as_usize(self) -> usize {
        self.0.get() as usize
    }
}

impl Default for BinaryLogBlockSize {
    #[inline]
    fn default() -> Self {
        Self::DEFAULT
    }
}
