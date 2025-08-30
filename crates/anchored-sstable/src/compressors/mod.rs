mod compressor_list;
mod implementors;
#[cfg(feature = "snappy-compressor")]
mod snappy_impl;
#[cfg(feature = "zstd-compressor")]
mod zstd_impl;

use std::error::Error;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};


pub use self::{compressor_list::CompressorList, implementors::NoneCompressor};

#[cfg(feature = "snappy-compressor")]
pub use self::snappy_impl::SnappyCompressor;
#[cfg(feature = "zstd-compressor")]
pub use self::zstd_impl::ZstdCompressor;


pub trait Compressor: Debug {
    /// Write the result of compressing `source` into `output_buf`.
    ///
    /// Implementors may assume that the passed `output_buf` is an empty `Vec`, and callers
    /// must uphold this assumption.
    ///
    /// All clones of `self` must behave identically.
    fn encode_into(
        &self,
        source:     &[u8],
        output_buf: &mut Vec<u8>,
    ) -> Result<(), CompressionError>;

    /// Write the result of decompressing `source` into `output_buf`.
    ///
    /// Implementors may assume that the passed `output_buf` is an empty `Vec`, and callers
    /// must uphold this assumption.
    ///
    /// All clones of `self` must behave identically.
    fn decode_into(
        &self,
        source:     &[u8],
        output_buf: &mut Vec<u8>,
    ) -> Result<(), DecompressionError>;
}


/// Get the ID associated with a compression/decompression format.
/// [`Table`]s use this ID to choose the appropriate decompressor.
///
/// This trait is associated with [`Compressor`], but is kept separate in order to leave
/// [`Compressor`] dyn-compatible.
///
/// Different implementations of the same format need not have distinct ID's. There is no universal
/// designation of what a compressor's ID should be; however, the three ID's used by default
/// LevelDB implementations should generally be respected.
///
/// [`Table`]: crate::table::Table
pub trait CompressorID {
    /// The identifier of a [`Compressor`].
    const ID: u8;
}

/// The compressor ID used by most LevelDB implementations to indicate no compression is used.
pub const NO_COMPRESSION:     u8 = 0;
/// The compressor ID used by most LevelDB implementations to indicate Snappy compression is used.
pub const SNAPPY_COMPRESSION: u8 = 1;
/// The compressor ID used by most LevelDB implementations to indicate ZStd compression is used.
pub const ZSTD_COMPRESSION:   u8 = 2;


#[derive(Debug, Clone)]
pub struct CompressionError {
    pub error_msg: String,
}

impl CompressionError {
    #[must_use]
    pub fn from_display<E: Display>(err: E) -> Self {
        Self {
            error_msg: err.to_string(),
        }
    }
}

impl Display for CompressionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "Error while compressing data: {}", self.error_msg)
    }
}

impl Error for CompressionError {}

#[derive(Debug, Clone)]
pub struct DecompressionError {
    pub error_msg: String,
}

impl DecompressionError {
    #[must_use]
    pub fn from_display<E: Display>(err: E) -> Self {
        Self {
            error_msg: err.to_string(),
        }
    }
}

impl Display for DecompressionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "Error while decompressing data: {}", self.error_msg)
    }
}

impl Error for DecompressionError {}
