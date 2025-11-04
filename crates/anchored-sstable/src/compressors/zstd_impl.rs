use std::io::{Read as _, Write as _};

use clone_behavior::{DeepClone, MirroredClone, Speed};
use zstd::{Encoder as ZStdEncoder, Decoder as ZStdDecoder};

use super::{Compressor, CompressorID, CompressionError, DecompressionError, ZSTD_COMPRESSION};


/// Uses [`zstd`] to provide support for Zstandard compression and decompression.
#[cfg_attr(docsrs, doc(cfg(feature = "zstd-compressor")))]
#[derive(Debug, Clone, Copy)]
pub struct ZstdCompressor {
    // Perhaps should be an i32, technically. Please open an issue if you need a negative
    // compression level or something else that doesn't fit in a u8.
    pub compression_level: u8,
}

impl Default for ZstdCompressor {
    fn default() -> Self {
        // This is `zstd::DEFAULT_COMPRESSION_LEVEL`.
        Self {
            compression_level: 3,
        }
    }
}

impl<S: Speed> MirroredClone<S> for ZstdCompressor {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<S: Speed> DeepClone<S> for ZstdCompressor {
    #[inline]
    fn deep_clone(&self) -> Self {
        *self
    }
}

impl CompressorID for ZstdCompressor {
    const ID: u8 = ZSTD_COMPRESSION;
}

impl Compressor for ZstdCompressor {
    fn encode_into(
        &self,
        source:     &[u8],
        output_buf: &mut Vec<u8>,
    ) -> Result<(), CompressionError> {
        let mut encoder = ZStdEncoder::new(output_buf, i32::from(self.compression_level))
            .map_err(CompressionError::from_display)?;

        encoder.write_all(source)
            .map_err(CompressionError::from_display)?;
        encoder.finish().map_err(CompressionError::from_display)?;

        Ok(())
    }

    fn decode_into(
        &self,
        source:     &[u8],
        output_buf: &mut Vec<u8>,
    ) -> Result<(), DecompressionError> {
        let mut decoder = ZStdDecoder::with_buffer(source)
            .map_err(DecompressionError::from_display)?;

        decoder.read_to_end(output_buf).map_err(DecompressionError::from_display)?;

        Ok(())
    }
}
