use clone_behavior::{DeepClone, MirroredClone, Speed};
use snap::raw as snap_raw;
use snap::raw::{Decoder as SnapDecoder, Encoder as SnapEncoder};

use super::{Compressor, CompressorID, CompressionError, DecompressionError, SNAPPY_COMPRESSION};


/// Uses [`snap`] to provide support for Snappy compression and decompression.
#[cfg_attr(docsrs, doc(cfg(feature = "snappy-compressor")))]
#[derive(Default, Debug, Clone, Copy)]
pub struct SnappyCompressor;

impl<S: Speed> MirroredClone<S> for SnappyCompressor {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self
    }
}

impl<S: Speed> DeepClone<S> for SnappyCompressor {
    #[inline]
    fn deep_clone(&self) -> Self {
        Self
    }
}

impl CompressorID for SnappyCompressor {
    const ID: u8 = SNAPPY_COMPRESSION;
}

impl Compressor for SnappyCompressor {
    fn encode_into(
        &self,
        source:     &[u8],
        output_buf: &mut Vec<u8>,
    ) -> Result<(), CompressionError> {
        output_buf.resize(snap_raw::max_compress_len(source.len()), 0);

        SnapEncoder::new()
            .compress(source, output_buf)
            .map(|_| ())
            .map_err(CompressionError::from_display)
    }

    fn decode_into(
        &self,
        source:     &[u8],
        output_buf: &mut Vec<u8>,
    ) -> Result<(), DecompressionError> {
        let decompress_len = snap_raw::decompress_len(source)
            .map_err(DecompressionError::from_display)?;
        output_buf.resize(decompress_len, 0);

        SnapDecoder::new()
            .decompress(source, output_buf)
            .map(|_| ())
            .map_err(DecompressionError::from_display)
    }
}
