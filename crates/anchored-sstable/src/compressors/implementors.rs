use clone_behavior::{IndependentClone, MirroredClone, Speed};

use super::{Compressor, CompressorID, CompressionError, DecompressionError};


/// Performs no compression.
///
/// Note that LevelDB implementations may special-case no compression and not bother with calling
/// this [`Compressor`] implementation.
#[derive(Default, Debug, Clone, Copy)]
pub struct NoneCompressor;

impl<S: Speed> MirroredClone<S> for NoneCompressor {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self
    }
}

impl<S: Speed> IndependentClone<S> for NoneCompressor {
    #[inline]
    fn independent_clone(&self) -> Self {
        Self
    }
}

impl CompressorID for NoneCompressor {
    const ID: u8 = 0;
}

impl Compressor for NoneCompressor {
    #[inline]
    fn encode_into(
        &self,
        source:     &[u8],
        output_buf: &mut Vec<u8>,
    ) -> Result<(), CompressionError> {
        output_buf.extend(source);
        Ok(())
    }

    #[inline]
    fn decode_into(
        &self,
        source:     &[u8],
        output_buf: &mut Vec<u8>,
    ) -> Result<(), DecompressionError> {
        output_buf.extend(source);
        Ok(())
    }
}
