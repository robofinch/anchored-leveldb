mod codec_trait;
// mod codec_list;

// #[cfg(feature = "google-leveldb-compression")]
// mod google_leveldb_codecs;
// #[cfg(feature = "mojang-leveldb-compression")]
// mod mojang_leveldb_codecs;

mod no_compression_impl;
#[cfg(feature = "snappy-compression")]
mod raw_snap_impl;
#[cfg(feature = "zstd-compression")]
mod zstd_impl;
#[cfg(feature = "zlib-compression")]
mod zlib_impl;


pub use self::no_compression_impl::NoCompressionCodec;
pub use self::codec_trait::{CodecCompressionError, CodecDecompressionError, CompressionCodec};

// `SnappyError` is a public reexport from `snap`.
#[cfg(feature = "snappy-compression")]
pub use self::raw_snap_impl::{SnappyCodec, SnappyDecoder, SnappyEncoder, SnappyError};
// `ZlibDeflateError` and `ZlibInflateError` are public reexports from `zlib-rs`.
#[cfg(feature = "zlib-compression")]
pub use self::zlib_impl::{ZlibCodec, ZlibDecoder, ZlibDeflateError, ZlibEncoder, ZlibInflateError};
#[cfg(feature = "zstd-compression")]
pub use self::zstd_impl::{
    ZstdCodec, ZstdCompressionError, ZstdDecoder, ZstdDecompressionError, ZstdEncoder,
    ZstdErrorCode,
};
