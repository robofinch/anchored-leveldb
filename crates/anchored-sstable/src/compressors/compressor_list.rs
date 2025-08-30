use sorted_vector_map::SortedVectorMap;

use super::{Compressor, CompressorID, NoneCompressor};
#[cfg(feature = "snappy-compressor")]
use super::SnappyCompressor;
#[cfg(feature = "zstd-compressor")]
use super::ZstdCompressor;


/// A list of [`Compressor`] implementations, associated with IDs.
///
/// See [`Compressor`] and [`CompressorID`] for more.
#[derive(Debug)]
pub struct CompressorList(SortedVectorMap<u8, Box<dyn Compressor>>);

impl CompressorList {
    /// Get a `CompressorList` with only the [`NoneCompressor`] at ID 0.
    ///
    /// Note that associating ID 0 with no compression is hardcoded, and need not actually dispatch
    /// to [`NoneCompressor`].
    #[inline]
    #[must_use]
    pub fn new_without_compressors() -> Self {
        let mut compressors = SortedVectorMap::with_capacity(1);
        compressors.insert(NoneCompressor::ID, Box::new(NoneCompressor));
        Self(SortedVectorMap::new())
    }

    /// Get a `CompressorList` with the [`NoneCompressor`] at ID 0, a Snappy compressor at ID 1
    /// (only if the `snappy-compressor` feature is enabled), and a Zstandard compressor at ID 2
    /// (only if the `zstd-compressor` feature is enabled)
    #[inline]
    #[must_use]
    pub fn with_default_compressors() -> Self {
        #[allow(unused_mut, reason = "if neither compressor feature is enabled, it's unused")]
        let mut compressor_list = Self::new_without_compressors();
        #[cfg(feature = "snappy-compressor")]
        compressor_list.add_snappy_compressor();
        #[cfg(feature = "zstd-compressor")]
        compressor_list.add_zstd_compressor();
        compressor_list
    }

    /// Sets ID 1 to the [`SnappyCompressor`].
    #[cfg(feature = "snappy-compressor")]
    #[cfg_attr(docsrs, doc(cfg(feature = "snappy-compressor")))]
    #[inline]
    pub fn add_snappy_compressor(&mut self) {
        self.add(SnappyCompressor);
    }

    /// Sets ID 2 to the [`ZstdCompressor`].
    #[cfg(feature = "zstd-compressor")]
    #[cfg_attr(docsrs, doc(cfg(feature = "zstd-compressor")))]
    #[inline]
    pub fn add_zstd_compressor(&mut self) {
        self.add(ZstdCompressor::default());
    }

    /// Set the provided `compressor`'s ID to refer to that compressor.
    ///
    /// Returns true if this operation did not overwrite a previous compressor,
    /// and false if a previously-set compressor had the same `id`.
    #[inline]
    pub fn add<C>(&mut self, compressor: C) -> bool
    where
        C: Compressor + CompressorID + 'static,
    {
        self.set_with_id(C::ID, compressor)
    }

    /// Set the given `id` to refer to the provided `compressor`.
    ///
    /// Returns true if this operation did not overwrite a previous compressor,
    /// and false if a previously-set compressor had the same `id`.
    #[inline]
    pub fn set_with_id<C>(&mut self, id: u8, compressor: C) -> bool
    where
        C: Compressor + 'static,
    {
        self.0.insert(id, Box::new(compressor)).is_none()
    }

    /// Check whether the given ID refers to any compressor.
    #[inline]
    #[must_use]
    pub fn is_set(&self, id: u8) -> bool {
        self.0.contains_key(&id)
    }

    /// Get the compressor referred to by `id`, if `id` was set.
    #[inline]
    #[must_use]
    pub fn get(&self, id: u8) -> Option<&dyn Compressor> {
        self.0.get(&id).map(|compressor| &**compressor)
    }
}
