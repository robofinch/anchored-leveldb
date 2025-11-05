use super::{Compressor, CompressorID, NoneCompressor};
#[cfg(feature = "snappy-compressor")]
use super::SnappyCompressor;
#[cfg(feature = "zstd-compressor")]
use super::ZstdCompressor;


/// A list of [`Compressor`] implementations, associated with IDs.
///
/// See [`Compressor`] and [`CompressorID`] for more.
#[derive(Debug)]
pub struct CompressorList(Vec<(u8, Box<dyn Compressor>)>);

impl CompressorList {
    /// Get a `CompressorList` with only the [`NoneCompressor`] at ID 0.
    ///
    /// Note that associating ID 0 with no compression is hardcoded, and need not actually dispatch
    /// to [`NoneCompressor`].
    #[inline]
    #[must_use]
    pub fn new_without_compressors() -> Self {
        Self(vec![(NoneCompressor::ID, Box::new(NoneCompressor))])
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
    /// This overwrites any previously-set compressor with the same `id`, _unless_ the `id`
    /// is zero; the association between ID 0 and no compression is hardcoded, so the
    /// [`NoneCompressor`] cannot be replaced.
    ///
    /// Returns false if a previously-set compressor had the same `id`, and returns true otherwise.
    #[inline]
    pub fn add<C>(&mut self, compressor: C) -> bool
    where
        C: Compressor + CompressorID + 'static,
    {
        self.set_with_id(C::ID, compressor)
    }

    /// Set the given `id` to refer to the provided `compressor`.
    ///
    /// This overwrites any previously-set compressor with the same `id`, _unless_ the `id`
    /// is zero; the association between ID 0 and no compression is hardcoded, so the
    /// [`NoneCompressor`] cannot be replaced.
    ///
    /// Returns false if a previously-set compressor had the same `id`, and returns true otherwise.
    #[inline]
    pub fn set_with_id<C>(&mut self, id: u8, compressor: C) -> bool
    where
        C: Compressor + 'static,
    {
        match self.0.binary_search_by_key(&id, |(existing_id, _)| *existing_id) {
            Ok(existing_idx) => {
                #[expect(clippy::indexing_slicing, reason = "index came from successful search")]
                {
                    self.0[existing_idx].1 = Box::new(compressor);
                };
                false
            }
            Err(idx_to_insert_at) => {
                self.0.insert(idx_to_insert_at, (id, Box::new(compressor)));
                true
            }
        }
    }

    /// Check whether the given ID refers to any compressor.
    #[inline]
    #[must_use]
    pub fn is_set(&self, id: u8) -> bool {
        self.0.binary_search_by_key(&id, |(existing_id, _)| *existing_id).is_ok()
    }

    /// Get the compressor referred to by `id`, if `id` was set.
    #[inline]
    #[must_use]
    pub fn get(&self, id: u8) -> Option<&dyn Compressor> {
        #[expect(clippy::indexing_slicing, reason = "index came from successful search")]
        self.0.binary_search_by_key(&id, |(existing_id, _)| *existing_id)
            .ok()
            .map(|idx| &*self.0[idx].1)
    }
}
