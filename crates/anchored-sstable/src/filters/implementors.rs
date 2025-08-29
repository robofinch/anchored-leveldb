use std::iter;
use std::marker::PhantomData;

use clone_behavior::{IndependentClone, MirroredClone, Speed};
use generic_container::FragileContainer;

use crate::internal_utils::U32_BYTES;
use super::FilterPolicy;


pub trait BloomPolicyName {
    const NAME: &'static [u8];
}

/// LevelDB-compatible hash function for Bloom filters.
///
/// Requires that `data.len()` is at most `u32::MAX`.
fn bloom_hash(data: &[u8]) -> u32 {
    let seed:       u32 = 0x_bc9f1d34;
    let multiplier: u32 = 0x_c6a4a793;

    #[expect(
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        reason = "caller asserts this doesn't overflow",
    )]
    let mut hash: u32 = seed ^ (data.len() as u32).wrapping_mul(multiplier);

    let mut data_iter = data.chunks_exact(U32_BYTES);

    for chunk in &mut data_iter {
        #[expect(clippy::unwrap_used, reason = "the chunk size means that this always succeeds")]
        let word = u32::from_le_bytes(chunk.try_into().unwrap());

        hash = hash.wrapping_add(word).wrapping_mul(multiplier);
        hash ^= hash >> 16_u8;
    }

    if !data_iter.remainder().is_empty() {
        for (idx, &byte) in data_iter.remainder().iter().enumerate() {
            hash = hash.wrapping_add(u32::from(byte) << (8 * idx));
        }

        hash = hash.wrapping_mul(multiplier);
        // This is not a typo; 24, not 16. I don't know the exact motivation,
        // but the LevelDB hash function for bloom filters does this.
        hash ^= hash >> 24_u8;
    }

    hash
}

/// ## 16-bit Architectures
/// This policy may experience overflows and logical errors on 16-bit architectures, so it
/// should not be used (if it's even possible to compile to such a target, or avoid OOM errors).
#[derive(Debug)]
pub struct BloomPolicy<Name> {
    bits_per_key:       u8,
    /// This Bloom filter parameter is also known as `k`.
    num_hash_functions: u8,
    _name:              PhantomData<Name>,
}

impl<Name> BloomPolicy<Name> {
    /// The number of filter bits to use per key. The default `BloomPolicy` filter uses 10 bits per
    /// key to get a false positive rate just under 1%.
    ///
    /// The filter is clamped to having at most around 43 bits per key, with a resulting false
    /// positive rate just under 0.0000001% (1e-7 percent).
    ///
    /// See <https://en.wikipedia.org/wiki/Bloom_filter#:~:text=9.6%20bits%20per%20element>.
    #[must_use]
    pub fn new(bits_per_key: u8) -> Self {
        // See https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
        // `bits_per_key` is m/n, so we need to multiply that by the natural log of 2.

        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::float_arithmetic,
            reason = "lossy operations are fine, we just want a reasonably close-ish value",
        )]
        let num_hash_functions = (f32::from(bits_per_key) * f32::ln(2.)) as u8;

        // Clamp it to reasonable values
        if num_hash_functions < 1 {
            Self {
                bits_per_key,
                num_hash_functions: 1,
                _name:              PhantomData,
            }
        } else if num_hash_functions > 30 {
            // 30 / ln(2) is (rounded) 43.3
            // (1 - e^( -ln(2) ))^30 is around 9.3e-10, which is just under 1e-7 percent
            // as stated in above documentation.
            // Note also that LevelDB reserves values greater than 30 for any future Bloom policy
            // formats.
            Self {
                bits_per_key:       43,
                num_hash_functions: 30,
                _name:              PhantomData,
            }
        } else {
            Self {
                bits_per_key,
                num_hash_functions,
                _name:              PhantomData,
            }
        }
    }
}

impl<Name: BloomPolicyName> FilterPolicy for BloomPolicy<Name> {
    #[inline]
    fn name(&self) -> &'static [u8] {
        Name::NAME
    }

    /// Extends the `filter` buffer with a filter corresponding to the provided flattened keys.
    ///
    /// Each element of `key_offsets` must be the index of the start of a key in `flattened_keys`.
    /// It is assumed that `flattened_keys.len() <= 1 << 20` and `key_offsets.len() <= 1 << 20`
    /// (note that `1 << 20` is [`FILTER_KEYS_LENGTH_LIMIT`]).
    ///
    /// The `filter` buffer is only extended; existing contents are not touched.
    ///
    /// ## 16-bit Architectures
    /// This function may experience overflows and logical errors on 16-bit architectures, so it
    /// should not be used (if it's even possible to compile to such a target, or avoid OOM errors).
    ///
    /// [`FILTER_KEYS_LENGTH_LIMIT`]: super::FILTER_KEYS_LENGTH_LIMIT
    fn create_filter(&self, flattened_keys: &[u8], key_offsets: &[usize], filter: &mut Vec<u8>) {
        // Note: as per the documentation of this policy, it is assumed that `usize` is at least
        // 32 bits.
        // Checking that stuff doesn't overflow:
        // We know `usize` is at least 32 bits.
        // Suppose that both buffers are 2^20 in length (the worst case).
        // Then, since the max bits per key is 43 (say 44-45 in case float ops are weird),
        // `self.bits_per_key` is definitely less than 64, so
        // the `unadjusted_num_filter_bits` product is at most 2^20 * 64 = 2^26.
        // Then, `num_filter_bits` would attempt to be set to at most 2^26 + 1.
        // So, we're good.

        let unadjusted_num_filter_bits = key_offsets.len() * usize::from(self.bits_per_key);

        // Enforce a minimum of 64 filter bits.
        let num_filter_bytes = if unadjusted_num_filter_bits < 64 {
            8
        } else {
            unadjusted_num_filter_bits.div_ceil(8)
        };

        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "see comment at top; this can't overflow",
        )]
        let num_filter_bits = (num_filter_bytes * 8) as u32;

        // ================================
        //  Add space in the filter buffer
        // ================================
        let old_filter_len = filter.len();

        filter.reserve(num_filter_bytes + 1);
        filter.extend(iter::repeat_n(0, num_filter_bytes));
        // Used by `key_may_match`.
        filter.push(self.num_hash_functions);

        #[expect(clippy::indexing_slicing, reason = "we extended the buf, this is in-bounds")]
        let filter_bits = &mut filter[old_filter_len..old_filter_len + num_filter_bytes];

        // ================================
        //  Set filter bits
        // ================================
        let mut key_offsets_iter = key_offsets.iter().peekable();

        while let Some(&key_offset) = key_offsets_iter.next() {
            let upper_bound = **key_offsets_iter
                .peek()
                .unwrap_or(&&key_offsets.len());

            #[expect(
                clippy::indexing_slicing,
                reason = "for valid `key_offsets`, we know \
                          `key_offset <= upper_bound <= flattened_keys.len()`",
            )]
            let key = &flattened_keys[key_offset..upper_bound];

            let mut hash = bloom_hash(key);
            let delta = hash.rotate_right(17);
            for _ in 0..self.num_hash_functions {
                #[expect(
                    clippy::as_conversions,
                    reason = "we assume that `usize` is at least 32 bits",
                )]
                let bit_to_set = (hash % num_filter_bits) as usize;

                #[expect(
                    clippy::indexing_slicing,
                    clippy::integer_division,
                    reason = "bit_to_set / 8 < num_filter_bits / 8 == filter_bits.len()",
                )]
                {
                    filter_bits[bit_to_set / 8] |= 1 << (bit_to_set & 8);
                };
                hash = hash.wrapping_add(delta);
            }
        }
    }

    /// Return `true` if the `key` may have been among the keys for which the `filter` was
    /// generated.
    ///
    /// False positives may occur, but never a false negative.
    ///
    /// ## 16-bit Architectures
    /// This function may experience overflows and logical errors on 16-bit architectures, so it
    /// should not be used (if it's even possible to compile to such a target, or avoid OOM errors).
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        if filter.len() < 2 {
            // The filter is too short to have any key-related data; there were no keys.
            return false;
        }

        #[expect(clippy::unwrap_used, reason = "we checked that the filter is nonempty")]
        let num_hash_functions = *filter.last().unwrap();

        if num_hash_functions > 30 {
            // This is not currently supported. It might be a Bloom policy format we don't know.
            // Default to returning true.
            return true;
        }

        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "see `self.create_filter`; num_filter_bits < 1 << 27",
        )]
        let num_filter_bits = (filter.len() - 1) as u32 * 8;

        let mut hash = bloom_hash(key);
        let delta = hash.rotate_right(17);

        for _ in 0..num_hash_functions {
            #[expect(
                clippy::as_conversions,
                reason = "we assume that `usize` is at least 32 bits",
            )]
            let bit_to_test = (hash % num_filter_bits) as usize;

            #[expect(
                clippy::indexing_slicing,
                clippy::integer_division,
                reason = "bit_to_test / 8 < num_filter_bits / 8 == filter_bits.len() - 1",
            )]
            if filter[bit_to_test / 8] & (1 << (bit_to_test % 8)) == 0 {
                // A bit associated with `key` was not set, so it can't possibly have been
                // in the original list of keys.
                return false;
            }
            hash = hash.wrapping_add(delta);
        }

        // This may be a false positive
        true
    }
}

impl<Name> Clone for BloomPolicy<Name> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<Name> Copy for BloomPolicy<Name> {}

impl<Name, S: Speed> MirroredClone<S> for BloomPolicy<Name> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<Name, S: Speed> IndependentClone<S> for BloomPolicy<Name> {
    #[inline]
    fn independent_clone(&self) -> Self {
        *self
    }
}

impl<Name> Default for BloomPolicy<Name> {
    /// The default `BloomPolicy` filter uses 10 bits per key to get an error rate just under 1%.
    ///
    /// See <https://en.wikipedia.org/wiki/Bloom_filter#:~:text=9.6%20bits%20per%20element>.
    fn default() -> Self {
        Self::new(10)
    }
}

/// An uninhabited type which implements [`FilterPolicy`].
///
/// In particular, `Option<NoFilterPolicy>` is a zero-sized type that can take the place of a
/// generic type similar to `Option<impl FilterPolicy>`.
#[derive(Debug, Clone, Copy)]
pub enum NoFilterPolicy {}

#[expect(clippy::uninhabited_references, reason = "this filter policy can never be constructed")]
impl FilterPolicy for NoFilterPolicy {
    fn name(&self) -> &'static [u8] {
        match *self {}
    }

    fn create_filter(&self, _: &[u8], _: &[usize], _: &mut Vec<u8>) {
        match *self {}
    }

    fn key_may_match(&self, _: &[u8], _: &[u8]) -> bool {
        match *self {}
    }
}

#[expect(clippy::uninhabited_references, reason = "this filter policy can never be constructed")]
impl<S: Speed> MirroredClone<S> for NoFilterPolicy {
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

#[expect(clippy::uninhabited_references, reason = "this filter policy can never be constructed")]
impl<S: Speed> IndependentClone<S> for NoFilterPolicy {
    fn independent_clone(&self) -> Self {
        *self
    }
}

impl<C: FragileContainer<dyn FilterPolicy>> FilterPolicy for C {
    fn name(&self) -> &'static [u8] {
        let inner: &dyn FilterPolicy = &*self.get_ref();
        inner.name()
    }

    fn create_filter(&self, flattened_keys: &[u8], key_offsets: &[usize], filter: &mut Vec<u8>) {
        let inner: &dyn FilterPolicy = &*self.get_ref();
        inner.create_filter(flattened_keys, key_offsets, filter);
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        let inner: &dyn FilterPolicy = &*self.get_ref();
        inner.key_may_match(key, filter)
    }
}
