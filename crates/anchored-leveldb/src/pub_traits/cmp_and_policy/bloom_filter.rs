use std::{error::Error, num::NonZeroU32};
use std::fmt::{Display, Formatter, Result as FmtResult};

use clone_behavior::{DeepClone, MirroredClone, Speed};

use super::{bytewise_implementors::BytewiseEquality, traits::FilterPolicy};


/// A LevelDB-compatible [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter).
#[derive(Debug, Clone, Copy)]
pub struct BloomPolicy {
    bits_per_key:       u8,
    /// This Bloom filter parameter is also known as `k`.
    num_hash_functions: u8,
}

impl BloomPolicy {
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
            }
        } else if num_hash_functions > 30 {
            // `30 / ln(2)` is (rounded) `43.3`
            // `(1 - e^( -ln(2) ))^30` is around `9.3e-10`, which is just under `1e-7` percent
            // as stated in above documentation.
            // Note also that LevelDB reserves values greater than 30 for any future Bloom policy
            // formats.
            Self {
                bits_per_key:       43,
                num_hash_functions: 30,
            }
        } else {
            Self {
                bits_per_key,
                num_hash_functions,
            }
        }
    }
}

impl FilterPolicy for BloomPolicy {
    type Eq          = BytewiseEquality;
    type FilterError = BloomPolicyOverflow;

    #[inline]
    fn name(&self) -> &'static [u8] {
        b"leveldb.BuiltinBloomFilter2"
    }

    fn create_filter(
        &self,
        flattened_keys: &[u8],
        key_offsets:    &[usize],
        filter:         &mut Vec<u8>,
    ) -> Result<(), Self::FilterError> {
        /// Return the number of bits and bytes (respectively) to use in the filter.
        ///
        /// Returns `None` if any overflow occurs.
        fn num_filter_bits_and_bytes(
            key_offsets_len: usize,
            bits_per_key:    u8,
        ) -> Option<(NonZeroU32, usize)> {
            // `key_offsets_len * bits_per_key`
            let unadjusted_num_filter_bits = key_offsets_len
                .checked_mul(usize::from(bits_per_key))?;

            // Enforce a minimum of 64 filter bits.
            let num_filter_bytes: usize = if unadjusted_num_filter_bits < 64 {
                8
            } else {
                unadjusted_num_filter_bits.div_ceil(8)
            };

            // `num_filter_bytes * 8`
            let num_filter_bits = u32::try_from(num_filter_bytes.checked_mul(8)?).ok()?;
            #[expect(clippy::unwrap_used, reason = "we always have at least 64 filter bits")]
            let num_filter_bits = NonZeroU32::new(num_filter_bits).unwrap();

            Some((num_filter_bits, num_filter_bytes))
        }

        let (num_filter_bits, num_filter_bytes) = num_filter_bits_and_bytes(
            key_offsets.len(),
            self.bits_per_key,
        ).ok_or(BloomPolicyOverflow::TooManyKeys)?;
        // Note that `num_filter_bits` is converted from a `usize` via `u32::try_from`,
        // so if the above function successfully returns, we know that `num_filter_bits`
        // fits in a usize.

        // ================================
        //  Add space in the filter buffer
        // ================================
        let old_filter_len = filter.len();

        let num_filter_bytes_plus_one = num_filter_bytes
            .checked_add(1)
            .ok_or(BloomPolicyOverflow::TooManyKeys)?;

        filter.try_reserve(num_filter_bytes_plus_one)
            // Any error here would be caused by a large value of `key_offsets.len()`.
            .map_err(|_ignore| BloomPolicyOverflow::TooManyKeys)?;

        // We're careful to not truncate the vec. Note that if the above call succeeded,
        // we successfully got a vector of capacity at least `filter.len() + num_filter_bytes + 1`,
        // so `filter.len() + num_filter_bytes` does not overflow.
        filter.resize(old_filter_len + num_filter_bytes, 0);
        // Used by `key_may_match`. Note that we reserved one extra byte for this field.
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
                .unwrap_or(&&flattened_keys.len());

            #[expect(
                clippy::indexing_slicing,
                reason = "for valid `key_offsets`, we know \
                          `key_offset <= upper_bound <= flattened_key_data.len()`",
            )]
            let key = &flattened_keys[key_offset..upper_bound];

            let mut hash = bloom_hash(key).ok_or(BloomPolicyOverflow::KeyTooLarge)?;
            let delta = hash.rotate_right(17);
            for _ in 0..self.num_hash_functions {
                #[expect(
                    clippy::as_conversions,
                    reason = "we know that `num_filter_bits` fits in a `usize`",
                )]
                let bit_to_set = (hash % num_filter_bits) as usize;

                // `bit_to_set < num_filter_bits` and `num_filter_bits` is a multiple of 8.
                // Mathematically, `bit_to_set / 8 < num_filter_bits / 8`, and since
                // `num_filter_bits / 8` is an integer, taking the floor of each side
                // preserves the strict inequality.
                // As for correctness, this sets bit
                // `(bit_to_set / 8) * 8 + (bit_to_set % 8) = bit_to_set`.
                #[expect(
                    clippy::indexing_slicing,
                    clippy::integer_division,
                    reason = "bit_to_set / 8 < num_filter_bits / 8 == filter_bits.len()",
                )]
                {
                    filter_bits[bit_to_set / 8] |= 1 << (bit_to_set % 8);
                };
                hash = hash.wrapping_add(delta);
            }
        }

        Ok(())
    }

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

        let Ok(num_filter_bytes) = u32::try_from(filter.len() - 1) else {
            // Overflow error. This is an invalid Bloom filter. Default to returning true.
            return true;
        };
        let Some(num_filter_bits) = num_filter_bytes.checked_mul(8) else {
            // Same as above.
            return true;
        };
        if usize::try_from(num_filter_bits).is_err() {
            // Same as above.
            return true;
        }

        let Some(mut hash) = bloom_hash(key) else {
            // This key is so large that the filter, if successfully generated by *this* policy,
            // could not possibly contain the given key (since this key being added would trigger
            // an error). So, perhaps we *could* return `false`. However, it feels better to flush
            // weird edge cases to `true`, just in case some other LevelDB implementation would
            // be able to generate a filter containing this key.
            // Either way, this branch and this false positive probably won't ever happen at all.
            return true;
        };
        let delta = hash.rotate_right(17);

        for _ in 0..num_hash_functions {
            #[expect(
                clippy::as_conversions,
                reason = "we checked that `num_filter_bits` fits in a `usize`",
            )]
            let bit_to_test = (hash % num_filter_bits) as usize;

            // `bit_to_set < num_filter_bits` and `num_filter_bits` is a multiple of 8.
            // Mathematically, `bit_to_set / 8 < num_filter_bits / 8`, and since
            // `num_filter_bits / 8` is an integer, taking the floor of each side preserves the
            // strict inequality.
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

impl Default for BloomPolicy {
    /// The default `BloomPolicy` filter uses 10 bits per key to get an error rate just under 1%.
    ///
    /// See <https://en.wikipedia.org/wiki/Bloom_filter#:~:text=9.6%20bits%20per%20element>.
    fn default() -> Self {
        Self::new(10)
    }
}

impl<S: Speed> MirroredClone<S> for BloomPolicy {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<S: Speed> DeepClone<S> for BloomPolicy {
    #[inline]
    fn deep_clone(&self) -> Self {
        *self
    }
}


/// The error returned if a filter could not be generated, which should usually only occur in extreme conditions.
#[derive(Debug, Clone, Copy)]
pub enum BloomPolicyOverflow {
    /// Returned due to a variety of possible overflows that can happen if a filter is generated
    /// for an excessively large number of keys.
    ///
    /// (Aside from 16-bit systems -- which presumably cannot use `anchored-leveldb` anyway --
    /// the limit is in the hundreds of millions of keys.)
    TooManyKeys,
    /// Returned if any key's length exceeds [`u32::MAX`].
    KeyTooLarge,
}

impl Display for BloomPolicyOverflow {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(match self {
            Self::TooManyKeys =>
                "could not generate a Bloom filter on an excessively large key set",
            Self::KeyTooLarge =>
                "could not generate a Bloom filter on a key set containing a 4 GiB key",
        })
    }
}

impl Error for BloomPolicyOverflow {}

impl<S: Speed> MirroredClone<S> for BloomPolicyOverflow {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<S: Speed> DeepClone<S> for BloomPolicyOverflow {
    #[inline]
    fn deep_clone(&self) -> Self {
        *self
    }
}

/// LevelDB-compatible hash function for Bloom filters.
///
/// Requires `None` if `data.len()` does not fit in a `u32`.
#[must_use]
fn bloom_hash(data: &[u8]) -> Option<u32> {
    let seed:       u32 = 0x_bc9f1d34;
    let multiplier: u32 = 0x_c6a4a793;

    let data_len_u32 = u32::try_from(data.len()).ok()?;
    let mut hash: u32 = seed ^ data_len_u32.wrapping_mul(multiplier);

    let mut data_iter = data.chunks_exact(size_of::<u32>());

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
        // but the LevelDB hash function for Bloom filters does this.
        hash ^= hash >> 24_u8;
    }

    Some(hash)
}
