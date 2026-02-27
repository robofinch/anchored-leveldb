use std::{cmp::Ordering, convert::Infallible};

use clone_behavior::{DeepClone, MirroredClone, Speed};

use crate::{pub_traits::cmp_and_policy::traits::CoarserThan, utils::common_prefix_len};
use super::traits::{EquivalenceRelation, LevelDBComparator};


/// Denotes the equivalence relation of the [`Eq`] implementation for `[u8]`.
#[derive(Default, Debug, Clone, Copy)]
pub struct BytewiseEquality;

impl EquivalenceRelation for BytewiseEquality {}

impl<S: Speed> MirroredClone<S> for BytewiseEquality {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<S: Speed> DeepClone<S> for BytewiseEquality {
    #[inline]
    fn deep_clone(&self) -> Self {
        *self
    }
}

impl CoarserThan<Self> for BytewiseEquality {}


#[derive(Default, Debug, Clone, Copy)]
pub struct BytewiseComparator;

impl LevelDBComparator for BytewiseComparator {
    type Eq = BytewiseEquality;
    type InvalidKeyError = Infallible;

    #[inline]
    fn name(&self) -> &'static [u8] {
        b"leveldb.BytewiseComparator"
    }

    #[inline]
    fn validate_comparable(&self, _key: &[u8]) -> Result<(), Self::InvalidKeyError> {
        Ok(())
    }

    /// Compare two byte slices by using [`Ord`].
    #[inline]
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }

    /// In O(n) time, find the shortest byte slice which compares greater than or equal to `from`
    /// and strictly less than `to`.
    ///
    /// The output slice is written to `separator`.
    ///
    /// It is assumed that `from` compares strictly less than `to` and that the passed
    /// `separator` is an empty `Vec`; callers must uphold these assumptions.
    ///
    /// The generated `separator` is at most as long as `from`.
    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        // Length of the prefix of bytes that `from` and `to` have in common.
        let common_len = common_prefix_len(from, to);

        // Note: we immediately know `common_len` is strictly less than the length of `to`.
        // If it were equal, then `to` would be a prefix of `from`, and thus compare
        // less than or equal to `from`.
        // Additionally, if `from[common_len]` exists,
        // we have `from[common_len] < to[common_len] <= 255`.
        assert!(
            common_len < to.len(),
            "`BytewiseComparator::find_short_separator` should be passed \
             a `from` strictly less than `to`",
        );

        // There are five disjoint cases we need to consider.
        // Where I and X are bytes strictly less than 255 (or strictly less than 254 in case 5),
        // J is I + 1, Y is X + 1, K is a byte strictly greater than J,
        // M is 255, A stands for any byte value, C stands for byte values in the common prefix,
        // * denotes repetition of the previous letter 0 or more times, and
        // + denotes repetition of the previous letter 1 or more times, the cases are:
        //
        //           1      2       3          4       5
        // from:     C*   | C*IM* | C*IM*XA* | C*IA* | C*IA*
        // to:       C*A+ | C*J   | C*J      | C*JA+ | C*KA*
        // solution: C*   | C*IM* | C*IM*Y   | C*J   | C*J

        if let Some(i_byte) = from.get(common_len) {
            // In this branch, we are not in case 1.

            // See above for why this cannot overflow.
            let j_byte = i_byte + 1;

            #[expect(clippy::indexing_slicing, reason = "see above, we know this ele exists")]
            let j_or_k_byte = to[common_len];

            if j_byte < j_or_k_byte || common_len + 1 < to.len() {
                // If `j_byte < j_or_k_byte`, then `j_or_k_byte` is K and we're in case 5.
                // Otherwise, `j_or_k_byte` is J, and if `common_len + 1 < to.len()`,
                // then `to` has more than just one byte after C*, so we can't be in cases 2 or 3,
                // leaving case 4.
                //
                // This is either case 4 or 5, so the solution is C*J.
                #[expect(
                    clippy::indexing_slicing,
                    reason = "common_len is at most the shorter len of the two slices",
                )]
                separator.extend(&from[..common_len]);
                separator.push(j_byte);

            } else {
                // This is either case 2 or 3.

                #[expect(
                    clippy::indexing_slicing,
                    reason = "common_len is at most the shorter len of the two slices",
                )]
                let maybe_x_offset = from[common_len..].iter()
                    .take_while(|&&byte| byte == u8::MAX)
                    .count();

                // Note: this is at most `from.len()`.
                let maybe_x_index = common_len + maybe_x_offset;

                // This extends `separator` by C*IM*, which we want to do in both cases 2 and 3.
                #[expect(clippy::indexing_slicing, reason = "`maybe_x_index <= from.len()`")]
                separator.extend(&from[..maybe_x_index]);

                if let Some(&x_byte) = from.get(maybe_x_index) {
                    // This is case 3. Push X + 1 to get C*IM*Y.
                    separator.push(x_byte + 1);
                } else {
                    // This is case 2. We already extended `separator` by C*IM*.
                }
            }
        } else {
            // This is case 1, so the solution is `from` itself.
            separator.extend(from);
        }
    }

    /// In O(n) time, find the shortest byte slice which compares greater than or equal to `key`.
    ///
    /// The output slice is written to `successor`.
    ///
    /// It is assumed that the passed `successor` is an empty `Vec`; callers must
    /// uphold this assumption.
    ///
    /// The generated `successor` is at most as long as `key`.
    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        // Using the same notation as above, the two cases are:
        // key:      M*XA* | M*
        // solution: M*Y   | M*

        let first_non_max_byte_idx = key.iter()
            .take_while(|&&byte| byte == u8::MAX)
            .count();

        // In both cases 1 and 2, we extend by M*. The number of `M`s is `first_non_max_byte_idx`.
        successor.resize(first_non_max_byte_idx, u8::MAX);

        if let Some(&non_max_byte) = key.get(first_non_max_byte_idx) {
            // Case 1
            successor.push(non_max_byte + 1);
        } else {
            // Case 2
        }
    }
}

impl<S: Speed> MirroredClone<S> for BytewiseComparator {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<S: Speed> DeepClone<S> for BytewiseComparator {
    #[inline]
    fn deep_clone(&self) -> Self {
        *self
    }
}
