use std::cmp::Ordering;

use clone_behavior::{IndependentClone, MirroredClone, Speed};
use generic_container::{FragileContainer, GenericContainer};
use seekable_iterator::Comparator;

use crate::internal_utils::common_prefix_len;
use super::TableComparator;


#[derive(Default, Debug, Clone, Copy)]
pub struct LexicographicComparator;

impl TableComparator for LexicographicComparator {
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
    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        // Length of the prefix of bytes that `from` and `to` have in common.
        let common_len = common_prefix_len(from, to);

        // Note: we immediately know `common_len` is strictly less than the length of `to`.
        // If it were equal, then `to` would be a prefix of `from`, and thus compare
        // less than or equal to `from`.
        // Additionally, if `from[common_len]` exists,
        // we have `from[common_len] < to[common_len] <= 255`.

        // There are six cases we worry about. Where C stands for "common",
        // I and X are bytes strictly less than 255 (or 254, based on context),
        // J is I + 1, Y is X + 1, K is a byte strictly greater than I+1,
        // M is 255, A is any byte value,
        // and * denotes repetition of the previous letter 0 or more times, the cases are:

        //           1     2     3       4        5       6
        // from:     C*  | C*I | C*IA* | C*IA*  | C*IM* | C*IM*XA*
        // to:       C*A | C*J | C*KA* | C*JAA* | C*J   | C*J
        // solution: C*  | C*I | C*J   | C*J    | C*IM* | C*IM*Y

        if let Some(i_byte) = from.get(common_len) {
            // See above for why this cannot overflow.
            let j_byte = i_byte + 1;

            #[expect(clippy::indexing_slicing, reason = "see above, we know this ele exists")]
            let j_or_k_byte = to[common_len];

            if j_byte < j_or_k_byte || common_len + 1 < to.len() {
                // This is either case 3 or case 4. Return C*J
                #[expect(
                    clippy::indexing_slicing,
                    reason = "common_len is at most the shorter len of the two slices",
                )]
                separator.extend(&from[..common_len]);
                separator.push(j_byte);
                return;

            } else if common_len + 1 == from.len().min(to.len()) {
                // This is case 2. Fall through to returning `from` itself.

            } else {
                // This is either case 5 or 6.
                #[expect(
                    clippy::indexing_slicing,
                    reason = "common_len is at most the shorter len of the two slices",
                )]
                let maybe_x_offset = from[common_len..].iter()
                    .take_while(|&&byte| byte == u8::MAX)
                    .count();

                // Note: this is at most `from.len()`.
                let maybe_x_index = common_len + maybe_x_offset;

                // This extends `separator` by C*IM*, which we want to do in both cases 5 and 6.
                #[expect(clippy::indexing_slicing, reason = "`maybe_x_index <= from.len()`")]
                separator.extend(&from[..maybe_x_index]);

                if let Some(&x_byte) = from.get(maybe_x_index) {
                    // This is case 6. Push X + 1 to get C*IM*Y
                    separator.push(x_byte + 1);
                } else {
                    // This is case 5. We already extended `separator` by C*IM*
                }

                return;
            }
        } else {
            // This is case 1. Fall through to returning `from` itself.
        }

        separator.extend(from);
    }

    /// In O(n) time, find the shortest byte slice which compares greater than or equal to `key`.
    ///
    /// The output slice is written to `successor`.
    ///
    /// It is assumed that the passed `successor` is an empty `Vec`; callers must
    /// uphold this assumption.
    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        // Using the same notation as above, plus 0 being the zero byte, the two cases are:
        // key:      M*XA* | M*
        // solution: M*Y   | M*0

        let first_non_max_byte_idx = key.iter()
            .take_while(|&&byte| byte == u8::MAX)
            .count();

        // In both cases 1 and 2, we extend by M*
        #[expect(clippy::indexing_slicing, reason = "iter count is at most `key.len()`")]
        successor.extend(&key[..first_non_max_byte_idx]);

        if let Some(&non_max_byte) = key.get(first_non_max_byte_idx) {
            // Case 1
            successor.push(non_max_byte + 1);
        } else {
            // Case 2
            successor.push(0);
        }
    }
}

impl<S: Speed> MirroredClone<S> for LexicographicComparator {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<S: Speed> IndependentClone<S> for LexicographicComparator {
    #[inline]
    fn independent_clone(&self) -> Self {
        *self
    }
}

/// Regardless of the comparator settings of a [`Table`], its metaindex block always uses
/// this default lexicographic comparator.
///
/// This comparator must be used for the metaindex block, and must not be used for any other block.
///
/// [`Table`]: crate::table::Table
#[derive(Default, Debug, Clone, Copy)]
pub struct MetaindexComparator;

impl TableComparator for MetaindexComparator {
    #[inline]
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        LexicographicComparator.cmp(lhs, rhs)
    }

    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        LexicographicComparator.find_short_separator(from, to, separator);
    }

    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        LexicographicComparator.find_short_successor(key, successor);
    }
}

impl<S: Speed> MirroredClone<S> for MetaindexComparator {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<S: Speed> IndependentClone<S> for MetaindexComparator {
    #[inline]
    fn independent_clone(&self) -> Self {
        *self
    }
}

impl<C: FragileContainer<dyn TableComparator>> TableComparator for C {
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        let comparator: &dyn TableComparator = &*self.get_ref();
        comparator.cmp(lhs, rhs)
    }

    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        let comparator: &dyn TableComparator = &*self.get_ref();
        comparator.find_short_separator(from, to, separator);
    }

    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        let comparator: &dyn TableComparator = &*self.get_ref();
        comparator.find_short_successor(key, successor);
    }
}

impl<Cmp, C> TableComparator for GenericContainer<Cmp, C>
where
    Cmp: TableComparator,
    C:   FragileContainer<Cmp>,
{
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        let cmp: &Cmp = &self.container.get_ref();
        cmp.cmp(lhs, rhs)
    }

    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        let cmp: &Cmp = &self.container.get_ref();
        cmp.find_short_separator(from, to, separator);
    }

    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        let cmp: &Cmp = &self.container.get_ref();
        cmp.find_short_successor(key, successor);
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct ComparatorAdapter<Cmp>(pub Cmp);

impl<Cmp: TableComparator> Comparator<[u8]> for ComparatorAdapter<Cmp> {
    #[inline]
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        self.0.cmp(lhs, rhs)
    }
}

impl<S: Speed, Cmp: MirroredClone<S>> MirroredClone<S> for ComparatorAdapter<Cmp> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}

impl<S: Speed, Cmp: IndependentClone<S>> IndependentClone<S> for ComparatorAdapter<Cmp> {
    #[inline]
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}
