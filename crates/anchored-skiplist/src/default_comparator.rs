use std::cmp::Ordering;

use crate::interface::Comparator;


/// A [`Comparator`] which uses the [`Ord`] implementation of `[u8]`.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DefaultComparator;

impl Comparator for DefaultComparator {
    /// Equivalent to `Ord::cmp(lhs, rhs)`.
    #[inline]
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        Ord::cmp(lhs, rhs)
    }
}
