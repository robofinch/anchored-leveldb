use std::cmp::Ordering;

use clone_behavior::{IndependentClone, MirroredClone, NearInstant, NonRecursive};

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

impl NonRecursive for DefaultComparator {}

impl IndependentClone<NearInstant> for DefaultComparator {
    #[inline]
    fn independent_clone(&self) -> Self {
        Self
    }
}

impl MirroredClone<NearInstant> for DefaultComparator {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self
    }
}
