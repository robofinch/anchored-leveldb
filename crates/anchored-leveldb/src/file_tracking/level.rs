#![expect(unsafe_code, reason = "index `[T; NUM_LEVELS_USIZE]` without bounds checking")]

use crate::config_constants::{NUM_LEVELS, NUM_LEVELS_USIZE};


/// Safety invariant: the inner value of a [`Level`] is strictly less than [`NUM_LEVELS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct Level(u8);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl Level {
    pub const ZERO: Self = Self(0);
    // SAFETY: `NUM_LEVELS_USIZE` is strictly greater than anything from 0 to 6.
    pub const ALL_LEVELS: [Self; NUM_LEVELS_USIZE] = [
        Self(0), Self(1), Self(2), Self(3), Self(4), Self(5), Self(6),
    ];

    #[inline]
    #[must_use]
    pub const fn inner(self) -> u8 {
        self.0
    }

    #[inline]
    #[must_use]
    pub const fn next_level(self) -> Option<Self> {
        // Note that `self.0 < NUM_LEVELS < u8::MAX`, so this doesn't overflow.
        if self.0 + 1 < NUM_LEVELS {
            // SAFETY: we checked the invariant.
            Some(Self(self.0 + 1))
        } else {
            None
        }
    }

    /// Get all the levels in increasing order, from level 0 to level 6.
    #[inline]
    pub fn all_levels() -> impl ExactSizeIterator<Item = Self> + DoubleEndedIterator {
        // SAFETY: the upper bound of the range is exclusive
        (0..NUM_LEVELS).map(Self)
    }

    /// Get all the nonzero levels in increasing order, from level 1 to level 6.
    #[inline]
    pub fn nonzero_levels() -> impl ExactSizeIterator<Item = Self> + DoubleEndedIterator {
        // SAFETY: the upper bound of the range is exclusive
        (1..NUM_LEVELS).map(Self)
    }

    /// Get all the nonzero levels except for the greatest level in increasing order, from
    /// level 1 to level 4.
    #[inline]
    pub fn middle_levels() -> impl ExactSizeIterator<Item = Self> + DoubleEndedIterator {
        // SAFETY: the subtraction does not overflow, so the upper bound of the range does not
        // include `NUM_LEVELS`.
        (1..NUM_LEVELS - 1).map(Self)
    }

    /// Get all the levels from `self` to `other`, inclusive.
    ///
    /// If `self > other`, the returned iterator is empty.
    #[inline]
    pub fn inclusive_range(
        self,
        other: Self,
    ) -> impl ExactSizeIterator<Item = Self> + DoubleEndedIterator {
        // SAFETY: the upper bound of the range is strictly less than `NUM_LEVELS` by the
        // safety invariant, and thus everything in the range is strictly less than `NUM_LEVELS`.
        (self.0..=other.0).map(Self)
    }
}

pub(crate) trait IndexLevel<T> {
    #[must_use]
    fn infallible_index(&self, level: Level) -> &T;

    #[must_use]
    fn infallible_index_mut(&mut self, level: Level) -> &mut T;

    #[must_use]
    fn enumerated_iter<'a>(&'a self) -> impl ExactSizeIterator<Item = (Level, &'a T)>
    where
        T: 'a;

    #[must_use]
    fn into_enumerated_iter(self) -> impl ExactSizeIterator<Item = (Level, T)>;
}

impl<T> IndexLevel<T> for [T; NUM_LEVELS_USIZE] {
    fn infallible_index(&self, level: Level) -> &T {
        // SAFETY:
        // We need to ensure that `0 <= usize::from(level.inner()) < self.len()`.
        // This holds, since `self.len() == usize::from(NUM_LEVELS) == NUM_LEVELS_USIZE`,
        // and `level.inner() < NUM_LEVELS` for any `level: Level`.
        unsafe { self.get_unchecked(usize::from(level.inner())) }
    }

    fn infallible_index_mut(&mut self, level: Level) -> &mut T {
        // SAFETY:
        // We need to ensure that `0 <= usize::from(level.inner()) < self.len()`.
        // This holds, since `self.len() == usize::from(NUM_LEVELS) == NUM_LEVELS_USIZE`,
        // and `level.inner() < NUM_LEVELS` for any `level: Level`.
        unsafe { self.get_unchecked_mut(usize::from(level.inner())) }
    }

    fn enumerated_iter<'a>(&'a self) -> impl ExactSizeIterator<Item = (Level, &'a T)>
    where
        T: 'a,
    {
        self.iter().fuse().enumerate().map(|(index, value)| {
            // SAFETY: the iter returns the `NUM_LEVELS_USIZE` elements in the array, and can
            // never return anything afterwards. The indices returned by `Enumerate` start at 0,
            // so they fall in the range `0..NUM_LEVELS_USIZE`. Since
            // `NUM_LEVELS_USIZE < usize::from(u8::MAX)`, casting to `u8` does not wrap, so
            // `index as u8 < NUM_LEVELS`. Thus, the safety invariant is upheld.
            (Level(index as u8), value)
        })
    }

    fn into_enumerated_iter(self) -> impl ExactSizeIterator<Item = (Level, T)> {
        self.into_iter().fuse().enumerate().map(|(index, value)| {
            // SAFETY: the iter returns the `NUM_LEVELS_USIZE` elements in the array, and can
            // never return anything afterwards. The indices returned by `Enumerate` start at 0,
            // so they fall in the range `0..NUM_LEVELS_USIZE`. Since
            // `NUM_LEVELS_USIZE < usize::from(u8::MAX)`, casting to `u8` does not wrap, so
            // `index as u8 < NUM_LEVELS`. Thus, the safety invariant is upheld.
            (Level(index as u8), value)
        })
    }
}

impl TryFrom<u8> for Level {
    type Error = ();

    #[inline]
    fn try_from(level: u8) -> Result<Self, Self::Error> {
        if level < NUM_LEVELS {
            Ok(Self(level))
        } else {
            Err(())
        }
    }
}

impl TryFrom<u32> for Level {
    type Error = ();

    #[inline]
    fn try_from(level: u32) -> Result<Self, Self::Error> {
        if level < u32::from(NUM_LEVELS) {
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "the above comparison ensures that this cast does not wrap",
            )]
            Ok(Self(level as u8))
        } else {
            Err(())
        }
    }
}
