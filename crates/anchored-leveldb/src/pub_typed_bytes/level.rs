use std::num::{NonZeroU8, NonZeroUsize};


pub const NUM_LEVELS: NonZeroU8 = NonZeroU8::new(7).unwrap();
pub(crate) const NUM_LEVELS_USIZE: NonZeroUsize = NonZeroUsize::new(7).unwrap();


/// A [`Level`] is a `u8` which is strictly less than [`NUM_LEVELS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Level(u8);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl Level {
    pub(crate) const ZERO: Self = Self(0);

    pub(crate) const ALL_LEVELS: [Self; NUM_LEVELS_USIZE.get()] = [
        Self(0), Self(1), Self(2), Self(3), Self(4), Self(5), Self(6),
    ];

    #[inline]
    #[must_use]
    pub const fn new(level: u8) -> Option<Self> {
        if level < NUM_LEVELS.get() {
            Some(Self(level))
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> u8 {
        self.0
    }

    #[inline]
    #[must_use]
    pub(crate) const fn next_level(self) -> Option<Self> {
        // Note that `self.0 < NUM_LEVELS < u8::MAX`, so this doesn't overflow.
        if self.0 + 1 < NUM_LEVELS.get() {
            Some(Self(self.0 + 1))
        } else {
            None
        }
    }

    /// Get all the levels in increasing order, from level 0 to level 6.
    #[inline]
    pub(crate) fn all_levels() -> impl ExactSizeIterator<Item = Self> + DoubleEndedIterator {
        (0..NUM_LEVELS.get()).map(Self)
    }

    /// Get all the levels from `self` to `other`, inclusive.
    ///
    /// If `self > other`, the returned iterator is empty.
    #[inline]
    pub(crate) fn inclusive_range(
        self,
        other: Self,
    ) -> impl ExactSizeIterator<Item = Self> + DoubleEndedIterator {
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

impl<T> IndexLevel<T> for [T; NUM_LEVELS_USIZE.get()] {
    fn infallible_index(&self, level: Level) -> &T {
        // We need to ensure that `0 <= usize::from(level.inner()) < self.len()`.
        // This holds, since `self.len() == usize::from(NUM_LEVELS) == NUM_LEVELS_USIZE`,
        // and `level.inner() < NUM_LEVELS` for any `level: Level`.
        &self[usize::from(level.inner())]
    }

    fn infallible_index_mut(&mut self, level: Level) -> &mut T {
        // We need to ensure that `0 <= usize::from(level.inner()) < self.len()`.
        // This holds, since `self.len() == usize::from(NUM_LEVELS) == NUM_LEVELS_USIZE`,
        // and `level.inner() < NUM_LEVELS` for any `level: Level`.
        &mut self[usize::from(level.inner())]
    }

    fn enumerated_iter<'a>(&'a self) -> impl ExactSizeIterator<Item = (Level, &'a T)>
    where
        T: 'a,
    {
        self.iter().fuse().enumerate().map(|(index, value)| {
            // The iter returns the `NUM_LEVELS_USIZE` elements in the array, and
            // can never return anything afterwards. The indices returned by `Enumerate` start at 0,
            // so they fall in the range `0..NUM_LEVELS_USIZE`. Since
            // `NUM_LEVELS_USIZE < usize::from(u8::MAX)`, casting to `u8` does not wrap, so
            // `index as u8 < NUM_LEVELS`. Thus, the invariant is upheld.
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "does not wrap",
            )]
            (Level(index as u8), value)
        })
    }

    fn into_enumerated_iter(self) -> impl ExactSizeIterator<Item = (Level, T)> {
        self.into_iter().fuse().enumerate().map(|(index, value)| {
            // The iter returns the `NUM_LEVELS_USIZE` elements in the array, and
            // can never return anything afterwards. The indices returned by `Enumerate` start at 0,
            // so they fall in the range `0..NUM_LEVELS_USIZE`. Since
            // `NUM_LEVELS_USIZE < usize::from(u8::MAX)`, casting to `u8` does not wrap, so
            // `index as u8 < NUM_LEVELS`. Thus, the invariant is upheld.
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "does not wrap",
            )]
            (Level(index as u8), value)
        })
    }
}

impl TryFrom<u8> for Level {
    type Error = ();

    #[inline]
    fn try_from(level: u8) -> Result<Self, Self::Error> {
        Self::new(level).ok_or(())
    }
}

impl TryFrom<u32> for Level {
    type Error = ();

    #[inline]
    fn try_from(level: u32) -> Result<Self, Self::Error> {
        if level < u32::from(NUM_LEVELS.get()) {
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

/// A [`NonZeroLevel`] is a [`NonZeroU8`] which is strictly less than [`NUM_LEVELS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct NonZeroLevel(NonZeroU8);

impl NonZeroLevel {
    #[inline]
    #[must_use]
    pub const fn new(level: NonZeroU8) -> Option<Self> {
        if level.get() < NUM_LEVELS.get() {
            Some(Self(level))
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> NonZeroU8 {
        self.0
    }

    /// Get all the nonzero levels in increasing order, from level 1 to level 6.
    #[inline]
    pub(crate) fn nonzero_levels() -> impl ExactSizeIterator<Item = Self> + DoubleEndedIterator {
        (1..NUM_LEVELS.get()).map(|num| Self(NonZeroU8::new(num).unwrap()))
    }

    /// Get all the nonzero levels except for the greatest level in increasing order, from
    /// level 1 to level 5.
    #[inline]
    pub(crate) fn middle_levels() -> impl ExactSizeIterator<Item = Self> + DoubleEndedIterator {
        (1..NUM_LEVELS.get() - 1).map(|num| Self(NonZeroU8::new(num).unwrap()))
    }
}
