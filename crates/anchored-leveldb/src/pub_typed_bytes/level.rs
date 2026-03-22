use std::num::{NonZeroU8, NonZeroUsize};


/// The number of levels in a LevelDB database.
pub const NUM_LEVELS:         NonZeroU8 = NonZeroU8::new(7).unwrap();
/// The number of nonzero levels in a LevelDB database.
pub const NUM_NONZERO_LEVELS: NonZeroU8 = NonZeroU8::new(6).unwrap();
/// The number of middle levels (excluding the first and last level) in a LevelDB database.
pub const NUM_MIDDLE_LEVELS:  NonZeroU8 = NonZeroU8::new(5).unwrap();

/// The number of levels in a LevelDB database.
///
/// Equal to [`NUM_LEVELS`].
pub const NUM_LEVELS_USIZE:         NonZeroUsize = NonZeroUsize::new(7).unwrap();
/// The number of nonzero levels in a LevelDB database.
///
/// Equal to [`NUM_NONZERO_LEVELS`].
pub const NUM_NONZERO_LEVELS_USIZE: NonZeroUsize = NonZeroUsize::new(6).unwrap();
/// The number of middle levels (excluding the first and last level) in a LevelDB database.
///
/// Equal to [`NUM_MIDDLE_LEVELS`].
pub const NUM_MIDDLE_LEVELS_USIZE:  NonZeroUsize = NonZeroUsize::new(5).unwrap();


/// A [`Level`] is a `u8` which is strictly less than [`NUM_LEVELS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Level(u8);

impl Level {
    pub(crate) const ZERO: Self = Self(0);

    /// All the levels in increasing order, from level 0 to level 6.
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

    pub(crate) const fn from_u32(level: u32) -> Option<Self> {
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "const-hack; also, by the bound check, `level as u8` does not truncate",
        )]
        if level < NUM_LEVELS.get() as u32 {
            Some(Self(level as u8))
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
    pub(crate) const fn next_level(self) -> Option<NonZeroLevel> {
        // Note that `self.0 < NUM_LEVELS < u8::MAX`, so this doesn't overflow.
        if self.0 + 1 < NUM_LEVELS.get() {
            #[expect(
                clippy::expect_used,
                reason = "the range invariant of `Level` means this always succeeds",
            )]
            Some(NonZeroLevel(NonZeroU8::new(self.0 + 1).expect("`Level.0 + 1` should not wrap")))
        } else {
            None
        }
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
        #[expect(clippy::indexing_slicing, reason = "statically known to succeed")]
        &self[usize::from(level.inner())]
    }

    fn infallible_index_mut(&mut self, level: Level) -> &mut T {
        // See `infallible_index`.
        #[expect(clippy::indexing_slicing, reason = "statically known to succeed")]
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
            // See `enumerated_iter`.
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "does not wrap",
            )]
            (Level(index as u8), value)
        })
    }
}

/// A [`NonZeroLevel`] is a [`NonZeroU8`] which is strictly less than [`NUM_LEVELS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct NonZeroLevel(NonZeroU8);

impl NonZeroLevel {
    /// All the nonzero levels in increasing order, from level 1 to level 6.
    pub(crate) const NONZERO_LEVELS: [Self; NUM_NONZERO_LEVELS_USIZE.get()] = [
        Self(NonZeroU8::new(1).unwrap()),
        Self(NonZeroU8::new(2).unwrap()),
        Self(NonZeroU8::new(3).unwrap()),
        Self(NonZeroU8::new(4).unwrap()),
        Self(NonZeroU8::new(5).unwrap()),
        Self(NonZeroU8::new(6).unwrap()),
    ];

    /// All the nonzero levels except for the greatest level in increasing order, from
    /// level 1 to level 5.
    pub(crate) const MIDDLE_LEVELS: [Self; NUM_MIDDLE_LEVELS_USIZE.get()] = [
        Self(NonZeroU8::new(1).unwrap()),
        Self(NonZeroU8::new(2).unwrap()),
        Self(NonZeroU8::new(3).unwrap()),
        Self(NonZeroU8::new(4).unwrap()),
        Self(NonZeroU8::new(5).unwrap()),
    ];

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
    pub const fn as_level(self) -> Level {
        Level(self.0.get())
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> NonZeroU8 {
        self.0
    }

    #[inline]
    #[must_use]
    pub(crate) const fn into_middle_level(self) -> Option<MiddleLevel> {
        if self.0.get() < NUM_LEVELS.get() - 1 {
            Some(MiddleLevel(self.0))
        } else {
            None
        }
    }
}

pub(crate) trait IndexNonZeroLevel<T> {
    #[must_use]
    fn infallible_index(&self, level: NonZeroLevel) -> &T;

    #[must_use]
    fn infallible_index_mut(&mut self, level: NonZeroLevel) -> &mut T;
}

impl<T> IndexNonZeroLevel<T> for [T; NUM_NONZERO_LEVELS_USIZE.get()] {
    fn infallible_index(&self, level: NonZeroLevel) -> &T {
        // We need to ensure that `0 <= usize::from(level.inner().get()) - 1 < self.len()`.
        // This holds, since
        // `self.len() == usize::from(NUM_NONZERO_LEVELS) == NUM_NONZERO_LEVELS_USIZE`,
        // and `0 <= level.inner().get() - 1 < NUM_NONZERO_LEVELS` for any `level: NonZeroLevel`.
        #[expect(clippy::indexing_slicing, reason = "statically known to succeed")]
        &self[usize::from(level.inner().get() - 1)]
    }

    fn infallible_index_mut(&mut self, level: NonZeroLevel) -> &mut T {
        // See `infallible_index`.
        #[expect(clippy::indexing_slicing, reason = "statically known to succeed")]
        &mut self[usize::from(level.inner().get() - 1)]
    }
}

/// A [`MiddleLevel`] is a [`NonZeroU8`] which is strictly less than the greatest level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct MiddleLevel(NonZeroU8);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl MiddleLevel {
    /// All the nonzero levels except for the greatest level in increasing order, from
    /// level 1 to level 5.
    pub(crate) const MIDDLE_LEVELS: [Self; NUM_MIDDLE_LEVELS_USIZE.get()] = [
        Self(NonZeroU8::new(1).unwrap()),
        Self(NonZeroU8::new(2).unwrap()),
        Self(NonZeroU8::new(3).unwrap()),
        Self(NonZeroU8::new(4).unwrap()),
        Self(NonZeroU8::new(5).unwrap()),
    ];

    #[inline]
    #[must_use]
    pub const fn new(level: NonZeroU8) -> Option<Self> {
        if level.get() < NUM_LEVELS.get() - 1 {
            Some(Self(level))
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    pub const fn as_level(self) -> Level {
        Level(self.0.get())
    }

    #[inline]
    #[must_use]
    pub const fn as_nonzero_level(self) -> NonZeroLevel {
        NonZeroLevel(self.0)
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> NonZeroU8 {
        self.0
    }

    #[inline]
    #[must_use]
    pub(crate) const fn next_level(self) -> NonZeroLevel {
        NonZeroLevel(self.0)
    }
}

pub(crate) trait IndexMiddleLevel<T> {
    #[must_use]
    fn infallible_index(&self, level: MiddleLevel) -> &T;

    #[must_use]
    fn infallible_index_mut(&mut self, level: MiddleLevel) -> &mut T;
}

impl<T> IndexMiddleLevel<T> for [T; NUM_MIDDLE_LEVELS_USIZE.get()] {
    fn infallible_index(&self, level: MiddleLevel) -> &T {
        // We need to ensure that `0 <= usize::from(level.inner().get()) - 1 < self.len()`.
        // This holds, since
        // `self.len() == usize::from(NUM_MIDDLE_LEVELS.get()) == NUM_MIDDLE_LEVELS_USIZE`,
        // and `0 <= level.inner().get() - 1 < NUM_NONZERO_LEVELS - 1 == NUM_MIDDLE_LEVELS_USIZE`
        // for any `level: MiddleLevel`.
        #[expect(clippy::indexing_slicing, reason = "statically known to succeed")]
        &self[usize::from(level.inner().get() - 1)]
    }

    fn infallible_index_mut(&mut self, level: MiddleLevel) -> &mut T {
        // See `infallible_index`.
        #[expect(clippy::indexing_slicing, reason = "statically known to succeed")]
        &mut self[usize::from(level.inner().get() - 1)]
    }
}
