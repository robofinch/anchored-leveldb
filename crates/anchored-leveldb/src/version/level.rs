#![expect(unsafe_code, reason = "index `[T; NUM_LEVELS as usize]` without bounds checking")]

use crate::format::NUM_LEVELS;


/// Invariant: the inner value of a [`Level`] is strictly less than [`NUM_LEVELS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct Level(u8);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl Level {
    #[inline]
    #[must_use]
    pub fn inner(self) -> u8 {
        self.0
    }
}

pub(crate) trait IndexLevel<T> {
    #[must_use]
    fn infallible_index(&self, level: Level) -> &T;

    #[must_use]
    fn infallible_index_mut(&mut self, level: Level) -> &mut T;
}

#[expect(clippy::as_conversions, reason = "needed to cast u8 -> usize in const context")]
impl<T> IndexLevel<T> for [T; NUM_LEVELS as usize] {
    fn infallible_index(&self, level: Level) -> &T {
        // SAFETY:
        // We neeed to ensure that `0 <= usize::from(level.inner()) < self.len()`.
        // This holds, since `self.len() = usize::from(NUM_LEVELS)`,
        // and `level.inner() < NUM_LEVELS` for any `level: Level`.
        unsafe { self.get_unchecked(usize::from(level.inner())) }
    }

    fn infallible_index_mut(&mut self, level: Level) -> &mut T {
        // SAFETY:
        // We neeed to ensure that `0 <= usize::from(level.inner()) < self.len()`.
        // This holds, since `self.len() = usize::from(NUM_LEVELS)`,
        // and `level.inner() < NUM_LEVELS` for any `level: Level`.
        unsafe { self.get_unchecked_mut(usize::from(level.inner())) }
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
