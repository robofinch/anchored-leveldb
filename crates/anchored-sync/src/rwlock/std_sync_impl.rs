#![expect(unsafe_code, reason = "make `Raw{Read,Write}Guard` useful with minimal overhead")]

use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};

use crate::would_block_error::WouldBlockError;
use super::POISON_ERROR_MSG;


#[derive(Debug)]
pub(super) struct RawRwLock(RwLock<()>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl RawRwLock {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(RwLock::new(()))
    }

    #[expect(clippy::expect_used, reason = "panicking on poison is standard")]
    #[inline]
    pub fn read(&self) -> RawReadGuard<'_> {
        let poison_result: Result<_, PoisonError<_>> = self.0.read();

        let guard = poison_result.expect(POISON_ERROR_MSG);

        RawReadGuard(guard)
    }

    #[inline]
    pub fn read_ignoring_poison(&self) -> RawReadGuard<'_> {
        let guard = match self.0.read() {
            Ok(guard)   => guard,
            Err(poison) => poison.into_inner(),
        };

        RawReadGuard(guard)
    }

    #[expect(clippy::panic, clippy::panic_in_result_fn, reason = "panicking on poison is standard")]
    #[inline]
    pub fn try_read(&self) -> Result<RawReadGuard<'_>, WouldBlockError> {
        match self.0.try_read() {
            Ok(guard)                      => Ok(RawReadGuard(guard)),
            Err(TryLockError::Poisoned(_)) => panic!("{POISON_ERROR_MSG}"),
            Err(TryLockError::WouldBlock)  => Err(WouldBlockError),
        }
    }

    #[inline]
    pub fn try_read_ignoring_poison(&self) -> Result<RawReadGuard<'_>, WouldBlockError> {
        match self.0.try_read() {
            Ok(guard)                           => Ok(RawReadGuard(guard)),
            Err(TryLockError::Poisoned(poison)) => Ok(RawReadGuard(poison.into_inner())),
            Err(TryLockError::WouldBlock)       => Err(WouldBlockError),
        }
    }

    #[expect(clippy::expect_used, reason = "panicking on poison is standard")]
    #[inline]
    pub fn write(&self) -> RawWriteGuard<'_> {
        let poison_result: Result<_, PoisonError<_>> = self.0.write();

        let guard = poison_result.expect(POISON_ERROR_MSG);

        RawWriteGuard(guard)
    }

    #[inline]
    pub fn write_ignoring_poison(&self) -> RawWriteGuard<'_> {
        let guard = match self.0.write() {
            Ok(guard)   => guard,
            Err(poison) => poison.into_inner(),
        };

        RawWriteGuard(guard)
    }

    #[expect(clippy::panic, clippy::panic_in_result_fn, reason = "panicking on poison is standard")]
    #[inline]
    pub fn try_write(&self) -> Result<RawWriteGuard<'_>, WouldBlockError> {
        match self.0.try_write() {
            Ok(guard)                      => Ok(RawWriteGuard(guard)),
            Err(TryLockError::Poisoned(_)) => panic!("{POISON_ERROR_MSG}"),
            Err(TryLockError::WouldBlock)  => Err(WouldBlockError),
        }
    }

    #[inline]
    pub fn try_write_ignoring_poison(&self) -> Result<RawWriteGuard<'_>, WouldBlockError> {
        match self.0.try_write() {
            Ok(guard)                           => Ok(RawWriteGuard(guard)),
            Err(TryLockError::Poisoned(poison)) => Ok(RawWriteGuard(poison.into_inner())),
            Err(TryLockError::WouldBlock)       => Err(WouldBlockError),
        }
    }

    #[inline]
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.0.is_poisoned()
    }

    #[inline]
    pub fn clear_poison(&self) {
        self.0.clear_poison();
    }

    /// # Safety
    /// The provided `RawReadGuard` must be a guard of *this* `self` value.
    ///
    /// That is, it must have been obtained from `self.read()`, `self.read_ignoring_poision()`,
    /// or a `try_` variant of those two functions.
    #[expect(clippy::unused_self, reason = "mirroring impls for the other raw rwlock variants")]
    #[inline]
    pub unsafe fn unlock_reader(&self, _guard: RawReadGuard<'_>) {
        // Dropping the guard automatically releases a read lock.
    }

    /// # Safety
    /// The provided `RawWriteGuard` must be a guard of *this* `self` value.
    ///
    /// That is, it must have been obtained from `self.write()`, `self.write_ignoring_poison()`,
    /// or a `try_` variant of those two functions.
    #[expect(clippy::unused_self, reason = "mirroring impls for the other raw rwlock variants")]
    #[inline]
    pub unsafe fn unlock_writer(&self, _guard: RawWriteGuard<'_>) {
        // Dropping the guard automatically releases a write lock.
    }
}

#[derive(Debug)]
#[must_use = "guards should not be unintentionally dropped"]
#[clippy::has_significant_drop]
pub(super) struct RawReadGuard<'a>(
    #[expect(dead_code, reason = "the field is used for its `Drop` impl")]
    RwLockReadGuard<'a, ()>,
);

#[derive(Debug)]
#[must_use = "guards should not be unintentionally dropped"]
#[clippy::has_significant_drop]
pub(super) struct RawWriteGuard<'a>(
    #[expect(dead_code, reason = "the field is used for its `Drop` impl")]
    RwLockWriteGuard<'a, ()>,
);
