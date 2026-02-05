#![expect(unsafe_code, reason = "make `RawMutexGuard` useful with minimal overhead")]

use std::sync::{Mutex, MutexGuard, PoisonError, TryLockError as StdSyncTryLockError};

use crate::would_block_error::WouldBlockError;
use super::POISON_ERROR_MSG;


#[derive(Debug)]
pub(super) struct RawMutex(Mutex<()>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl RawMutex {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(Mutex::new(()))
    }

    #[expect(clippy::expect_used, reason = "panicking on poison is standard")]
    pub fn lock(&self) -> RawMutexGuard<'_> {
        let poison_result: Result<_, PoisonError<_>> = self.0.lock();

        let guard = poison_result.expect(POISON_ERROR_MSG);

        RawMutexGuard(guard)
    }

    pub fn lock_ignoring_poison(&self) -> RawMutexGuard<'_> {
        let guard = match self.0.lock() {
            Ok(guard)   => guard,
            Err(poison) => poison.into_inner(),
        };

        RawMutexGuard(guard)
    }

    #[expect(clippy::panic, clippy::panic_in_result_fn, reason = "panicking on poison is standard")]
    pub fn try_lock(&self) -> Result<RawMutexGuard<'_>, WouldBlockError> {
        match self.0.try_lock() {
            Ok(guard) => Ok(RawMutexGuard(guard)),
            Err(StdSyncTryLockError::Poisoned(_)) => panic!("{POISON_ERROR_MSG}"),
            Err(StdSyncTryLockError::WouldBlock) => Err(WouldBlockError),
        }
    }

    pub fn try_lock_ignoring_poison(&self) -> Result<RawMutexGuard<'_>, WouldBlockError> {
        match self.0.try_lock() {
            Ok(guard) => Ok(RawMutexGuard(guard)),
            Err(StdSyncTryLockError::Poisoned(poison)) => {
                Ok(RawMutexGuard(poison.into_inner()))
            }
            Err(StdSyncTryLockError::WouldBlock) => Err(WouldBlockError),
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
    /// The provided `RawMutexGuard` must be a guard of *this* `self` value.
    ///
    /// That is, it must have been obtained from `self.lock()`, `self.lock_ignoring_poison()`,
    /// or a `try_` variant of those two functions.
    #[expect(clippy::unused_self, reason = "mirroring impls for the other raw mutex variants")]
    pub unsafe fn unlock(&self, _guard: RawMutexGuard<'_>) {
        // Dropping the guard automatically unlocks the mutex.
    }
}

#[derive(Debug)]
#[must_use = "guards should not be unintentionally dropped"]
#[clippy::has_significant_drop]
pub(super) struct RawMutexGuard<'a>(
    #[expect(dead_code, reason = "the field is used for its `Drop` impl")]
    MutexGuard<'a, ()>,
);
