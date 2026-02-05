#![expect(unsafe_code, reason = "make `RawCellMutexGuard` useful with minimal overhead")]

use core::{cell::Cell, marker::PhantomData};
use core::fmt::{Debug, Formatter, Result as FmtResult};

use crate::would_block_error::WouldBlockError;


#[derive(Debug)]
pub(super) struct RawCellMutex(Cell<bool>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl RawCellMutex {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(Cell::new(false))
    }

    pub fn lock(&self) -> RawCellMutexGuard<'_> {
        assert!(
            !self.0.get(),
            "Attempted to lock an anchored-sync MaybeSyncMutex \
             on a thread which already holds the lock",
        );

        self.0.set(true);

        RawCellMutexGuard(PhantomData)
    }

    pub fn lock_ignoring_poison(&self) -> RawCellMutexGuard<'_> {
        self.lock()
    }

    pub fn try_lock(&self) -> Result<RawCellMutexGuard<'_>, WouldBlockError> {
        if self.0.get() {
            Err(WouldBlockError)
        } else {
            self.0.set(true);

            Ok(RawCellMutexGuard(PhantomData))
        }
    }

    pub fn try_lock_ignoring_poison(&self) -> Result<RawCellMutexGuard<'_>, WouldBlockError> {
       self.try_lock()
    }

    #[expect(clippy::unused_self, reason = "mirroring std::sync impl")]
    #[inline]
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        false
    }

    #[expect(clippy::unused_self, reason = "mirroring std::sync impl")]
    #[inline]
    pub const fn clear_poison(&self) {}

    /// # Safety
    /// The provided `RawMutexGuard` must be a guard of *this* `self` value.
    ///
    /// That is, it must have been obtained from `self.lock()`, `self.lock_ignoring_poison()`,
    /// or a `try_` variant of those two functions.
    #[expect(clippy::unused_self, reason = "mirroring std::sync impl")]
    pub const unsafe fn unlock(&self, _guard: RawCellMutexGuard<'_>) {
        // Dropping the guard automatically unlocks the mutex.
    }
}

#[must_use = "guards should not be unintentionally dropped"]
pub(super) struct RawCellMutexGuard<'a>(PhantomData<(&'a (), *const ())>);

impl Debug for RawCellMutexGuard<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("RawCellMutexGuard").field(&"<lock token>").finish()
    }
}
