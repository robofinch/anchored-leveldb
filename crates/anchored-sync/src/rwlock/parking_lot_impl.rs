#![expect(unsafe_code, reason = "make `Raw{Read,Write}Guard` useful with minimal overhead")]

use core::marker::PhantomData;
use core::fmt::{Debug, Formatter, Result as FmtResult};

use parking_lot::RawRwLock as ParkingLotRawRwLock;
use parking_lot::lock_api::RawRwLock as _;

use crate::would_block_error::WouldBlockError;


pub(super) struct RawRwLock(ParkingLotRawRwLock);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl RawRwLock {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(ParkingLotRawRwLock::INIT)
    }

    #[inline]
    pub fn read(&self) -> RawReadGuard<'_> {
        self.0.lock_shared();

        // We acquired a read lock.
        RawReadGuard(PhantomData)
    }

    #[inline]
    pub fn read_ignoring_poison(&self) -> RawReadGuard<'_> {
        self.read()
    }

    #[inline]
    pub fn try_read(&self) -> Result<RawReadGuard<'_>, WouldBlockError> {
        if self.0.try_lock_shared() {
            // We acquired a read lock.
            Ok(RawReadGuard(PhantomData))
        } else {
            Err(WouldBlockError)
        }
    }

    #[inline]
    pub fn try_read_ignoring_poison(&self) -> Result<RawReadGuard<'_>, WouldBlockError> {
        self.try_read()
    }

    #[inline]
    pub fn write(&self) -> RawWriteGuard<'_> {
        self.0.lock_exclusive();

        // We acquired a write lock.
        RawWriteGuard(PhantomData)
    }

    #[inline]
    pub fn write_ignoring_poison(&self) -> RawWriteGuard<'_> {
        self.write()
    }

    #[inline]
    pub fn try_write(&self) -> Result<RawWriteGuard<'_>, WouldBlockError> {
        if self.0.try_lock_exclusive() {
            // We acquired a write lock.
            Ok(RawWriteGuard(PhantomData))
        } else {
            Err(WouldBlockError)
        }
    }

    #[inline]
    pub fn try_write_ignoring_poison(&self) -> Result<RawWriteGuard<'_>, WouldBlockError> {
        self.try_write()
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
    /// The provided `RawReadGuard` must be a guard of *this* `self` value.
    ///
    /// That is, it must have been obtained from `self.read()`, `self.read_ignoring_poision()`,
    /// or a `try_` variant of those two functions.
    #[inline]
    pub unsafe fn unlock_reader(&self, _guard: RawReadGuard<'_>) {
        // SAFETY: as proven by the guard (which does not impl `Send`, `Clone`, etc, and can only
        // be constructed in this module), this thread had a shared lock of `self.0`
        // which is being released by this function.
        unsafe {
            self.0.unlock_shared();
        }
    }

    /// # Safety
    /// The provided `RawWriteGuard` must be a guard of *this* `self` value.
    ///
    /// That is, it must have been obtained from `self.write()`, `self.write_ignoring_poison()`,
    /// or a `try_` variant of those two functions.
    #[inline]
    pub unsafe fn unlock_writer(&self, _guard: RawWriteGuard<'_>) {
        // SAFETY: as proven by the guard (which does not impl `Send`, `Clone`, etc, and can only
        // be constructed in this module), `self.0` was exclusively locked by this thread,
        // and that lock is being released by this function.
        unsafe {
            self.0.unlock_exclusive();
        }
    }
}

impl Debug for RawRwLock {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("RawRwLock").field(&"<parking_lot::RawRwLock>").finish()
    }
}

#[must_use = "guards should not be unintentionally dropped"]
pub(super) struct RawReadGuard<'a>(PhantomData<(&'a (), *const ())>);

impl Debug for RawReadGuard<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("RawReadGuard").field(&"<read lock token>").finish()
    }
}

#[must_use = "guards should not be unintentionally dropped"]
pub(super) struct RawWriteGuard<'a>(PhantomData<(&'a (), *const ())>);

impl Debug for RawWriteGuard<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("RawWriteGuard").field(&"<write lock token>").finish()
    }
}
