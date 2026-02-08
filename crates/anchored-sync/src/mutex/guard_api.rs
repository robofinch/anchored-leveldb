#![expect(unsafe_code, reason = "access data protected by the lock")]

use core::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    ops::{Deref, DerefMut},
};

use super::MaybeSyncMutexGuard;


impl<const SYNC: bool, T: ?Sized> Deref for MaybeSyncMutexGuard<'_, SYNC, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        // Safety invariant: we are allowed to access this field (regardless of thread).
        let data = &self.mutex.data;

        // SAFETY: We must uphold the aliasing rules. The existence of this guard implies that
        // we currently hold the lock and have exclusive access to `self.mutex.data` (and still
        // will during at least lifetime `'_`), so the created reference will not alias any
        // live mutable references.
        unsafe { &*data.get() }
    }
}

impl<const SYNC: bool, T: ?Sized> DerefMut for MaybeSyncMutexGuard<'_, SYNC, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Safety invariant: we are allowed to access this field (regardless of thread).
        let data = &self.mutex.data;

        // SAFETY: We must uphold the aliasing rules. The existence of this guard implies that
        // we currently hold the lock and have exclusive access to `self.mutex.data` (and still
        // will during at least lifetime `'_`), so the created reference will not alias any
        // live references not derived from the returned reference.
        unsafe { &mut *data.get() }
    }
}

impl<const SYNC: bool, T: ?Sized + Debug> Debug for MaybeSyncMutexGuard<'_, SYNC, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        // Safety invariant: this only accesses `self.mutex.data` (via `Deref`)
        Debug::fmt(&**self, f)
    }
}

impl<const SYNC: bool, T: ?Sized + Display> Display for MaybeSyncMutexGuard<'_, SYNC, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        // Safety invariant: this only accesses `self.mutex.data` (via `Deref`)
        Display::fmt(&**self, f)
    }
}

