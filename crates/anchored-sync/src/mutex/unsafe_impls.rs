#![expect(
    unsafe_code,
    reason = "needed to read union fields, drop ManuallyDrop fields, and impl Send + Sync",
)]

use core::mem::ManuallyDrop;

use super::{
    MaybeSyncMutex, MaybeSyncMutexGuard, MaybeSyncRawMutexGuard,
    RawCellMutex, RawCellMutexGuard, RawMutex, RawMutexGuard,
};


// Rough table of contents:
// - `unsafe` helper functions for `MaybeSyncMutex`
// - impls of `Drop`, `Send`, and `Sync` for `MaybeSyncMutex`
// - impls of `Drop` and `Sync` for `MaybeSyncMutexGuard`

impl<const SYNC: bool, T: ?Sized> MaybeSyncMutex<SYNC, T> {
    /// # Safety
    /// `self.raw` must not be used again after calling this method (not even by moving it).
    pub(super) unsafe fn drop_raw_mutex(&mut self) {
        if SYNC {
            // SAFETY: if `SYNC`, then the `sync` field is initialized
            let sync = unsafe { &mut self.raw.sync };
            // SAFETY: we do not use `sync` (not even moving it) after calling
            // `ManuallyDrop::drop` on `sync`, since we do not use our reference to `self.raw`
            // or `self.raw.sync`, and the caller promises not to do so either.
            unsafe {
                ManuallyDrop::drop(sync);
            };
        } else {
            // SAFETY: if `SYNC`, then the `unsync` field is initialized
            let unsync = unsafe { &mut self.raw.unsync };
            // SAFETY: we do not use `unsync` (not even moving it) after calling
            // `ManuallyDrop::drop` on `unsync`, since we do not use our reference to `self.raw`
            // or `self.raw.unsync`, and the caller promises not to do so either.
            unsafe {
                ManuallyDrop::drop(unsync);
            };
        }
    }

    /// # Safety
    /// `sync_f` and `unsync_f` must, on the thread on which `lock_fn` is called, obtain and
    /// return a raw mutex guard associated with the mutex given to them as an argument.
    ///
    /// This ensures that the created guard can be unlocked on the current thread and precludes
    /// any pathological functions that return guards from some other mutex.
    pub(super) unsafe fn lock_fn<'a, IfSync, IfUnsync>(
        &'a self,
        sync_f:   IfSync,
        unsync_f: IfUnsync,
    ) -> MaybeSyncMutexGuard<'a, SYNC, T>
    where
        IfSync:   FnOnce(&'a RawMutex) -> RawMutexGuard<'a>,
        IfUnsync: FnOnce(&'a RawCellMutex) -> RawCellMutexGuard<'a>,
    {
        if SYNC {
            // SAFETY: `SYNC` is true, so the `sync` field is initialized.
            let sync = unsafe { &self.raw.sync };

            let raw_guard = sync_f(sync);

            MaybeSyncMutexGuard {
                mutex: self,
                raw:   MaybeSyncRawMutexGuard {
                    // Safety invariant: `SYNC` is true, and we initialize the `sync` field
                    // to a guard associated with `guard.mutex.raw.sync` obtained on the current
                    // thread.
                    sync: ManuallyDrop::new(raw_guard),
                },
            }
        } else {
            // SAFETY: `SYNC` is false, so the `unsync` field is initialized.
            let unsync = unsafe { &self.raw.unsync };

            let raw_guard = unsync_f(unsync);

            MaybeSyncMutexGuard {
                mutex: self,
                raw:   MaybeSyncRawMutexGuard {
                    // Safety invariant: `SYNC` is false, and we initialize the `unsync` field
                    // to a guard associated with `guard.mutex.raw.unsync`.
                    unsync: ManuallyDrop::new(raw_guard),
                },
            }
        }
    }

    /// # Safety
    /// If `sync_f` or `unsync_f` returns `Ok(_)`, then it must, on the thread on which
    /// `try_lock_fn` is called, obtain and return a raw mutex guard associated with the mutex
    /// given to it as an argument.
    ///
    /// This ensures that the created guard can be unlocked on the current thread and precludes
    /// any pathological functions that return guards from some other mutex.
    pub(super) unsafe fn try_lock_fn<'a, IfSync, IfUnsync, E>(
        &'a self,
        sync_f:   IfSync,
        unsync_f: IfUnsync,
    ) -> Result<MaybeSyncMutexGuard<'a, SYNC, T>, E>
    where
        IfSync:   FnOnce(&'a RawMutex) -> Result<RawMutexGuard<'a>, E>,
        IfUnsync: FnOnce(&'a RawCellMutex) -> Result<RawCellMutexGuard<'a>, E>,
    {
        if SYNC {
            // SAFETY: `SYNC` is true, so the `sync` field is initialized.
            let sync = unsafe { &self.raw.sync };

            let raw_guard = sync_f(sync)?;

            Ok(MaybeSyncMutexGuard {
                mutex: self,
                raw:   MaybeSyncRawMutexGuard {
                    // Safety invariant: `SYNC` is true, and we initialize the `sync` field
                    // to a guard associated with `guard.mutex.raw.sync` obtained on the current
                    // thread.
                    sync: ManuallyDrop::new(raw_guard),
                },
            })
        } else {
            // SAFETY: `SYNC` is false, so the `unsync` field is initialized.
            let unsync = unsafe { &self.raw.unsync };

            let raw_guard = unsync_f(unsync)?;

            Ok(MaybeSyncMutexGuard {
                mutex: self,
                raw:   MaybeSyncRawMutexGuard {
                    // Safety invariant: `SYNC` is false, and we initialize the `unsync` field
                    // to a guard associated with `guard.mutex.raw.unsync`.
                    unsync: ManuallyDrop::new(raw_guard),
                },
            })
        }
    }
}

impl<const SYNC: bool, T: ?Sized> Drop for MaybeSyncMutex<SYNC, T> {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: we do not use `self.raw` after calling this function, since this is the last
        // action taken in the destructor of `self`.
        unsafe { self.drop_raw_mutex() }
    }
}

// SAFETY: Same as the impl for `Mutex<T>`. It needs `T: Send` since the `Drop` impl (at the very
// least) can access the `T` if the `Mutex` is sent to a different thread. A `Sync` bound is not
// needed, since at most one thread accesses the `T` at a time (as guaranteed by the `Mutex`).
unsafe impl<T: ?Sized + Send> Send for MaybeSyncMutex<true, T> {}
// SAFETY: Same as the impl for `Mutex<T>`. It needs `T: Send` since getting a `&mut T` from
// `lock` allows a `T` to be moved out (possibly into a different thread). A `Sync` bound is not
// needed, since at most one thread accesses the `T` at a time (as guaranteed by the `Mutex`).
unsafe impl<T: ?Sized + Send> Sync for MaybeSyncMutex<true, T> {}

impl<const SYNC: bool, T: ?Sized> Drop for MaybeSyncMutexGuard<'_, SYNC, T> {
    #[inline]
    fn drop(&mut self) {
        // Note: the safety invariants of `self.raw` and `self.mutex.raw` permit this destructor
        // function to soundly access them.
        if SYNC {
            // SAFETY: if `SYNC`, then the `sync` field is initialized
            let sync = unsafe { &mut self.raw.sync };
            // SAFETY: we do not use `sync` (not even moving it) after calling
            // `ManuallyDrop::take` on `sync`; we know this since we are in the destructor,
            // so even if `mutex_sync.unlock` were to run arbitrary code, that code would not
            // be able to access the `raw.sync` field owned by the
            // no-longer-soundly-reachable-from-other-code `self` value.
            let guard = unsafe { ManuallyDrop::take(sync) };

            // SAFETY: if `SYNC`, then the `sync` field is initialized
            let mutex_sync = unsafe { &self.mutex.raw.sync };

            // SAFETY: since `MaybeSyncMutexGuard` does not implement `Send`, regardless of
            // `SYNC` (since it has a union field of type `RawCellMutexGuard`, which does not
            // implement `Send`), we know we're on the same thread from which the guard was
            // obtained. Additionally, the guard was *not* obtained from a different mutex; we keep
            // the mutex reference and guard together, and do not swap them out.
            unsafe {
                mutex_sync.unlock(guard);
            }
        } else {
            // SAFETY: if `!SYNC`, then the `unsync` field is initialized
            let unsync = unsafe { &mut self.raw.unsync };
            // SAFETY: we do not use `unsync` (not even moving it) after calling
            // `ManuallyDrop::take` on `unsync`; we know this since we are in the destructor,
            // so even if `mutex_unsync.unlock` were to run arbitrary code, that code would not
            // be able to access the `raw.unsync` field owned by the
            // no-longer-soundly-reachable-from-other-code `self` value.
            let guard = unsafe { ManuallyDrop::take(unsync) };

            // SAFETY: if `!SYNC`, then the `unsync` field is initialized
            let mutex_unsync = unsafe { &self.mutex.raw.unsync };

            // SAFETY: The guard was *not* obtained from a different mutex; we keep
            // the mutex reference and guard together, and do not swap them out.
            unsafe {
                mutex_unsync.unlock(guard);
            }
        }
    }
}

// SAFETY: Same as the impl for `std::sync::MutexGuard<T>`. Sharing a mutex guard across
// threads (behind a `&`) is akin to sharing a `&&mut T` between threads, which is permitted
// iff `T: Sync`. The relevant `&mut T`, in this case, comes from `guard.mutex.data`.
// The `guard` also has `guard.raw` and `guard.mutex.raw`. As per the safety invariants of
// the `guard` fields, we only access those fields in the destructor, so we do not allow concurrent
// access to them from multiple threads even if the guard implements `Sync`. Therefore, the fact
// that those two fields' types might not be `Sync` does not matter.
unsafe impl<const SYNC: bool, T: ?Sized + Sync> Sync for MaybeSyncMutexGuard<'_, SYNC, T> {}
