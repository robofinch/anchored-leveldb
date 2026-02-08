#![expect(unsafe_code, reason = "needed to read union fields and associate a guard with its mutex")]

use core::{cell::UnsafeCell, mem::ManuallyDrop, ptr};
use core::fmt::{Debug, Formatter, Result as FmtResult};

use crate::would_block_error::WouldBlockError;
use super::{
    MaybeSyncMutex, MaybeSyncMutexGuard, MaybeSyncRawMutex,
    POISON_ERROR_MSG, RawCellMutex, RawMutex,
};


impl<const SYNC: bool, T> MaybeSyncMutex<SYNC, T> {
    /// Creates a new mutex in an unlocked state.
    #[inline]
    #[must_use]
    pub const fn new(value: T) -> Self {
        if SYNC {
            Self {
                raw: MaybeSyncRawMutex {
                    // Safety invariant: `SYNC` is true, and we initialize the `sync` field.
                    sync: ManuallyDrop::new(RawMutex::new()),
                },
                data: UnsafeCell::new(value),
            }
        } else {
            Self {
                raw: MaybeSyncRawMutex {
                    // Safety invariant: `SYNC` is false, and we initialize the `unsync` field.
                    unsync: ManuallyDrop::new(RawCellMutex::new()),
                },
                data: UnsafeCell::new(value),
            }
        }
    }

    /// Consumes this mutex, returning the underlying data.
    ///
    /// # Panics
    /// [If this mutex supports poisoning] and a thread panicked while holding the lock,
    /// the mutex becomes poisoned. This function panics if the mutex is currently poisoned.
    ///
    /// [If this mutex supports poisoning]: MaybeSyncMutex::supports_poisoning
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> T {
        assert!(!self.is_poisoned(), "{POISON_ERROR_MSG}");

        self.into_inner_ignoring_poison()
    }

    /// Consumes this mutex, returning the underlying data.
    ///
    /// # Ignoring poison
    /// [If this mutex supports poisoning] and a thread panicked while holding the lock,
    /// the mutex becomes poisoned. This function ignores the poisoned state of the mutex.
    ///
    /// [If this mutex supports poisoning]: MaybeSyncMutex::supports_poisoning
    #[inline]
    #[must_use]
    pub fn into_inner_ignoring_poison(self) -> T {
        let mut this = ManuallyDrop::new(self);

        // SAFETY: We do not use `this.raw` after calling this function (not even by moving
        // it or `this`), and since we have exclusive ownership over `self`, nobody else will
        // be able to access `this.raw` either.
        unsafe {
            this.drop_raw_mutex();
        };

        let this_data: *const UnsafeCell<T> = &raw const this.data;

        // SAFETY:
        // - `&raw const this.data` is valid for reads (we own the pointee, so we
        //   can guarantee that there's nothing aliasing this read, we know that it
        //   has sufficient for a read, the pointee is in a single allocation, etc).
        // - `&raw const this.data` is properly-aligned (`Self` is not `repr(packed)`)
        // - `this.data` is a properly initialized value of type `UnsafeCell<T>`
        //
        // Note that this cannot cause a double-drop of `this.data`, since `this`
        // is wrapped in a `ManuallyDrop`.
        let this_data: UnsafeCell<T> = unsafe { ptr::read(this_data) };

        this_data.into_inner()
    }
}

impl<const SYNC: bool, T: ?Sized> MaybeSyncMutex<SYNC, T> {
    /// Acquires a mutex, blocking the current thread until it is able to do so.
    ///
    /// This function will block the local thread until it is available to acquire the mutex.
    /// Upon returning, the thread is the only thread with the lock held. An RAII guard is returned
    /// to allow scoped unlock of the lock. When the guard goes out of scope, the mutex will be
    /// unlocked.
    ///
    /// # Panics and Deadlocks
    /// If the current thread already holds the lock, this function will panic or deadlock.
    ///
    /// [If this mutex supports poisoning] and a thread panicked while holding the lock,
    /// the mutex becomes poisoned. This function panics if the mutex is currently poisoned.
    ///
    /// [If this mutex supports poisoning]: MaybeSyncMutex::supports_poisoning
    #[inline]
    pub fn lock(&self) -> MaybeSyncMutexGuard<'_, SYNC, T> {
        // SAFETY: `RawMutex::lock` and `RawCellMutex::lock` obtain a guard associated with the
        // input mutex on the current thread.
        unsafe { self.lock_fn(RawMutex::lock, RawCellMutex::lock) }
    }

    /// Acquires a mutex, blocking the current thread until it is able to do so.
    ///
    /// This function will block the local thread until it is available to acquire the mutex.
    /// Upon returning, the thread is the only thread with the lock held. An RAII guard is returned
    /// to allow scoped unlock of the lock. When the guard goes out of scope, the mutex will be
    /// unlocked.
    ///
    /// # Ignoring poison
    /// [If this mutex supports poisoning] and a thread panicked while holding the lock,
    /// the mutex becomes poisoned. This function ignores the poisoned state of the mutex, though
    /// does not clear the poisoned state.
    ///
    /// # Panics and Deadlocks
    /// If the current thread already holds the lock, this function will panic or deadlock.
    ///
    /// [If this mutex supports poisoning]: MaybeSyncMutex::supports_poisoning
    #[inline]
    pub fn lock_ignoring_poison(&self) -> MaybeSyncMutexGuard<'_, SYNC, T> {
        // SAFETY: `RawMutex::lock_ignoring_poison` and `RawCellMutex::lock_ignoring_poison`
        // obtain a guard associated with the input mutex on the current thread.
        unsafe {
            self.lock_fn(
                RawMutex::lock_ignoring_poison,
                RawCellMutex::lock_ignoring_poison,
            )
        }
    }

    /// Attempts to acquire this lock.
    ///
    /// If the lock could not be acquired at this time, then `Err` is returned. Otherwise, an RAII
    /// guard is returned. The lock will be unlocked when the guard is dropped.
    ///
    /// This function does not block.
    ///
    /// # Errors
    /// If the mutex could not be acquired because it is already locked, then this call will
    /// return a [`WouldBlockError`] error.
    ///
    /// This includes if the lock is already held by the current thread.
    ///
    /// # Panics
    /// [If this mutex supports poisoning] and a thread panicked while holding the lock,
    /// the mutex becomes poisoned. This function panics if the mutex is currently poisoned.
    ///
    /// [If this mutex supports poisoning]: MaybeSyncMutex::supports_poisoning
    #[inline]
    pub fn try_lock(&self) -> Result<MaybeSyncMutexGuard<'_, SYNC, T>, WouldBlockError> {
        // SAFETY: `RawMutex::try_lock` and `RawCellMutex::try_lock` obtain a guard
        // associated with the input mutex on the current thread (if they are successful).
        unsafe { self.try_lock_fn(RawMutex::try_lock, RawCellMutex::try_lock) }
    }

    /// Attempts to acquire this lock.
    ///
    /// If the lock could not be acquired at this time, then `Err` is returned. Otherwise, an RAII
    /// guard is returned. The lock will be unlocked when the guard is dropped.
    ///
    /// This function does not block.
    ///
    /// # Ignoring poison
    /// [If this mutex supports poisoning] and a thread panicked while holding the lock,
    /// the mutex becomes poisoned. This function ignores the poisoned state of the mutex, though
    /// does not clear the poisoned state.
    ///
    /// # Errors
    /// If the mutex could not be acquired because it is already locked, then this call will
    /// return a [`WouldBlockError`] error.
    ///
    /// This includes if the lock is already held by the current thread.
    ///
    /// [If this mutex supports poisoning]: MaybeSyncMutex::supports_poisoning
    #[inline]
    pub fn try_lock_ignoring_poison(
        &self,
    ) -> Result<MaybeSyncMutexGuard<'_, SYNC, T>, WouldBlockError> {
        // SAFETY: `RawMutex::try_lock_ignoring_poison` and `RawCellMutex::try_lock_ignoring_poison`
        // obtain a guard associated with the input mutex on the current thread
        // (if they are successful).
        unsafe {
            self.try_lock_fn(
                RawMutex::try_lock_ignoring_poison,
                RawCellMutex::try_lock_ignoring_poison,
            )
        }
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the mutex mutably, no actual locking needs to take place – the
    /// mutable borrow statically guarantees no new locks can be acquired while this reference
    /// exists. Note that this method does not clear any previous abandoned locks
    /// (e.g., via [`forget()`] on a [`MaybeSyncMutexGuard`]).
    ///
    /// # Panics
    /// [If this mutex supports poisoning] and a thread panicked while holding the lock,
    /// the mutex becomes poisoned. This function panics if the mutex is currently poisoned.
    ///
    /// [`forget()`]: core::mem::forget
    /// [If this mutex supports poisoning]: MaybeSyncMutex::supports_poisoning
    #[inline]
    #[must_use]
    pub fn get_mut(&mut self) -> &mut T {
        assert!(!self.is_poisoned(), "{POISON_ERROR_MSG}");

        self.get_mut_ignoring_poison()
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the mutex mutably, no actual locking needs to take place – the
    /// mutable borrow statically guarantees no new locks can be acquired while this reference
    /// exists. Note that this method does not clear any previous abandoned locks
    /// (e.g., via [`forget()`] on a [`MaybeSyncMutexGuard`]).
    ///
    /// # Ignoring poison
    /// [If this mutex supports poisoning] and a thread panicked while holding the lock,
    /// the mutex becomes poisoned. This function ignores the poisoned state of the mutex.
    ///
    /// [`forget()`]: core::mem::forget
    /// [If this mutex supports poisoning]: MaybeSyncMutex::supports_poisoning
    #[inline]
    #[must_use]
    pub const fn get_mut_ignoring_poison(&mut self) -> &mut T {
        self.data.get_mut()
    }

    /// Check whether the mutex supports poisoning.
    ///
    /// This mutex supports poisoning if the `parking_lot` feature is **not** enabled and
    /// `SYNC` is `true`.
    ///
    /// If poisoning is not supported, the mutex will never become poisoned.
    #[inline]
    #[must_use]
    pub const fn supports_poisoning() -> bool {
        if cfg!(feature = "parking_lot") {
            false
        } else {
            // Poisoning is supported iff the `parking_lot` feature is not enabled
            // and `SYNC` is true.
            SYNC
        }
    }

    /// Determines whether the mutex is poisoned.
    ///
    /// See [`std::sync::Mutex::is_poisoned`].
    ///
    /// The mutex can become poisoned at any time that it might be held by a different thread
    /// ([if the mutex supports poisoning]). You should not trust a `false` value for program
    /// correctness without additional synchronization.
    ///
    /// In particular, the return value is reliable if the thread calling this ownership has
    /// exclusive access to the mutex (relevant for checking for poison before calling
    /// [`into_inner_ignoring_poison`] or [`get_mut_ignoring_poison`]) or if the lock is held by
    /// the thread calling this method (relevant for checking for poison after calling
    /// [`lock_ignoring_poison`] or similar).
    ///
    /// Therefore, even though this mutex does not have methods that return
    /// `Result<_, PoisonError<_>>` or similar, the support for poisoning provided by this mutex
    /// (when supported at all) is not less powerful than that of [`std::sync::Mutex`].
    ///
    /// [if the mutex supports poisoning]: MaybeSyncMutex::supports_poisoning
    /// [`into_inner_ignoring_poison`]: MaybeSyncMutex::into_inner_ignoring_poison
    /// [`lock_ignoring_poison`]: MaybeSyncMutex::lock_ignoring_poison
    /// [`get_mut_ignoring_poison`]: MaybeSyncMutex::get_mut_ignoring_poison
    #[inline]
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        if SYNC {
            // SAFETY: `SYNC` is true, so the `sync` field is initialized.
            let sync = unsafe { &self.raw.sync };
            sync.is_poisoned()
        } else {
            // SAFETY: `SYNC` is false, so the `unsync` field is initialized.
            let unsync = unsafe { &self.raw.unsync };
            unsync.is_poisoned()
        }
    }

    /// Clear the poisoned state from a mutex.
    ///
    /// See [`std::sync::Mutex::clear_poison`].
    ///
    /// If the mutex is poisoned, it will remain poisoned until this function is called. This
    /// allows recovering from a poisoned state and marking that it has recovered.
    #[inline]
    pub fn clear_poison(&self) {
        if SYNC {
            // SAFETY: `SYNC` is true, so the `sync` field is initialized.
            let sync = unsafe { &self.raw.sync };
            sync.clear_poison();
        } else {
            // SAFETY: `SYNC` is false, so the `unsync` field is initialized.
            let unsync = unsafe { &self.raw.unsync };
            unsync.clear_poison();
        }
    }
}

impl<const SYNC: bool, T: ?Sized + Debug> Debug for MaybeSyncMutex<SYNC, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let mut debug = f.debug_struct("MaybeSyncMutex");
        debug.field("SYNC", &SYNC);
        match self.try_lock_ignoring_poison() {
            Ok(guard)            => debug.field("data", &&*guard),
            Err(WouldBlockError) => debug.field("data", &"<locked>"),
        };
        if Self::supports_poisoning() {
            debug.field("poisoned", &self.is_poisoned());
        }
        debug.finish_non_exhaustive()
    }
}

impl<const SYNC: bool, T: Default> Default for MaybeSyncMutex<SYNC, T> {
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<const SYNC: bool, T> From<T> for MaybeSyncMutex<SYNC, T> {
    /// Creates a new mutex in an unlocked state. This is equivalent to [`MaybeSyncMutex::new`].
    #[inline]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}
