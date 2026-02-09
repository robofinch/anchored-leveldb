#![expect(unsafe_code, reason = "needed to read union fields and associate a guard with its lock")]

use core::{cell::UnsafeCell, mem::ManuallyDrop, ptr};
use core::fmt::{Debug, Formatter, Result as FmtResult};

use crate::would_block_error::WouldBlockError;
use crate::rwlock::{
    MaybeSyncRawRwLock, MaybeSyncReadGuard, MaybeSyncRwLock, MaybeSyncWriteGuard, POISON_ERROR_MSG, RawCellRwLock, RawRwLock
};


impl<const SYNC: bool, T> MaybeSyncRwLock<SYNC, T> {
    /// Creates a new `MaybeSyncRwLock` in an unlocked state.
    #[inline]
    #[must_use]
    pub const fn new(value: T) -> Self {
        if SYNC {
            Self {
                raw: MaybeSyncRawRwLock {
                    // Safety invariant: `SYNC` is true, and we initialize the `sync` field.
                    sync: ManuallyDrop::new(RawRwLock::new()),
                },
                data: UnsafeCell::new(value),
            }
        } else {
            Self {
                raw: MaybeSyncRawRwLock {
                    // Safety invariant: `SYNC` is false, and we initialize the `unsync` field.
                    unsync: ManuallyDrop::new(RawCellRwLock::new()),
                },
                data: UnsafeCell::new(value),
            }
        }
    }

    /// Consumes this `MaybeSyncRwLock`, returning the underlying data.
    ///
    /// # Panics
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function panics if the lock is currently poisoned.
    ///
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> T {
        assert!(!self.is_poisoned(), "{POISON_ERROR_MSG}");

        self.into_inner_ignoring_poison()
    }

    /// Consumes this `MaybeSyncRwLock`, returning the underlying data.
    ///
    /// # Ignoring poison
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function ignores the poisoned state of the lock.
    ///
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    #[inline]
    #[must_use]
    pub fn into_inner_ignoring_poison(self) -> T {
        let mut this = ManuallyDrop::new(self);

        // SAFETY: We do not use `this.raw` after calling this function (not even by moving
        // it or `this`), and since we have exclusive ownership over `self`, nobody else will
        // be able to access `this.raw` either.
        unsafe {
            this.drop_raw_rwlock();
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

impl<const SYNC: bool, T: ?Sized> MaybeSyncRwLock<SYNC, T> {
    /// Locks this read-write lock with shared read access, blocking the current thread until it
    /// is able to do so.
    ///
    /// The calling thread will be blocked until there are no more writers which hold the lock.
    /// There may be other readers currently holding the lock when this method returns. This method
    /// does not provide any guarantees with respect to the ordering of whether contentious readers
    /// or writers will acquire the lock first.
    ///
    /// When the returned RAII guard is dropped, the shared access acquired by this function will
    /// be released.
    ///
    /// # Panics, Aborts, and Deadlocks
    /// If the current thread has write access over this `MaybeSyncRwLock`, then this function
    /// will panic, abort, deadlock, or similar; in any case, it won't return normally.
    ///
    /// **If `SYNC` is true and the current thread already has read access over this
    /// `MaybeSyncRwLock`, then even though multiple readers are permitted to exist, this function
    /// *may* panic, abort, deadlock, or similar.** (For instance, a different thread may be
    /// attempting to acquire a write lock, and in order to avoid starving writers, the
    /// `MaybeSyncRwLock` may choose to block new attempts to acquire read locks until after the
    /// write lock is acquired.)
    ///
    /// Additionally, the number of read locks of a `MaybeSyncRwLock` which may be held at the same
    /// time is large but finite. If the maximum number of read locks had already been reached
    /// (across all threads), possibly by leaking read guards with [`forget()`] in a loop, then
    /// this function will likewise panic, abort, deadlock, or similar.
    ///
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function panics if the lock is currently poisoned.
    ///
    /// [`forget()`]: core::mem::forget
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    pub fn read(&self) -> MaybeSyncReadGuard<'_, SYNC, T> {
        // SAFETY: `RawRwLock::read` and `RawCellRwLock::read` obtain a guard associated with the
        // input lock on the current thread.
        unsafe { self.read_fn(RawRwLock::read, RawCellRwLock::read) }
    }

    /// Locks this read-write lock with shared read access, blocking the current thread until it
    /// is able to do so.
    ///
    /// The calling thread will be blocked until there are no more writers which hold the lock.
    /// There may be other readers currently holding the lock when this method returns. This method
    /// does not provide any guarantees with respect to the ordering of whether contentious readers
    /// or writers will acquire the lock first.
    ///
    /// When the returned RAII guard is dropped, the shared access acquired by this function will
    /// be released.
    ///
    /// # Ignoring poison
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function ignores the poisoned state of the lock,
    /// though does not clear the poisoned state.
    ///
    /// # Panics, Aborts, and Deadlocks
    /// If the current thread has already write-locked this `MaybeSyncRwLock`, then this function
    /// will panic, abort, deadlock, or similar; in any case, it won't return normally.
    ///
    /// **If `SYNC` is true and the current thread already has read access over this
    /// `MaybeSyncRwLock`, then even though multiple readers are permitted to exist, this function
    /// *may* panic, abort, deadlock, or similar.** (For instance, a different thread may be
    /// attempting to acquire a write lock, and in order to avoid starving writers, the
    /// `MaybeSyncRwLock` may choose to block new attempts to acquire read locks until after the
    /// write lock is acquired.)
    ///
    /// Additionally, the number of read locks of a `MaybeSyncRwLock` which may be held at the same
    /// time is large but finite. If the maximum number of read locks had already been reached
    /// (across all threads), possibly by leaking read guards with [`forget()`] in a loop, then
    /// this function will likewise panic, abort, deadlock, or similar.
    ///
    /// [`forget()`]: core::mem::forget
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    pub fn read_ignoring_poison(&self) -> MaybeSyncReadGuard<'_, SYNC, T> {
        // SAFETY: `RawRwLock::read_ignoring_poison` and `RawCellRwLock::read_ignoring_poison`
        // obtain a guard associated with the input lock on the current thread.
        unsafe {
            self.read_fn(
                RawRwLock::read_ignoring_poison,
                RawCellRwLock::read_ignoring_poison,
            )
        }
    }

    /// Attempts to acquire this `MaybeSyncRwLock` with shared read access.
    ///
    /// If the lock could not be acquired at this time, then `Err` is returned. Otherwise, an RAII
    /// guard is returned. The shared access acquired by this function will be unlocked when the
    /// guard is dropped.
    ///
    /// This function does not block.
    ///
    /// This function does not provide any guarantees with respect to the ordering of whether
    /// contentious readers or writers will acquire the lock first.
    ///
    /// # Errors
    /// This call returns a [`WouldBlockError`] if the lock is currently held exclusively or the
    /// maximum number of concurrent read locks has been reached. This includes if a write lock is
    /// already held by the current thread. A [`WouldBlockError`] may also be returned if a
    /// different thread is currently contending for a write lock.
    ///
    /// Note that the number of read locks of a `MaybeSyncRwLock` which may be held at the same
    /// time is large but finite, and may (on some target platforms) be reached by leaking read
    /// guards with [`forget()`] in a loop.
    ///
    /// # Panics
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function panics if the lock is currently poisoned.
    ///
    /// [`forget()`]: core::mem::forget
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    pub fn try_read(&self) -> Result<MaybeSyncReadGuard<'_, SYNC, T>, WouldBlockError> {
        // SAFETY: `RawRwLock::try_read` and `RawCellRwLock::try_read` obtain a guard
        // associated with the input lock on the current thread (if they are successful).
        unsafe { self.try_read_fn(RawRwLock::try_read, RawCellRwLock::try_read) }
    }

    /// Attempts to acquire this `MaybeSyncRwLock` with shared read access.
    ///
    /// If the lock could not be acquired at this time, then `Err` is returned. Otherwise, an RAII
    /// guard is returned. The shared access acquired by this function will be unlocked when the
    /// guard is dropped.
    ///
    /// This function does not block.
    ///
    /// This function does not provide any guarantees with respect to the ordering of whether
    /// contentious readers or writers will acquire the lock first.
    ///
    /// # Ignoring poison
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function ignores the poisoned state of the lock,
    /// though does not clear the poisoned state.
    ///
    /// # Errors
    /// This call returns a [`WouldBlockError`] if the lock is currently held exclusively or the
    /// maximum number of concurrent read locks has been reached. This includes if a write lock is
    /// already held by the current thread. A [`WouldBlockError`] may also be returned if a
    /// different thread is currently contending for a write lock.
    ///
    /// Note that the number of read locks of a `MaybeSyncRwLock` which may be held at the same
    /// time is large but finite, and may (for some target platforms) be reached by leaking read
    /// guards with [`forget()`] in a loop.
    ///
    /// [`forget()`]: core::mem::forget
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    pub fn try_read_ignoring_poison(
        &self,
    ) -> Result<MaybeSyncReadGuard<'_, SYNC, T>, WouldBlockError> {
        // SAFETY: `RawRwLock::try_read_ignoring_poison` and
        // `RawCellRwLock::try_read_ignoring_poison` obtain a guard associated with the input lock
        // on the current thread (if they are successful).
        unsafe {
            self.try_read_fn(
                RawRwLock::try_read_ignoring_poison,
                RawCellRwLock::try_read_ignoring_poison,
            )
        }
    }

    /// Locks this read-write lock with exclusive write access, blocking the current thread until it
    /// is able to do so.
    ///
    /// This function will not return while other writers or readers have access to the lock.
    ///
    /// When the returned RAII guard is dropped, the write access is released.
    ///
    /// # Panics, Aborts, and Deadlocks
    /// If the current thread already holds this `MaybeSyncRwLock` (with either read or write
    /// access), then this function will panic, abort, deadlock, or similar; in any case, it won't
    /// return normally.
    ///
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function panics if the lock is currently poisoned.
    ///
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    pub fn write(&self) -> MaybeSyncWriteGuard<'_, SYNC, T> {
        // SAFETY: `RawRwLock::rewritead` and `RawCellRwLock::write` obtain a guard associated with the
        // input lock on the current thread.
        unsafe { self.write_fn(RawRwLock::write, RawCellRwLock::write) }
    }

    /// Locks this read-write lock with exclusive write access, blocking the current thread until it
    /// is able to do so.
    ///
    /// This function will not return while other writers or readers have access to the lock.
    ///
    /// When the returned RAII guard is dropped, the write access is released.
    ///
    /// # Ignoring poison
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function ignores the poisoned state of the lock,
    /// though does not clear the poisoned state.
    ///
    /// # Panics, Aborts, and Deadlocks
    /// If the current thread already holds this `MaybeSyncRwLock` (with either read or write
    /// access), then this function will panic, abort, deadlock, or similar; in any case, it won't
    /// return normally.
    ///
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    pub fn write_ignoring_poison(&self) -> MaybeSyncWriteGuard<'_, SYNC, T> {
        // SAFETY: `RawRwLock::write_ignoring_poison` and `RawCellRwLock::write_ignoring_poison`
        // obtain a guard associated with the input lock on the current thread.
        unsafe {
            self.write_fn(
                RawRwLock::write_ignoring_poison,
                RawCellRwLock::write_ignoring_poison,
            )
        }
    }

    /// Attempts to acquire this `MaybeSyncRwLock` with exclusive write access.
    ///
    /// If the lock could not be acquired at this time, then `Err` is returned. Otherwise, an RAII
    /// guard is returned which will release the lock when it is dropped.
    ///
    /// This function does not block.
    ///
    /// This function does not provide any guarantees with respect to the ordering of whether
    /// contentious readers or writers will acquire the lock first.
    ///
    /// # Errors
    /// This call returns a [`WouldBlockError`] if the lock is currently held (whether with shared
    /// or exclusive access). This includes if the lock is already held by the current thread.
    ///
    /// # Panics
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function panics if the lock is currently poisoned.
    ///
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    pub fn try_write(&self) -> Result<MaybeSyncWriteGuard<'_, SYNC, T>, WouldBlockError> {
        // SAFETY: `RawRwLock::try_write` and `RawCellRwLock::try_write` obtain a guard
        // associated with the input lock on the current thread (if they are successful).
        unsafe { self.try_write_fn(RawRwLock::try_write, RawCellRwLock::try_write) }
    }

    /// Attempts to acquire this `MaybeSyncRwLock` with exclusive write access.
    ///
    /// If the lock could not be acquired at this time, then `Err` is returned. Otherwise, an RAII
    /// guard is returned which will release the lock when it is dropped.
    ///
    /// This function does not block.
    ///
    /// This function does not provide any guarantees with respect to the ordering of whether
    /// contentious readers or writers will acquire the lock first.
    ///
    /// # Ignoring poison
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function ignores the poisoned state of the lock,
    /// though does not clear the poisoned state.
    ///
    /// # Errors
    /// This call returns a [`WouldBlockError`] if the lock is currently held (whether with shared
    /// or exclusive access). This includes if the lock is already held by the current thread.
    ///
    /// [`forget()`]: core::mem::forget
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    pub fn try_write_ignoring_poison(
        &self,
    ) -> Result<MaybeSyncWriteGuard<'_, SYNC, T>, WouldBlockError> {
        // SAFETY: `RawRwLock::try_write_ignoring_poison` and
        // `RawCellRwLock::try_write_ignoring_poison` obtain a guard associated with the input lock
        // on the current thread (if they are successful).
        unsafe {
            self.try_write_fn(
                RawRwLock::try_write_ignoring_poison,
                RawCellRwLock::try_write_ignoring_poison,
            )
        }
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the lock mutably, no actual locking needs to take place – the
    /// mutable borrow statically guarantees no new locks can be acquired while this reference
    /// exists. Note that this method does not clear any previous abandoned locks
    /// (e.g., via [`forget()`] on a [`MaybeSyncWriteGuard`]).
    ///
    /// # Panics
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function panics if the lock is currently poisoned.
    ///
    /// [`forget()`]: core::mem::forget
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    #[inline]
    #[must_use]
    pub fn get_mut(&mut self) -> &mut T {
        assert!(!self.is_poisoned(), "{POISON_ERROR_MSG}");

        self.get_mut_ignoring_poison()
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the lock mutably, no actual locking needs to take place – the
    /// mutable borrow statically guarantees no new locks can be acquired while this reference
    /// exists. Note that this method does not clear any previous abandoned locks
    /// (e.g., via [`forget()`] on a [`MaybeSyncWriteGuard`]).
    ///
    /// # Ignoring poison
    /// [If this lock supports poisoning] and a thread panicked while holding a write lock, the
    /// `MaybeSyncRwLock` becomes poisoned. This function ignores the poisoned state of the lock.
    ///
    /// [`forget()`]: core::mem::forget
    /// [If this lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    #[inline]
    #[must_use]
    pub const fn get_mut_ignoring_poison(&mut self) -> &mut T {
        self.data.get_mut()
    }

    /// Check whether the lock supports poisoning.
    ///
    /// This `MaybeSyncRwLock` supports poisoning if the `parking_lot` feature is **not** enabled
    /// and `SYNC` is `true`.
    ///
    /// If poisoning is not supported, the lock will never become poisoned.
    ///
    /// If poisoning is supported, the lock becomes poisoned if a [`MaybeSyncWriteGuard`] is
    /// dropped on a panicking thread. Note in particular that dropping a read lock while
    /// panicking does *not* poison the `MaybeSyncRwLock`.
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

    /// Determines whether the lock is poisoned.
    ///
    /// See [`std::sync::RwLock::is_poisoned`].
    ///
    /// The lock can become poisoned at any time that a write lock might be held by a different
    /// thread ([if the lock supports poisoning]). You should not trust a `false` value for program
    /// correctness without additional synchronization.
    ///
    /// In particular, the return value is reliable if the thread calling this ownership has
    /// exclusive access to the `MaybeSyncRwLock` (relevant for checking for poison before calling
    /// [`into_inner_ignoring_poison`] or [`get_mut_ignoring_poison`]) or if either a read or write
    /// lock is held by the thread calling this method (relevant for checking for poison after
    /// calling [`read_ignoring_poison`] or similar).
    ///
    /// Therefore, even though this lock does not have methods that return
    /// `Result<_, PoisonError<_>>` or similar, the support for poisoning provided by
    /// `MaybeSyncRwLock` (when supported at all) is not less powerful than that of
    /// [`std::sync::RwLock`].
    ///
    /// [if the lock supports poisoning]: MaybeSyncRwLock::supports_poisoning
    /// [`into_inner_ignoring_poison`]: MaybeSyncRwLock::into_inner_ignoring_poison
    /// [`read_ignoring_poison`]: MaybeSyncRwLock::read_ignoring_poison
    /// [`get_mut_ignoring_poison`]: MaybeSyncRwLock::get_mut_ignoring_poison
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

    /// Clear the poisoned state from a read-write lock.
    ///
    /// See [`std::sync::RwLock::clear_poison`].
    ///
    /// If the lock is poisoned, it will remain poisoned until this function is called. This
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

impl<const SYNC: bool, T: ?Sized + Debug> Debug for MaybeSyncRwLock<SYNC, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let mut debug = f.debug_struct("MaybeSyncRwLock");
        debug.field("SYNC", &SYNC);
        match self.try_read_ignoring_poison() {
            Ok(guard)            => debug.field("data", &&*guard),
            Err(WouldBlockError) => debug.field("data", &"<locked>"),
        };
        if Self::supports_poisoning() {
            debug.field("poisoned", &self.is_poisoned());
        }
        debug.finish_non_exhaustive()
    }
}

impl<const SYNC: bool, T: Default> Default for MaybeSyncRwLock<SYNC, T> {
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<const SYNC: bool, T> From<T> for MaybeSyncRwLock<SYNC, T> {
    /// Creates a new `MaybeSyncRwLock` in an unlocked state. This is equivalent to
    /// [`MaybeSyncRwLock::new`].
    #[inline]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}
