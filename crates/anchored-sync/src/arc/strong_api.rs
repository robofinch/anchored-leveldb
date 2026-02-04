#![expect(unsafe_code, reason = "implement unsafe functions in the stable public API of Arc/Rc")]

use core::{
    borrow::Borrow, cmp::Ordering, error::Error, ffi::CStr, mem::MaybeUninit, ops::Deref, pin::Pin,
};
use core::{
    fmt::{Debug, Display, Formatter, Pointer, Result as FmtResult},
    hash::{Hash, Hasher},
};

use alloc::{
    rc::{Rc, Weak as WeakRc},
    sync::{Arc, Weak as WeakArc},
};

#[cfg(feature = "clone-behavior")]
use clone_behavior::{MirroredClone, Speed};

use crate::maybe_sync::MaybeSync;
use super::{MaybeSyncArc, MaybeSyncWeak};


impl<const SYNC: bool, T> MaybeSyncArc<SYNC, T> {
    /// See [`Arc::new`] and [`Rc::new`].
    #[inline]
    #[must_use]
    pub fn new(data: T) -> Self {
        match Self::operations() {
            MaybeSync::Sync(ops) => (ops.from_arc)(Arc::new(data)),
            MaybeSync::Unsync(ops) => (ops.from_arc)(Rc::new(data)),
        }
    }

    /// See [`Arc::new_cyclic`] and [`Rc::new_cyclic`].
    #[inline]
    pub fn new_cyclic<F>(data_fn: F) -> Self
    where
        F: FnOnce(&MaybeSyncWeak<SYNC, T>) -> T,
    {
        match Self::operations() {
            MaybeSync::Sync(ops) => {
                (ops.from_arc)(Arc::new_cyclic(|weak_arc_ref: &WeakArc<T>| {
                    let weak_this_ref = (ops.from_weak_ref)(weak_arc_ref);
                    data_fn(weak_this_ref.weak_ref())
                }))
            }
            MaybeSync::Unsync(ops) => {
                (ops.from_arc)(Rc::new_cyclic(|weak_rc_ref: &WeakRc<T>| {
                    let weak_this_ref = (ops.from_weak_ref)(weak_rc_ref);
                    data_fn(weak_this_ref.weak_ref())
                }))
            }
        }
    }

    /// See [`Arc::new_uninit`] and [`Rc::new_uninit`].
    #[inline]
    #[must_use]
    pub fn new_uninit() -> MaybeSyncArc<SYNC, MaybeUninit<T>> {
        match MaybeSyncArc::operations() {
            MaybeSync::Sync(ops) => (ops.from_arc)(Arc::new_uninit()),
            MaybeSync::Unsync(ops) => (ops.from_arc)(Rc::new_uninit()),
        }
    }

    // TODO: support once MSRV is at least 1.92
    // /// See [`Arc::new_zeroed`] and [`Rc::new_zeroed`].
    // #[inline]
    // #[must_use]
    // pub fn new_zeroed() -> MaybeSyncArc<SYNC, MaybeUninit<T>> {
    //     match MaybeSyncArc::operations() {
    //         MaybeSync::Sync(ops) => (ops.from_arc)(Arc::new_zeroed()),
    //         MaybeSync::Unsync(ops) => (ops.from_arc)(Rc::new_zeroed()),
    //     }
    // }

    /// See [`Arc::pin`] and [`Rc::pin`].
    #[inline]
    #[must_use]
    pub fn pin(data: T) -> Pin<Self> {
        match Self::operations() {
            MaybeSync::Sync(ops) => (ops.from_arc_pin)(Arc::pin(data)),
            MaybeSync::Unsync(ops) => (ops.from_arc_pin)(Rc::pin(data)),
        }
    }

    /// See [`Arc::try_unwrap`] and [`Rc::try_unwrap`].
    #[inline]
    pub fn try_unwrap(this: Self) -> Result<T, Self> {
        match Self::operations() {
            MaybeSync::Sync(ops) => {
                Arc::try_unwrap((ops.into_arc)(this)).map_err(ops.from_arc)
            }
            MaybeSync::Unsync(ops) => {
                Rc::try_unwrap((ops.into_arc)(this)).map_err(ops.from_arc)
            }
        }
    }

    /// See [`Arc::into_inner`] and [`Rc::into_inner`].
    #[inline]
    #[must_use]
    pub fn into_inner(this: Self) -> Option<T> {
        match Self::operations() {
            MaybeSync::Sync(ops) => Arc::into_inner((ops.into_arc)(this)),
            MaybeSync::Unsync(ops) => Rc::into_inner((ops.into_arc)(this)),
        }
    }
}

impl<const SYNC: bool, T> MaybeSyncArc<SYNC, [T]> {
    /// See [`Arc::new_uninit_slice`] and [`Rc::new_uninit_slice`].
    #[inline]
    #[must_use]
    pub fn new_uninit_slice(len: usize) -> MaybeSyncArc<SYNC, [MaybeUninit<T>]> {
        match MaybeSyncArc::operations() {
            MaybeSync::Sync(ops) => (ops.from_arc)(Arc::new_uninit_slice(len)),
            MaybeSync::Unsync(ops) => (ops.from_arc)(Rc::new_uninit_slice(len)),
        }
    }

    // TODO: support once MSRV is at least 1.92
    // /// See [`Arc::new_zeroed_slice`] and [`Rc::new_zeroed_slice`].
    // #[inline]
    // #[must_use]
    // pub fn new_zeroed_slice(len: usize) -> MaybeSyncArc<SYNC, [MaybeUninit<T>]> {
    //     match MaybeSyncArc::operations() {
    //         MaybeSync::Sync(ops) => (ops.from_arc)(Arc::new_zeroed_slice(len)),
    //         MaybeSync::Unsync(ops) => (ops.from_arc)(Rc::new_zeroed_slice(len)),
    //     }
    // }
}

impl<const SYNC: bool, T> MaybeSyncArc<SYNC, MaybeUninit<T>> {
    /// Converts to `MaybeSyncArc<SYNC, T>`.
    ///
    /// This is equivalent to the difficult-to-doclink-to
    /// `Arc::<MaybeUninit<T>>::assume_init` or `Rc::<MaybeUninit<T>>::assume_init`.
    ///
    /// # Safety
    ///
    /// As with [`MaybeUninit::assume_init`],
    /// it is up to the caller to guarantee that the inner value
    /// really is in an initialized state.
    /// Calling this when the content is not yet fully initialized
    /// causes immediate undefined behavior.
    #[inline]
    #[must_use]
    pub unsafe fn assume_init(self) -> MaybeSyncArc<SYNC, T> {
        match Self::operations_pair::<T>() {
            MaybeSync::Sync((ops, init_ops)) => {
                let arc: Arc<MaybeUninit<T>> = (ops.into_arc)(self);
                // SAFETY: initialization is asserted by the caller.
                let init_arc = unsafe { arc.assume_init() };
                (init_ops.from_arc)(init_arc)
            }
            MaybeSync::Unsync((ops, init_ops)) => {
                let arc: Rc<MaybeUninit<T>> = (ops.into_arc)(self);
                // SAFETY: initialization is asserted by the caller.
                let init_arc = unsafe { arc.assume_init() };
                (init_ops.from_arc)(init_arc)
            }
        }
    }
}

impl<const SYNC: bool, T> MaybeSyncArc<SYNC, [MaybeUninit<T>]> {
    /// Converts to `MaybeSyncArc<SYNC, [T]>`.
    ///
    /// This is equivalent to the difficult-to-doclink-to
    /// `Arc::<[MaybeUninit<T>]>::assume_init` or `Rc::<[MaybeUninit<T>]>::assume_init`.
    ///
    /// # Safety
    ///
    /// As with [`MaybeUninit::assume_init`],
    /// it is up to the caller to guarantee that the inner value
    /// really is in an initialized state.
    /// Calling this when the content is not yet fully initialized
    /// causes immediate undefined behavior.
    #[inline]
    #[must_use]
    pub unsafe fn assume_init(self) -> MaybeSyncArc<SYNC, [T]> {
        match Self::operations_pair::<[T]>() {
            MaybeSync::Sync((ops, init_ops)) => {
                let arc: Arc<[MaybeUninit<T>]> = (ops.into_arc)(self);
                // SAFETY: initialization is asserted by the caller.
                let init_arc = unsafe { arc.assume_init() };
                (init_ops.from_arc)(init_arc)
            }
            MaybeSync::Unsync((ops, init_ops)) => {
                let arc: Rc<[MaybeUninit<T>]> = (ops.into_arc)(self);
                // SAFETY: initialization is asserted by the caller.
                let init_arc = unsafe { arc.assume_init() };
                (init_ops.from_arc)(init_arc)
            }
        }
    }
}

impl<const SYNC: bool, T: ?Sized> MaybeSyncArc<SYNC, T> {
    /// See [`Arc::from_raw`] and [`Rc::from_raw`].
    ///
    /// # Safety
    /// If `SYNC` is true, then the safety requirements of [`Arc::from_raw`] must be met.
    /// Otherwise, `SYNC` is false, and the safety requirements of [`Rc::from_raw`] must be met.
    #[inline]
    #[must_use]
    pub unsafe fn from_raw(ptr: *const T) -> Self {
        match Self::operations() {
            MaybeSync::Sync(ops) => {
                // SAFETY: `SYNC` is true in this branch, so the caller asserts this is sound.
                let arc = unsafe { Arc::from_raw(ptr) };
                (ops.from_arc)(arc)
            }
            MaybeSync::Unsync(ops) => {
                // SAFETY: `SYNC` is false in this branch, so the caller asserts this is sound.
                let rc = unsafe { Rc::from_raw(ptr) };
                (ops.from_arc)(rc)
            }
        }
    }

    /// See [`Arc::into_raw`] and [`Rc::into_raw`].
    #[must_use = "losing the pointer will leak memory"]
    pub fn into_raw(this: Self) -> *const T {
        match Self::operations() {
            MaybeSync::Sync(ops) => Arc::into_raw((ops.into_arc)(this)),
            MaybeSync::Unsync(ops) => Rc::into_raw((ops.into_arc)(this)),
        }
    }

    /// See [`Arc::increment_strong_count`] and [`Rc::increment_strong_count`].
    ///
    /// # Safety
    /// If `SYNC` is true, then the safety requirements of [`Arc::increment_strong_count`] must be
    /// met. Otherwise, `SYNC` is false, and the safety requirements of
    /// [`Rc::increment_strong_count`] must be met.
    #[inline]
    pub unsafe fn increment_strong_count(ptr: *const T) {
        if SYNC {
            // SAFETY: `SYNC` is true in this branch, so the caller asserts this is sound.
            unsafe {
                Arc::increment_strong_count(ptr);
            }
        } else {
            // SAFETY: `SYNC` is false in this branch, so the caller asserts this is sound.
            unsafe {
                Rc::increment_strong_count(ptr);
            }
        }
    }

    /// See [`Arc::decrement_strong_count`] and [`Rc::decrement_strong_count`].
    ///
    /// # Safety
    /// If `SYNC` is true, then the safety requirements of [`Arc::decrement_strong_count`] must be
    /// met. Otherwise, `SYNC` is false, and the safety requirements of
    /// [`Rc::decrement_strong_count`] must be met.
    #[inline]
    pub unsafe fn decrement_strong_count(ptr: *const T) {
        if SYNC {
            // SAFETY: `SYNC` is true in this branch, so the caller asserts this is sound.
            unsafe {
                Arc::decrement_strong_count(ptr);
            }
        } else {
            // SAFETY: `SYNC` is false in this branch, so the caller asserts this is sound.
            unsafe {
                Rc::decrement_strong_count(ptr);
            }
        }
    }

    /// See [`Arc::as_ptr`] and [`Rc::as_ptr`].
    #[inline]
    #[must_use]
    pub fn as_ptr(this: &Self) -> *const T {
        match Self::operations() {
            MaybeSync::Sync(ops) => Arc::as_ptr((ops.as_arc_ref)(this)),
            MaybeSync::Unsync(ops) => Rc::as_ptr((ops.as_arc_ref)(this)),
        }
    }

    /// See [`Arc::downgrade`] and [`Rc::downgrade`].
    #[inline]
    #[must_use]
    pub fn downgrade(this: &Self) -> MaybeSyncWeak<SYNC, T> {
        match Self::operations() {
            MaybeSync::Sync(ops) => (ops.from_weak)(Arc::downgrade((ops.as_arc_ref)(this))),
            MaybeSync::Unsync(ops) => (ops.from_weak)(Rc::downgrade((ops.as_arc_ref)(this))),
        }
    }

    /// See [`Arc::weak_count`] and [`Rc::weak_count`].
    #[inline]
    #[must_use]
    pub fn weak_count(this: &Self) -> usize {
        match Self::operations() {
            MaybeSync::Sync(ops) => Arc::weak_count((ops.as_arc_ref)(this)),
            MaybeSync::Unsync(ops) => Rc::weak_count((ops.as_arc_ref)(this)),
        }
    }

    /// See [`Arc::strong_count`] and [`Rc::strong_count`].
    #[inline]
    #[must_use]
    pub fn strong_count(this: &Self) -> usize {
        match Self::operations() {
            MaybeSync::Sync(ops) => Arc::strong_count((ops.as_arc_ref)(this)),
            MaybeSync::Unsync(ops) => Rc::strong_count((ops.as_arc_ref)(this)),
        }
    }

    /// See [`Arc::ptr_eq`] and [`Rc::ptr_eq`].
    #[inline]
    #[must_use]
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        match Self::operations() {
            MaybeSync::Sync(ops) => Arc::ptr_eq((ops.as_arc_ref)(this), (ops.as_arc_ref)(other)),
            MaybeSync::Unsync(ops) => Rc::ptr_eq((ops.as_arc_ref)(this), (ops.as_arc_ref)(other)),
        }
    }

    /// See [`Arc::get_mut`] and [`Rc::get_mut`].
    #[inline]
    #[must_use]
    pub fn get_mut(this: &mut Self) -> Option<&mut T> {
        match Self::operations() {
            MaybeSync::Sync(ops) => Arc::get_mut((ops.as_arc_mut)(this)),
            MaybeSync::Unsync(ops) => Rc::get_mut((ops.as_arc_mut)(this)),
        }
    }
}

impl<const SYNC: bool, T: Clone> MaybeSyncArc<SYNC, T> {
    /// See [`Arc::make_mut`] and [`Rc::make_mut`].
    #[inline]
    #[must_use]
    pub fn make_mut(this: &mut Self) -> &mut T {
        match Self::operations() {
            MaybeSync::Sync(ops) => Arc::make_mut((ops.as_arc_mut)(this)),
            MaybeSync::Unsync(ops) => Rc::make_mut((ops.as_arc_mut)(this)),
        }
    }

    /// See [`Arc::unwrap_or_clone`] and [`Rc::unwrap_or_clone`].
    #[inline]
    #[must_use]
    pub fn unwrap_or_clone(this: Self) -> T {
        match Self::operations() {
            MaybeSync::Sync(ops) => Arc::unwrap_or_clone((ops.into_arc)(this)),
            MaybeSync::Unsync(ops) => Rc::unwrap_or_clone((ops.into_arc)(this)),
        }
    }
}

// Borrowing traits

impl<const SYNC: bool, T: ?Sized> Deref for MaybeSyncArc<SYNC, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        match Self::operations() {
            MaybeSync::Sync(ops) => (ops.as_arc_ref)(self),
            MaybeSync::Unsync(ops) => (ops.as_arc_ref)(self),
        }
    }
}

impl<const SYNC: bool, T: ?Sized> AsRef<T> for MaybeSyncArc<SYNC, T> {
    #[inline]
    fn as_ref(&self) -> &T {
        self
    }
}

impl<const SYNC: bool, T: ?Sized> Borrow<T> for MaybeSyncArc<SYNC, T> {
    #[inline]
    fn borrow(&self) -> &T {
        self
    }
}

// Cloning traits

impl<const SYNC: bool, T: ?Sized> Clone for MaybeSyncArc<SYNC, T> {
    #[inline]
    fn clone(&self) -> Self {
        match Self::operations() {
            MaybeSync::Sync(ops) => (ops.from_arc)(Arc::clone((ops.as_arc_ref)(self))),
            MaybeSync::Unsync(ops) => (ops.from_arc)(Rc::clone((ops.as_arc_ref)(self))),
        }
    }
}

#[cfg(feature = "clone-behavior")]
impl<const SYNC: bool, T: ?Sized, S: Speed> MirroredClone<S> for MaybeSyncArc<SYNC, T> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        self.clone()
    }
}

// Construction traits

impl<const SYNC: bool, T> Default for MaybeSyncArc<SYNC, [T]> {
    #[inline]
    fn default() -> Self {
        match Self::operations() {
            MaybeSync::Sync(ops) => (ops.from_arc)(Arc::default()),
            MaybeSync::Unsync(ops) => (ops.from_arc)(Rc::default()),
        }
    }
}

impl<const SYNC: bool> Default for MaybeSyncArc<SYNC, CStr> {
    #[inline]
    fn default() -> Self {
        match Self::operations() {
            MaybeSync::Sync(ops) => (ops.from_arc)(Arc::default()),
            MaybeSync::Unsync(ops) => (ops.from_arc)(Rc::default()),
        }
    }
}

impl<const SYNC: bool, T: Default> Default for MaybeSyncArc<SYNC, T> {
    #[inline]
    fn default() -> Self {
        match Self::operations() {
            MaybeSync::Sync(ops) => (ops.from_arc)(Arc::default()),
            MaybeSync::Unsync(ops) => (ops.from_arc)(Rc::default()),
        }
    }
}

impl<const SYNC: bool> Default for MaybeSyncArc<SYNC, str> {
    #[inline]
    fn default() -> Self {
        match Self::operations() {
            MaybeSync::Sync(ops) => (ops.from_arc)(Arc::default()),
            MaybeSync::Unsync(ops) => (ops.from_arc)(Rc::default()),
        }
    }
}

impl<const SYNC: bool, T: ?Sized> Default for Pin<MaybeSyncArc<SYNC, T>>
where
    MaybeSyncArc<SYNC, T>: Default,
{
    #[inline]
    fn default() -> Self {
        #[expect(clippy::use_self, reason = "writing `Pin` is more clear than using `Self`")]
        // SAFETY: same as the implementation of `Default` for `Pin<Arc<T>>` and `Pin<Rc<T>>`.
        // In particular, `MaybeSyncArc` respects the pinning invariants in the same way
        // that `Arc` and `Rc` do.
        unsafe { Pin::new_unchecked(MaybeSyncArc::<SYNC, T>::default()) }
    }
}

// Formatting traits

impl<const SYNC: bool, T: ?Sized + Debug> Debug for MaybeSyncArc<SYNC, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Debug::fmt(&**self, f)
    }
}

impl<const SYNC: bool, T: ?Sized + Display> Display for MaybeSyncArc<SYNC, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&**self, f)
    }
}

impl<const SYNC: bool, T: ?Sized> Pointer for MaybeSyncArc<SYNC, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Pointer::fmt(&(&raw const **self), f)
    }
}

impl<const SYNC: bool, T: ?Sized + Error> Error for MaybeSyncArc<SYNC, T> {
    fn cause(&self) -> Option<&dyn Error> {
        #[expect(deprecated, reason = "forward impl to inner type, just like Rc/Arc")]
        T::cause(self)
    }

    fn description(&self) -> &str {
        #[expect(deprecated, reason = "forward impl to inner type, just like Rc/Arc")]
        T::description(self)
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        T::source(self)
    }
}

// Comparison traits

impl<const SYNC: bool, T: ?Sized + PartialEq> PartialEq for MaybeSyncArc<SYNC, T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        T::eq(self, other)
    }
}

impl<const SYNC: bool, T: ?Sized + Eq> Eq for MaybeSyncArc<SYNC, T> {}

impl<const SYNC: bool, T: ?Sized + PartialOrd> PartialOrd for MaybeSyncArc<SYNC, T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        T::partial_cmp(self, other)
    }
}

impl<const SYNC: bool, T: ?Sized + Ord> Ord for MaybeSyncArc<SYNC, T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        T::cmp(self, other)
    }
}

// Miscellaneous

impl<const SYNC: bool, T: ?Sized + Hash> Hash for MaybeSyncArc<SYNC, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        T::hash(self, state);
    }
}
