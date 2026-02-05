#![expect(
    unsafe_code,
    reason = "needed to read union fields and consume Drop-impl'ing wrappers into inner fields",
)]

use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::ptr;
use core::{hint::unreachable_unchecked, mem::ManuallyDrop, pin::Pin};

use alloc::{
    rc::{Rc, Weak as WeakRc},
    sync::{Arc, Weak as WeakArc},
};

use crate::maybe_sync::MaybeSync;
use super::{MaybeSyncArc, MaybeSyncArcInner, MaybeSyncWeak, MaybeSyncWeakInner};


// Brief table of contents:
// - utilities
// - the workhorse `unsafe` code
// - exposed API

// ================================================================
//  Utilities
// ================================================================

/// The operations that can be performed on a `MaybeSyncArc<SYNC, _>` or `MaybeSyncWeak<SYNC, _>`.
///
/// The choice of functions is determined by `SYNC`.
pub(super) struct Operations<F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11> {
    pub from_arc:      F1,
    pub into_arc:      F2,
    pub as_arc_ref:    F3,
    pub as_arc_mut:    F4,
    pub from_arc_pin:  F5,
    pub into_arc_pin:  F6,
    pub from_weak:     F7,
    pub into_weak:     F8,
    pub as_weak_ref:   F9,
    pub as_weak_mut:   F10,
    pub from_weak_ref: F11,
}

/// A `&'a Weak` cannot soundly be converted to a `&'a MaybeSyncWeak<_, T>`, as their
/// layouts are not guaranteed to match. Instead, a `&'a Weak` can be converted to a
/// `MaybeSyncWeakRef<'a, _, T>` (for appropriate choices of `WeakArc`/`WeakRc` and `SYNC`).
#[repr(transparent)]
pub(super) struct MaybeSyncWeakRef<'a, const SYNC: bool, T: ?Sized> {
    /// This field must never be dropped.
    weak: ManuallyDrop<MaybeSyncWeak<SYNC, T>>,
    lt:   PhantomData<&'a MaybeSyncWeak<SYNC, T>>,
}

macro_rules! operations {
    ($Strong:ty, $Weak:ty, $WeakRef:ty, $Arc:ty, $WeakArc:ty $(,)?) => {
        Operations<
            // Strong conversions
            impl Fn($Arc)         -> $Strong,
            impl Fn($Strong)      -> $Arc,
            impl Fn(&$Strong)     -> &$Arc,
            impl Fn(&mut $Strong) -> &mut $Arc,
            impl Fn(Pin<$Arc>)    -> Pin<$Strong>,
            impl Fn(Pin<$Strong>) -> Pin<$Arc>,
            // Weak conversions
            impl Fn($WeakArc)     -> $Weak,
            impl Fn($Weak)        -> $WeakArc,
            impl Fn(&$Weak)       -> &$WeakArc,
            impl Fn(&mut $Weak)   -> &mut $WeakArc,
            impl Fn(&$WeakArc)    -> $WeakRef,
        >
    };
}

macro_rules! arc_operations {
    // This branch is not strictly necessary; they could be unified by doing `{$SYNC}` below.
    // However, if we do that, rust-analyzer has a meltdown.
    (true, $T:ident) => {
        operations!(
            MaybeSyncArc<true, $T>, MaybeSyncWeak<true, $T>, MaybeSyncWeakRef<'_, true, $T>,
            Arc<$T>, WeakArc<$T>,
        )
    };

    ($SYNC:ident, $T:ident) => {
        operations!(
            MaybeSyncArc<$SYNC, $T>, MaybeSyncWeak<$SYNC, $T>, MaybeSyncWeakRef<'_, $SYNC, $T>,
            Arc<$T>, WeakArc<$T>,
        )
    };
}

macro_rules! rc_operations {
    // This branch is not strictly necessary, but is included for the same reason as above.
    (false, $T:ident) => {
        operations!(
            MaybeSyncArc<false, $T>, MaybeSyncWeak<false, $T>,
            MaybeSyncWeakRef<'_, false, $T>,
            Rc<$T>, WeakRc<$T>,
        )
    };

    ($SYNC:ident, $T:ident) => {
        operations!(
            MaybeSyncArc<$SYNC, $T>, MaybeSyncWeak<$SYNC, $T>, MaybeSyncWeakRef<'_, $SYNC, $T>,
            Rc<$T>, WeakRc<$T>,
        )
    };
}

macro_rules! maybe_sync_operations {
    ($SYNC:ident, $T:ident) => {
        MaybeSync<arc_operations!($SYNC, $T), rc_operations!($SYNC, $T)>
    };

    ($SYNC:ident, $T:ident, $U:ident) => {
        MaybeSync<
            (arc_operations!($SYNC, $T), arc_operations!($SYNC, $U)),
            (rc_operations!($SYNC, $T), rc_operations!($SYNC, $U)),
        >
    };
}

/// Coerce a closure into satisfying a higher-ranked bound.
#[inline]
#[must_use]
const fn coerce_ref_fn<I, O, F: for<'a> Fn(&'a I) -> &'a O>(func: F) -> F {
    func
}

/// Coerce a closure into satisfying a higher-ranked bound.
#[inline]
#[must_use]
const fn coerce_mut_fn<I, O, F: for<'a> Fn(&'a mut I) -> &'a mut O>(func: F) -> F {
    func
}

/// Coerce a closure into satisfying a higher-ranked bound.
#[inline]
#[must_use]
const fn coerce_weak_ref_fn<const SYNC: bool, T, I, F>(func: F) -> F
where
    T: ?Sized,
    F: for<'a> Fn(&'a I) -> MaybeSyncWeakRef<'a, SYNC, T>,
{
    func
}

// ================================================================
//  `unsafe` code
// ================================================================

impl<const SYNC: bool, T: ?Sized> MaybeSyncArc<SYNC, T> {
    /// # Safety
    /// `SYNC` must be `true`.
    #[inline]
    #[must_use]
    unsafe fn asserted_sync_operations() -> arc_operations!(SYNC, T) {
        Operations {
            from_arc: |arc: Arc<T>| {
                Self {
                    inner: MaybeSyncArcInner {
                        // Safety invariant: `SYNC` is true, and we initialize the `sync` field.
                        sync: ManuallyDrop::new(arc),
                    },
                }
            },
            into_arc: |this: Self| {
                let this = ManuallyDrop::new(this);
                // SAFETY:
                // - `&raw const this.inner` is valid for reads (we own the pointee, so we
                //   can guarantee that there's nothing aliasing this read, we know that it
                //   has sufficient for a read, the pointee is in a single allocation, etc).
                // - `&raw const this.inner` is properly-aligned (`Self` is not `repr(packed)`)
                // - `this.inner` is a properly initialized value of type `MaybeSyncArcInner<U>`
                //   for some `U` which is a subtype of `T`
                //   (noting the potential for covariant casts)
                //
                // Note that this cannot cause a double-drop of `this.inner`, since `this`
                // is wrapped in a `ManuallyDrop`.
                let inner = unsafe { ptr::read(&raw const this.inner) };

                // SAFETY: since `SYNC` is true, the `sync` field is initialized
                let sync = unsafe { inner.sync };
                ManuallyDrop::into_inner(sync)
            },
            as_arc_ref: coerce_ref_fn(|this: &Self| {
                // SAFETY: since `SYNC` is true, the `sync` field is initialized
                let sync = unsafe { &this.inner.sync };
                &**sync
            }),
            as_arc_mut: coerce_mut_fn(|this: &mut Self| {
                // SAFETY: since `SYNC` is true, the `sync` field is initialized
                let sync = unsafe { &mut this.inner.sync };
                &mut **sync
            }),
            from_arc_pin: |arc: Pin<Arc<T>>| {
                // SAFETY: `Pin<MaybeSyncArc<_, T>>` treats its pointee as pinned,
                // just like `Arc<T>` and `Rc<T>` do. Since we do not move out of the pointee in
                // the body of this function (we just move the wrapping `Arc`) and pin `this`
                // before exposing it to arbitrary user code, we uphold the pinning invariants.
                let arc: Arc<T> = unsafe { Pin::into_inner_unchecked(arc) };

                let this = Self {
                    inner: MaybeSyncArcInner {
                        // Safety invariant: `SYNC` is true, and we initialize the `sync` field.
                        sync: ManuallyDrop::new(arc),
                    },
                };

                // SAFETY: `Pin<MaybeSyncArc<_, T>>` treats its pointee as pinned,
                // just like `Arc<T>` and `Rc<T>` do.
                unsafe { Pin::new_unchecked(this) }
            },
            into_arc_pin: |this: Pin<Self>| {
                // SAFETY: `Pin<Arc<T>>` treats its pointee as pinned. Since we do not move out
                // of the pointee in the body of this function (we just move the wrapping `Arc`)
                // and pin the `Arc` before exposing it to arbitrary user code, we uphold the
                // pinning invariants.
                let this: Self = unsafe { Pin::into_inner_unchecked(this) };
                let this = ManuallyDrop::new(this);

                // SAFETY: Note that this cannot cause a double-drop of `this.inner`,
                // since `this` is wrapped in a `ManuallyDrop`.
                // For more details, see the longer safety comment for `into_arc` above.
                let inner = unsafe { ptr::read(&raw const this.inner) };

                // SAFETY: since `SYNC` is true, the `sync` field is initialized
                let sync = unsafe { inner.sync };
                let arc = ManuallyDrop::into_inner(sync);

                // SAFETY: `Pin<Arc<T>>` treats its pointee as pinned.
                unsafe { Pin::new_unchecked(arc) }
            },
            from_weak: |weak: WeakArc<T>| {
                MaybeSyncWeak::<SYNC, _> {
                    inner: MaybeSyncWeakInner {
                        // Safety invariant: `SYNC` is true, and we initialize the `sync` field.
                        sync: ManuallyDrop::new(weak),
                    },
                }
            },
            into_weak: |weak_this: MaybeSyncWeak<SYNC, T>| {
                let weak_this = ManuallyDrop::new(weak_this);
                // SAFETY: Note that this cannot cause a double-drop of `weak_this.inner`,
                // since `weak_this` is wrapped in a `ManuallyDrop`. For more details, see the
                // longer safety comment for `into_arc` above.
                let inner = unsafe { ptr::read(&raw const weak_this.inner) };

                // SAFETY: since `SYNC` is true, the `sync` field is initialized
                let sync = unsafe { inner.sync };
                ManuallyDrop::into_inner(sync)
            },
            as_weak_ref: coerce_ref_fn(|weak_this: &MaybeSyncWeak<SYNC, T>| {
                // SAFETY: since `SYNC` is true, the `sync` field is initialized
                let sync = unsafe { &weak_this.inner.sync };
                &**sync
            }),
            as_weak_mut: coerce_mut_fn(|weak_this: &mut MaybeSyncWeak<SYNC, T>| {
                // SAFETY: since `SYNC` is true, the `sync` field is initialized
                let sync = unsafe { &mut weak_this.inner.sync };
                &mut **sync
            }),
            from_weak_ref: coerce_weak_ref_fn(|weak| {
                // SAFETY: `SYNC` is true, as asserted by the caller.
                unsafe { MaybeSyncWeakRef::asserted_sync_weak_ref(weak) }
            }),
        }
    }

    /// # Safety
    /// `SYNC` must be `false`.
    #[inline]
    #[must_use]
    unsafe fn asserted_unsync_operations() -> rc_operations!(SYNC, T) {
        Operations {
            from_arc: |rc: Rc<T>| {
                Self {
                    inner: MaybeSyncArcInner {
                        // Safety invariant: `SYNC` is false, and we initialize the `unsync` field.
                        unsync: ManuallyDrop::new(rc),
                    },
                }
            },
            into_arc: |this: Self| {
                let this = ManuallyDrop::new(this);
                // SAFETY: Note that this cannot cause a double-drop of `this.inner`,
                // since `this` is wrapped in a `ManuallyDrop`. For more details, see the
                // longer safety comment for the `SYNC` version of `into_arc` above.
                let inner = unsafe { ptr::read(&raw const this.inner) };

                // SAFETY: since `SYNC` is false, the `unsync` field is initialized
                let unsync = unsafe { inner.unsync };
                ManuallyDrop::into_inner(unsync)
            },
            as_arc_ref: coerce_ref_fn(|this: &Self| {
                // SAFETY: since `SYNC` is false, the `unsync` field is initialized
                let unsync = unsafe { &this.inner.unsync };
                &**unsync
            }),
            as_arc_mut: coerce_mut_fn(|this: &mut Self| {
                // SAFETY: since `SYNC` is false, the `unsync` field is initialized
                let unsync = unsafe { &mut this.inner.unsync };
                &mut **unsync
            }),
            from_arc_pin: |rc: Pin<Rc<T>>| {
                // SAFETY: `Pin<MaybeSyncArc<_, T>>` treats its pointee as pinned,
                // just like `Arc<T>` and `Rc<T>` do. Since we do not move out of the pointee in
                // the body of this function (we just move the wrapping `Rc`) and pin `this`
                // before exposing it to arbitrary user code, we uphold the pinning invariants.
                let rc: Rc<T> = unsafe { Pin::into_inner_unchecked(rc) };

                let this = Self {
                    inner: MaybeSyncArcInner {
                        // Safety invariant: `SYNC` is false, and we initialize the `unsync` field.
                        unsync: ManuallyDrop::new(rc),
                    },
                };

                // SAFETY: `Pin<MaybeSyncArc<_, T>>` treats its pointee as pinned,
                // just like `Arc<T>` and `Rc<T>` do.
                unsafe { Pin::new_unchecked(this) }
            },
            into_arc_pin: |this: Pin<Self>| {
                // SAFETY: `Pin<Rc<T>>` treats its pointee as pinned. Since we do not move out
                // of the pointee in the body of this function (we just move the wrapping `Rc`)
                // and pin the `Rc` before exposing it to arbitrary user code, we uphold the
                // pinning invariants.
                let this: Self = unsafe { Pin::into_inner_unchecked(this) };
                let this = ManuallyDrop::new(this);

                // SAFETY: Note that this cannot cause a double-drop of `this.inner`,
                // since `this` is wrapped in a `ManuallyDrop`. For more details, see the
                // longer safety comment for the `SYNC` version of `into_arc` above.
                let inner = unsafe { ptr::read(&raw const this.inner) };

                // SAFETY: since `SYNC` is false, the `unsync` field is initialized
                let unsync = unsafe { inner.unsync };
                let rc = ManuallyDrop::into_inner(unsync);

                // SAFETY: `Pin<Rc<T>>` treats its pointee as pinned.
                unsafe { Pin::new_unchecked(rc) }
            },
            from_weak: |weak: WeakRc<T>| {
                MaybeSyncWeak::<SYNC, _> {
                    inner: MaybeSyncWeakInner {
                        // Safety invariant: `SYNC` is false, and we initialize the `unsync` field.
                        unsync: ManuallyDrop::new(weak),
                    },
                }
            },
            into_weak: |weak_this: MaybeSyncWeak<SYNC, T>| {
                let weak_this = ManuallyDrop::new(weak_this);
                // SAFETY: Note that this cannot cause a double-drop of `weak_this.inner`,
                // since `weak_this` is wrapped in a `ManuallyDrop`. For more details, see the
                // longer safety comment for the `SYNC` version of `into_arc` above.
                let inner = unsafe { ptr::read(&raw const weak_this.inner) };

                // SAFETY: since `SYNC` is false, the `unsync` field is initialized
                let unsync = unsafe { inner.unsync };
                ManuallyDrop::into_inner(unsync)
            },
            as_weak_ref: coerce_ref_fn(|weak_this: &MaybeSyncWeak<SYNC, T>| {
                // SAFETY: since `SYNC` is false, the `unsync` field is initialized
                let unsync = unsafe { &weak_this.inner.unsync };
                &**unsync
            }),
            as_weak_mut: coerce_mut_fn(|weak_this: &mut MaybeSyncWeak<SYNC, T>| {
                // SAFETY: since `SYNC` is false, the `unsync` field is initialized
                let unsync = unsafe { &mut weak_this.inner.unsync };
                &mut **unsync
            }),
            from_weak_ref: coerce_weak_ref_fn(|weak| {
                // SAFETY: `SYNC` is false, as asserted by the caller.
                unsafe { MaybeSyncWeakRef::asserted_unsync_weak_ref(weak) }
            }),
        }
    }
}

impl<'a, const SYNC: bool, T: ?Sized> MaybeSyncWeakRef<'a, SYNC, T> {
    /// # Safety
    /// `SYNC` must be `true`.
    unsafe fn asserted_sync_weak_ref(weak: &'a WeakArc<T>) -> Self {
        // Essentially, we want to construct a `ManuallyDrop<MaybeSyncWeak<SYNC, T>>`
        // without ever "arming" the destructor of a `MaybeSyncWeak<SYNC, T>` or a `WeakArc`.
        // I choose `MaybeUninit` to do so.

        let mut this: MaybeUninit<Self> = MaybeUninit::uninit();
        // Note that `Self = MaybeSyncWeakRef<'_, SYNC, T>` is a transparent wrapper around
        // `ManuallyDrop<MaybeSyncWeak<SYNC, T>>`, which is a transparent wrapper around
        // `MaybeSyncWeak<SYNC, T>`, which is a transparent wrapper around `MaybeSyncWeakInner<T>`.
        // Therefore, since `this.as_mut_ptr()` is valid for writes of type `ManuallyDrop<Self>`,
        // `this_inner` is also valid for writes of type `MaybeSyncWeakInner<T>`.
        let this_inner = this.as_mut_ptr().cast::<MaybeSyncWeakInner<T>>();
        // `allow` is needed instead of `expect`, since for some reason, it doesn't always trigger.
        #[allow(
            clippy::multiple_unsafe_ops_per_block,
            reason = "The two ops are a raw pointer deref and accessing a field of a union. \
                      However, due to the prepended `&raw mut`, that pointer is not deref'd and \
                      the field is not actually accessed; this computes a pointer offset",
        )]
        // SAFETY: For requirements for this to be sound, see
        // <https://github.com/rust-lang/rust/pull/127679#issue-2406868805> or similar.
        // Basically, `&raw mut (*this_inner).sync` does NOT read `this`, it adds
        // the offset of the `sync` field to `this_inner`, using `ptr::offset` semantics
        // rather than `ptr::wrapping_offset` semantics. Since `ptr::offset` unsafely requires
        // that the offset remains in-bounds of the source allocation, this operation is unsafe,
        // but does NOT read the uninit data pointed to by `this_inner`.
        // Therefore, the reason this is sound is: `this_inner` points to the start of an allocation
        // of size `size_of::<MaybeUninit<ManuallyDrop<Self>>>()`, which equals
        // `size_of::<MaybeSyncWeakInner<T>>()`. The `sync` field is necessarily in bounds of an
        // allocation big enough for type `MaybeSyncWeakInner<T>`, so performing this offset
        // does not go out-of-bounds and is thus sound.
        let sync = unsafe { &raw mut (*this_inner).sync };

        let weak_ptr: *const WeakArc<T> = weak;
        // Note that `ManuallyDrop<<WeakArc<T>>` is a transparent wrapper around `WeakArc<T>`,
        // so since `weak_ptr` is valid for reads of type `WeakArc<T>` (aside from concerns
        // about double drops), it is also valid for reads of type `ManuallyDrop<<WeakArc<T>>`.
        let weak_ptr = weak_ptr.cast::<ManuallyDrop<WeakArc<T>>>();

        // SAFETY:
        // - `weak_ptr` is valid for reads, as discussed above; note in particular that subtle
        //   concerns about pointer provenance should not be able to render this code unsound,
        //   as `WeakArc` does not have `noalias` semantics or other exclusive ownership semantics.
        //
        //   To further elaborate, `WeakArc` (just like `WeakRc`) implements `Freeze`, and the
        //   reason it is not `Copy` is solely because it semantically owns one weak reference
        //   count. Also, note that the global allocator (which "forward[s] calls to the allocator
        //   registered with the `#[global_allocator]` attribute if there is one, or the `std`
        //   crate’s default") implements `Copy`, and as per that documentation blurb, it's
        //   not a problem if the allocator marked with `#[global_allocator]` cannot be copied.
        //
        //   Therefore, we "should just" need to address concerns about the weak reference count.
        //   Technically, it could be possible to notice that there's one more `WeakArc` lying
        //   around than there are weak reference counts (by comparing addresses of `&WeakArc`
        //   references), but this reference type is only used in `MaybeSyncArc::new_cyclic`
        //   and we do not expose the given `&WeakArc` to arbitrary user code, so as far as the
        //   user's callback knows, the `WeakArc` copy in the `MaybeSyncWeakRef` uniquely holds
        //   a weak reference count. `std`, meanwhile, is known not to do anything weird. Therefore,
        //   we don't need to worry about questionable address-comparing code.
        //
        //   As for concerns about double drops, we are careful to never enable a destructor,
        //   not even transiently, that could drop the copied `WeakArc`. Therefore, the fact that
        //   we do not own a weak count is not a problem, since we never incorrectly assert
        //   ownership over a weak count (whether in `Drop` or otherwise, since we never expose
        //   the `WeakArc` except behind an immutable reference), and the `PhantomData` field
        //   ensures that we stop using the copied `WeakArc` before the source `WeakArc` could have
        //   a chance to assert unique ownership over its weak reference count (which we're
        //   semantically borrowing).
        //
        // - `weak_ptr` is properly-aligned, since it came from a necessarily-properly-aligned
        //   reference.
        //
        // - `weak_ptr` points to a valid value of type `ManuallyDrop<WeakArc<T>>` since it
        //   came from a reference to a value of type `WeakArc<T>`, and `ManuallyDrop` is a
        //   transparent wrapper.
        let weak_copy: ManuallyDrop<WeakArc<T>> = unsafe {
            ptr::read(weak_ptr)
        };

        // SAFETY:
        // - `sync` is valid for writes of type `ManuallyDrop<WeakArc<T>>`, since it is a pointer
        //   offset to the `sync` field from a pointer valid for writes of type
        //   `MaybeSyncWeakInner<T>` (as discussed above).
        // - `sync` is properly-aligned, for the same reason as the first bullet point, noting
        //   that `MaybeSyncWeakInner<T>` is not `repr(packed)`.
        unsafe {
            ptr::write(sync, weak_copy);
        };

        // SAFETY: We have fully initialized the `weak` field (specifically, the `sync`
        // field of the union is initialized). The type of the `lt` field is an
        // inhabited ZST, so it does not need to be written; it's already initialized.
        // Therefore, `this` is fully initialized.
        unsafe { this.assume_init() }
    }

    /// # Safety
    /// `SYNC` must be `false`.
    unsafe fn asserted_unsync_weak_ref(weak: &'a WeakRc<T>) -> Self {
        // Essentially, we want to construct a `ManuallyDrop<MaybeSyncWeak<SYNC, T>>`
        // without ever "arming" the destructor of a `MaybeSyncWeak<SYNC, T>` or a `WeakRc`.
        // I choose `MaybeUninit` to do so.

        let mut this: MaybeUninit<Self> = MaybeUninit::uninit();
        let this_inner = this.as_mut_ptr().cast::<MaybeSyncWeakInner<T>>();
        // `allow` is needed instead of `expect`, since for some reason, it doesn't always trigger.
        #[allow(
            clippy::multiple_unsafe_ops_per_block,
            reason = "The two ops are a raw pointer deref and accessing a field of a union. \
                      However, due to the prepended `&raw mut`, that pointer is not deref'd and \
                      the field is not actually accessed; this computes a pointer offset",
        )]
        // SAFETY: Same as in `asserted_sync_weak_ref`, up to replacing mentions of `sync`
        // with `unsync`.
        let unsync = unsafe { &raw mut (*this_inner).unsync };

        let weak_ptr: *const WeakRc<T> = weak;
        let weak_ptr = weak_ptr.cast::<ManuallyDrop<WeakRc<T>>>();

        // SAFETY: Same as in `asserted_sync_weak_ref`, up to replacing mentions of
        // `WeakArc` with `WeakRc`.
        let weak_copy: ManuallyDrop<WeakRc<T>> = unsafe {
            ptr::read(weak_ptr)
        };

        // SAFETY:
        // - `unsync` is valid for writes of type `ManuallyDrop<WeakRc<T>>`, since it is a pointer
        //   offset to the `unsync` field from a pointer valid for writes of type
        //   `MaybeSyncWeakInner<T>` (as discussed above).
        // - `unsync` is properly-aligned, for the same reason as the first bullet point, noting
        //   that `MaybeSyncWeakInner<T>` is not `repr(packed)`.
        unsafe {
            ptr::write(unsync, weak_copy);
        };

        // SAFETY: We have fully initialized the `weak` field (specifically, the `unsync`
        // field of the union is initialized). The type of the `lt` field is an
        // inhabited ZST, so it does not need to be written; it's already initialized.
        // Therefore, `this` is fully initialized.
        unsafe { this.assume_init() }
    }
}

// ================================================================
//  Safe API exposed for the above
// ================================================================

impl<const SYNC: bool, T: ?Sized> MaybeSyncArc<SYNC, T> {
    #[inline]
    #[must_use]
    pub(super) fn operations() -> maybe_sync_operations!(SYNC, T) {
        if SYNC {
            // SAFETY: `SYNC` is `true`.
            MaybeSync::Sync(unsafe { Self::asserted_sync_operations() })
        } else {
            // SAFETY: `SYNC` is `false`.
            MaybeSync::Unsync(unsafe { Self::asserted_unsync_operations() })
        }
    }

    #[inline]
    #[must_use]
    pub(super) fn operations_pair<U: ?Sized>() -> maybe_sync_operations!(SYNC, T, U) {
        if SYNC {
            MaybeSync::Sync((
                // SAFETY: `SYNC` is `true`.
                unsafe { Self::asserted_sync_operations() },
                // SAFETY: `SYNC` is `true`.
                unsafe { MaybeSyncArc::<SYNC, U>::asserted_sync_operations() },
            ))
        } else {
            MaybeSync::Unsync((
                // SAFETY: `SYNC` is `false`.
                unsafe { Self::asserted_unsync_operations() },
                // SAFETY: `SYNC` is `false`.
                unsafe { MaybeSyncArc::<SYNC, U>::asserted_unsync_operations() },
            ))
        }
    }
}

impl<T: ?Sized> MaybeSyncArc<true, T> {
    #[inline]
    #[must_use]
    pub(super) fn sync_operations() -> arc_operations!(true, T) {
        match Self::operations() {
            MaybeSync::Sync(sync_ops) => sync_ops,
            // SAFETY: `SYNC` is `true`, so `Self::operations()` returns the `Sync` version.
            MaybeSync::Unsync(_) => unsafe { unreachable_unchecked() },
        }
    }
}

impl<T: ?Sized> MaybeSyncArc<false, T> {
    #[inline]
    #[must_use]
    pub(super) fn unsync_operations() -> rc_operations!(false, T) {
        match Self::operations() {
            // SAFETY: `SYNC` is `false`, so `Self::operations()` returns the `Unsync` version.
            MaybeSync::Sync(_) => unsafe { unreachable_unchecked() },
            MaybeSync::Unsync(unsync_ops) => unsync_ops,
        }
    }
}

impl<const SYNC: bool, T: ?Sized> MaybeSyncWeak<SYNC, T> {
    #[inline]
    #[must_use]
    pub(super) fn operations() -> maybe_sync_operations!(SYNC, T) {
        MaybeSyncArc::operations()
    }
}

impl<T: ?Sized> MaybeSyncWeak<true, T> {
    #[inline]
    #[must_use]
    pub(super) fn sync_operations() -> arc_operations!(true, T) {
        MaybeSyncArc::sync_operations()
    }
}

impl<T: ?Sized> MaybeSyncWeak<false, T> {
    #[inline]
    #[must_use]
    pub(super) fn unsync_operations() -> rc_operations!(false, T) {
        MaybeSyncArc::unsync_operations()
    }
}

impl<const SYNC: bool, T: ?Sized> MaybeSyncWeakRef<'_, SYNC, T> {
    #[inline]
    #[must_use]
    pub(super) fn weak_ref(&self) -> &MaybeSyncWeak<SYNC, T> {
        &self.weak
    }
}
