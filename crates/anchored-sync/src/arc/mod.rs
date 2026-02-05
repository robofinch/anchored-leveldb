/// A variety of functions mapping between the `SYNC`-generic types and the corresponding
/// concrete types.
///
/// These operations are used to consolidate `unsafe` code manipulating union fields to as few
/// places as possible.
mod operations;
/// Implementations of `Drop`, `Send`, and `Sync`.
///
/// (A few functions in the `strong_api` module also require `unsafe`, but they only defer to
/// `unsafe` methods of `Arc` or `Rc`.)
mod unsafe_impls;

/// Conversions between `MaybeSyncArc` and `Arc`/`Rc` and between `MaybeSyncWeak` and
/// `sync::Weak`/`rc::Weak`.
mod conversions;

/// Replica of the public API of `Arc` and `Rc` (in particular: stabilized methods,
/// associated functions, and many though not all trait implementations).
mod strong_api;
/// Replica of the public API of `sync::Weak` and `rc::Weak` (in particular: stabilized methods,
/// associated functions, and many though not all trait implementations).
mod weak_api;


use core::mem::ManuallyDrop;
use alloc::{
    rc::{Rc, Weak as WeakRc},
    sync::{Arc, Weak as WeakArc},
};


/// This type is equivalent to [`Arc<T>`] if `SYNC` is `true`, and it is otherwise
/// equivalent to [`Rc<T>`].
///
/// The type is implemented with a union.
///
/// # Guarantees for `unsafe` code
/// Whenever a method, associated function, or trait implementation of this type (provided in
/// this crate) delegates to `Arc` or `Rc`, `unsafe` code can soundly assume that the relevant
/// method will always delegate to the corresponding code for `Arc` (if `SYNC` is true)
/// or `Rc` (if `SYNC` is false). (Naturally, this guarantee cannot extend to methods provided by
/// arbitrary other crates.)
#[repr(transparent)]
pub struct MaybeSyncArc<const SYNC: bool, T: ?Sized> {
    /// # Safety invariant
    /// If `SYNC` is true, then the `sync` field of this union is initialized; otherwise,
    /// the `unsync` field is initialized.
    inner: MaybeSyncArcInner<T>,
}

#[expect(clippy::default_union_representation, reason = "we never do *any* type punning")]
union MaybeSyncArcInner<T: ?Sized> {
    sync:   ManuallyDrop<Arc<T>>,
    unsync: ManuallyDrop<Rc<T>>,
}

/// This type is equivalent to [`sync::Weak<T>`] if `SYNC` is `true`, and it is otherwise
/// equivalent to [`rc::Weak<T>`].
///
/// The type is implemented with a union.
///
/// # Guarantees for `unsafe` code
/// Whenever a method, associated function, or trait implementation of this type (provided in
/// this crate) delegates to `sync::Weak` or `rc::Weak`, `unsafe` code can soundly assume that the
/// relevant method will always delegate to the corresponding code for `sync::Weak`
/// (if `SYNC` is true) or `rc::Weak` (if `SYNC` is false). (Naturally, this guarantee cannot
/// extend to methods provided by arbitrary other crates.)
///
/// [`sync::Weak<T>`]: alloc::sync::Weak
/// [`rc::Weak<T>`]: alloc::rc::Weak
#[repr(transparent)]
pub struct MaybeSyncWeak<const SYNC: bool, T: ?Sized> {
    /// # Safety invariant
    /// If `SYNC` is true, then the `sync` field of this union is initialized; otherwise,
    /// the `unsync` field is initialized.
    ///
    /// Note also that `ManuallyDrop<Self>` does not necessarily own a weak count of the
    /// `WeakArc` or `WeakRc` (this is needed for `MaybeSyncWeakRef` to work), but the stored
    /// `WeakArc` or `WeakRc` must have at least one weak count while this struct exists.
    inner: MaybeSyncWeakInner<T>,
}

#[expect(clippy::default_union_representation, reason = "we never do *any* type punning")]
union MaybeSyncWeakInner<T: ?Sized> {
    sync:   ManuallyDrop<WeakArc<T>>,
    unsync: ManuallyDrop<WeakRc<T>>,
}
