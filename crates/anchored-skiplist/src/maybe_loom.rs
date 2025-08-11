#![expect(unexpected_cfgs, reason = "used for loom tests")]

#[cfg(loom)]
#[derive(Debug)]
pub(crate) struct Arc<T: ?Sized>(loom::sync::Arc<T>);

#[cfg(loom)]
mod arc_impl {
    #![expect(unsafe_code, reason = "Implement StableDeref for an Arc variant")]

    use std::ops::Deref;

    use stable_deref_trait::StableDeref;

    use super::Arc;

    // SAFETY:
    // loom's `Arc` internally uses a normal `std` `Arc`, which implements `StableDeref`.
    unsafe impl<T: ?Sized> StableDeref for Arc<T> {}

    impl<T> Arc<T> {
        pub(crate) fn new(t: T) -> Self {
            Self(loom::sync::Arc::new(t))
        }
    }

    impl<T: ?Sized> Deref for Arc<T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl<T: ?Sized> Clone for Arc<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
}

#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicPtr as AtomicPtr;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicUsize as AtomicUsize;
#[cfg(loom)]
pub(crate) use loom::sync::Mutex as Mutex;
#[cfg(loom)]
pub(crate) use loom::sync::MutexGuard as MutexGuard;

#[cfg(not(loom))]
pub(crate) use std::sync::Arc as Arc;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicPtr as AtomicPtr;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicUsize as AtomicUsize;
#[cfg(not(loom))]
pub(crate) use std::sync::Mutex as Mutex;
#[cfg(not(loom))]
pub(crate) use std::sync::MutexGuard as MutexGuard;
