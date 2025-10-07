use std::fmt::Debug;
use std::{
    cell::{RefCell, RefMut},
    ops::{Deref, DerefMut},
    rc::{Rc, Weak as WeakRc},
    sync::{
        Arc, Mutex, MutexGuard, PoisonError, RwLock,
        RwLockReadGuard, RwLockWriteGuard, Weak as WeakArc,
    },
};

use clone_behavior::{ConstantTime, MirroredClone};
use generic_container::{Container, FragileMutContainer};
use generic_container::kinds::{ArcKind, RcKind};


/// A higher-kinded abstraction over types which resemble `Rc<T>` or `Arc<T>`.
pub trait RefcountedFamily {
    type Container<T>: Container<T> + MirroredClone<ConstantTime>;
    type WeakContainer<T>: MirroredClone<ConstantTime>;

    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    type ContainerAsDebug<T: Debug>: Debug;
    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    type WeakContainerAsDebug<T: Debug>: Debug;

    /// See [`Rc::downgrade`] and [`Arc::downgrade`].
    fn downgrade<T>(container: &Self::Container<T>) -> Self::WeakContainer<T>;
    /// See [`rc::Weak::upgrade`] and [`sync::Weak::upgrade`].
    ///
    /// [`rc::Weak::upgrade`]: std::rc::Weak::upgrade
    /// [`sync::Weak::upgrade`]: std::sync::Weak::upgrade
    fn upgrade<T>(container: Self::WeakContainer<T>) -> Option<Self::Container<T>>;

    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    ///
    /// The body of this method should be `container`.
    fn debug<T: Debug>(container: &Self::Container<T>) -> &Self::ContainerAsDebug<T>;
    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    ///
    /// The body of this method should be `container`.
    fn debug_weak<T: Debug>(container: &Self::WeakContainer<T>) -> &Self::WeakContainerAsDebug<T>;
}

impl RefcountedFamily for RcKind {
    type Container<T> = Rc<T>;
    type WeakContainer<T> = WeakRc<T>;
    type ContainerAsDebug<T: Debug> = Rc<T>;
    type WeakContainerAsDebug<T: Debug> = WeakRc<T>;

    fn downgrade<T>(container: &Self::Container<T>) -> Self::WeakContainer<T> {
        Rc::downgrade(&container)
    }

    fn upgrade<T>(container: Self::WeakContainer<T>) -> Option<Self::Container<T>> {
        container.upgrade()
    }

    fn debug<T: Debug>(container: &Self::Container<T>) -> &Self::ContainerAsDebug<T> {
        container
    }

    fn debug_weak<T: Debug>(container: &Self::WeakContainer<T>) -> &Self::WeakContainerAsDebug<T> {
        container
    }
}

impl RefcountedFamily for ArcKind {
    type Container<T> = Arc<T>;
    type WeakContainer<T> = WeakArc<T>;
    type ContainerAsDebug<T: Debug> = Arc<T>;
    type WeakContainerAsDebug<T: Debug> = WeakArc<T>;

    fn downgrade<T>(container: &Self::Container<T>) -> Self::WeakContainer<T> {
        Arc::downgrade(&container)
    }

    fn upgrade<T>(container: Self::WeakContainer<T>) -> Option<Self::Container<T>> {
        container.upgrade()
    }

    fn debug<T: Debug>(container: &Self::Container<T>) -> &Self::ContainerAsDebug<T> {
        container
    }

    fn debug_weak<T: Debug>(container: &Self::WeakContainer<T>) -> &Self::WeakContainerAsDebug<T> {
        container
    }
}

/// A higher-kinded abstraction over types which resemble `RefCell<T>`, `RwLock<T>`, or `Mutex<T>`.
///
/// Implementations may panic when poison is encountered.
pub trait RwCellFamily {
    type RwCell<T>: RwCell<T>;

    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    type RwCellAsDebug<T: Debug>: Debug;

    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    ///
    /// The body of this method should be `container`.
    fn debug<T: Debug>(container: &Self::RwCell<T>) -> &Self::RwCellAsDebug<T>;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct RefCellKind;

impl RwCellFamily for RefCellKind {
    type RwCell<T> = RefCell<T>;
    type RwCellAsDebug<T: Debug> = RefCell<T>;

    fn debug<T: Debug>(container: &Self::RwCell<T>) -> &Self::RwCellAsDebug<T> {
        container
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct RwLockKind;

impl RwCellFamily for RwLockKind {
    type RwCell<T> = RwLock<T>;
    type RwCellAsDebug<T: Debug> = RwLock<T>;

    fn debug<T: Debug>(container: &Self::RwCell<T>) -> &Self::RwCellAsDebug<T> {
        container
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct MutexKind;

impl RwCellFamily for MutexKind {
    type RwCell<T> = Mutex<T>;
    type RwCellAsDebug<T: Debug> = Mutex<T>;

    fn debug<T: Debug>(container: &Self::RwCell<T>) -> &Self::RwCellAsDebug<T> {
        container
    }
}

/// An abstraction over types which resemble `RefCell<T>`, `RwLock<T>`, or `Mutex<T>`.
///
/// Implementations may panic when poison is encountered.
pub trait RwCell<T> {
    /// An immutably borrowed value from the cell.
    ///
    /// May have a nontrivial `Drop` implementation, as with the [`Ref`] type corresponding
    /// to [`RefCell`].
    ///
    /// [`Ref`]: std::cell::Ref
    /// [`RefCell`]: std::cell::RefCell
    type Ref<'a>: Deref<Target = T> where Self: 'a;
    /// A mutably borrowed value from the cell.
    ///
    /// May have a nontrivial `Drop` implementation, as with the [`RefMut`] type corresponding
    /// to [`RefCell`].
    ///
    /// [`RefMut`]: std::cell::RefMut
    /// [`RefCell`]: std::cell::RefCell
    type RefMut<'a>: DerefMut<Target = T> where Self: 'a;

    /// Create a new cell that owns the provided `T`.
    #[must_use]
    fn new_rw_cell(t: T) -> Self;

    /// Retrieve the inner `T` from the cell.
    #[must_use]
    fn into_inner(self) -> T;

    /// Get immutable access to the inner `T`.
    ///
    /// # Panics or Deadlocks
    /// A single thread must not call this method concurrently. Doing so may result in a panic
    /// or deadlock. This method exists solely to provide opportunities for better multithreaded
    /// performance with types like `RwLock<T>`. For types which that is not relevant to,
    /// including `RefCell<T>` and `Mutex<T>`, implementations can simply call `self.write()`.
    #[must_use]
    fn read(&self) -> Self::Ref<'_>;

    /// Get mutable access to the inner `T`.
    ///
    /// # Panics or Deadlocks
    /// A single thread must not call this method concurrently. Doing so may result in a panic
    /// or deadlock.
    #[must_use]
    fn write(&self) -> Self::RefMut<'_>;
}

impl<T> RwCell<T> for RefCell<T> {
    type Ref<'a>    = RefMut<'a, T> where Self: 'a;
    type RefMut<'a> = RefMut<'a, T> where Self: 'a;

    #[inline]
    fn new_rw_cell(t: T) -> Self {
        Self::new(t)
    }

    #[inline]
    fn into_inner(self) -> T {
        #[expect(clippy::use_self, reason = "distinction from `RwCell::into_inner`")]
        RefCell::into_inner(self)
    }

    #[inline]
    fn read(&self) -> Self::Ref<'_> {
        self.write()
    }

    #[inline]
    fn write(&self) -> Self::RefMut<'_> {
        self.borrow_mut()
    }
}

impl<T> RwCell<T> for RwLock<T> {
    type Ref<'a>    = RwLockReadGuard<'a, T> where Self: 'a;
    type RefMut<'a> = RwLockWriteGuard<'a, T> where Self: 'a;

    #[inline]
    fn new_rw_cell(t: T) -> Self {
        Self::new(t)
    }

    #[inline]
    fn into_inner(self) -> T {
        #[expect(clippy::use_self, reason = "distinction from `RwCell::into_inner`")]
        let maybe_poison: Result<_, PoisonError<_>> = RwLock::into_inner(self);
        #[expect(clippy::unwrap_used, reason = "poison means a thread has panicked")]
        maybe_poison.unwrap()
    }

    #[inline]
    fn read(&self) -> Self::Ref<'_> {
        #[expect(clippy::use_self, reason = "distinction from `RwCell::read`")]
        let maybe_poison: Result<_, PoisonError<_>> = RwLock::read(self);
        #[expect(clippy::unwrap_used, reason = "poison means a thread has panicked")]
        maybe_poison.unwrap()
    }

    #[inline]
    fn write(&self) -> Self::RefMut<'_> {
        #[expect(clippy::use_self, reason = "distinction from `RwCell::write`")]
        let maybe_poison: Result<_, PoisonError<_>> = RwLock::write(self);
        #[expect(clippy::unwrap_used, reason = "poison means a thread has panicked")]
        maybe_poison.unwrap()
    }
}

impl<T> RwCell<T> for Mutex<T> {
    type Ref<'a>    = MutexGuard<'a, T> where Self: 'a;
    type RefMut<'a> = MutexGuard<'a, T> where Self: 'a;

    #[inline]
    fn new_rw_cell(t: T) -> Self {
        Self::new(t)
    }

    #[inline]
    fn into_inner(self) -> T {
        #[expect(clippy::use_self, reason = "distinction from `RwCell::into_inner`")]
        let maybe_poison: Result<_, PoisonError<_>> = Mutex::into_inner(self);
        #[expect(clippy::unwrap_used, reason = "poison means a thread has panicked")]
        maybe_poison.unwrap()
    }

    #[inline]
    fn read(&self) -> Self::Ref<'_> {
        self.write()
    }

    #[inline]
    fn write(&self) -> Self::RefMut<'_> {
        let maybe_poison: Result<_, PoisonError<_>> = self.lock();
        #[expect(clippy::unwrap_used, reason = "poison means a thread has panicked")]
        maybe_poison.unwrap()
    }
}
