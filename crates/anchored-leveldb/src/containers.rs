use std::{fmt::Debug, rc::Rc};
use std::{
    cell::{RefCell, RefMut},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use clone_behavior::{ConstantTime, MirroredClone};
use generic_container::{FragileContainer, FragileMutContainer};
use generic_container::kinds::{ArcKind, ArcMutexKind, ArcRwLockKind, RcKind, RcRefCellKind};


pub trait ContainerKind {
    type Container<T>: FragileContainer<T> + MirroredClone<ConstantTime>;

    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    type ContainerAsDebug<T: Debug>: Debug;

    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    ///
    /// The body of this method should be `container`.
    fn debug<T: Debug>(container: &Self::Container<T>) -> &Self::ContainerAsDebug<T>;
}

pub trait MutContainerKind {
    type MutContainer<T>: FragileMutContainer<T> + MirroredClone<ConstantTime>;

    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    type MutContainerAsDebug<T: Debug>: Debug;

    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    ///
    /// The body of this method should be `container`.
    fn debug_mut<T: Debug>(container: &Self::MutContainer<T>) -> &Self::MutContainerAsDebug<T>;
}

macro_rules! kind {
    ($t:ident; $($kind:ident {$($struct:tt)*}),* $(,)?) => {
        $(
            impl ContainerKind for $kind {
                type Container<T> = $($struct)*;
                type ContainerAsDebug<T> = $($struct)* where T: Debug;

                fn debug<T: Debug>(container: &Self::Container<T>) -> &Self::ContainerAsDebug<T> {
                    container
                }
            }
        )*
    };
}

macro_rules! mut_kind {
    ($t:ident; $($kind:ident {$($struct:tt)*}),* $(,)?) => {
        $(
            impl ContainerKind for $kind {
                type Container<T> = $($struct)*;
                type ContainerAsDebug<T> = $($struct)* where T: Debug;

                fn debug<T: Debug>(container: &Self::Container<T>) -> &Self::ContainerAsDebug<T> {
                    container
                }
            }

            impl MutContainerKind for $kind {
                type MutContainer<T> = $($struct)*;
                type MutContainerAsDebug<T> = $($struct)* where T: Debug;

                fn debug_mut<T: Debug>(
                    container: &Self::MutContainer<T>,
                ) -> &Self::MutContainerAsDebug<T> {
                    container
                }
            }
        )*
    };
}

kind!(
    T;
    RcKind  {Rc<T>},
    ArcKind {Arc<T>},
);

mut_kind!(
    T;
    ArcMutexKind  {Arc<Mutex<T>>},
    ArcRwLockKind {Arc<RwLock<T>>},
    RcRefCellKind {Rc<RefCell<T>>},
);

/// An abstraction over types which resemble `RefCell<T>`, `RwLock<T>`, or `Mutex<T>`.
///
/// Implementations may panic when poison is encountered.
pub trait RwCell<T> {
    /// An immutably borrowed value from the cell.
    ///
    /// May have a nontrivial `Drop` implementatation, as with the [`Ref`] type corresponding
    /// to [`RefCell`].
    ///
    /// [`Ref`]: std::cell::Ref
    /// [`RefCell`]: std::cell::RefCell
    type Ref<'a>: Deref<Target = T> where Self: 'a;
    /// A mutably borrowed value from the cell.
    ///
    /// May have a nontrivial `Drop` implementatation, as with the [`RefMut`] type corresponding
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
        let maybe_poison: Result<_, PoisonError<_>> = RwLock::into_inner(self);
        maybe_poison.unwrap()
    }

    #[inline]
    fn read(&self) -> Self::Ref<'_> {
        let maybe_poison: Result<_, PoisonError<_>> = RwLock::read(self);
        maybe_poison.unwrap()
    }

    #[inline]
    fn write(&self) -> Self::RefMut<'_> {
        let maybe_poison: Result<_, PoisonError<_>> = RwLock::write(self);
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
        let maybe_poison: Result<_, PoisonError<_>> = Mutex::into_inner(self);
        maybe_poison.unwrap()
    }

    #[inline]
    fn read(&self) -> Self::Ref<'_> {
        self.write()
    }

    #[inline]
    fn write(&self) -> Self::RefMut<'_> {
        let maybe_poison: Result<_, PoisonError<_>> = Mutex::lock(self);
        maybe_poison.unwrap()
    }
}
