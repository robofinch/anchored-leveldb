use std::rc::Rc;
use std::{
    cell::{Ref, RefCell, RefMut},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::error::{MutexPoisoned, Never};


// TODO: documentation

#[expect(
    clippy::doc_markdown,
    reason = "There are no backticks around InnerFile in the header, as it is already in quotes \
              and looks better in the header without both IMO; and it semantically needs quotes, \
              as the term itself is discussed.",
)]
/// The inner buffer of a `MemoryFile`, providing interior mutability and the ability to cheaply
/// clone.
///
/// ### "InnerFile"
///
/// The generic parameters of [`MemoryFSWithInner`] and [`MemoryFileWithInner`], which should
/// implement this `MemoryFileInner` trait, are named `InnerFile`; for consistency, the
/// documentation of this trait refers to the type implementing `MemoryFileInner` as an
/// "`InnerFile`".
///
/// [`MemoryFileWithInner`]: super::file::MemoryFileWithInner
/// [`MemoryFSWithInner`]: super::fs::MemoryFSWithInner
pub trait MemoryFileInner: Clone {
    /// Provides immutable access to the file's inner buffer.
    type InnerBufRef<'a>:    Deref<Target = Vec<u8>> where Self: 'a;
    /// Provides mutable access to the file's inner buffer.
    type InnerBufRefMut<'a>: DerefMut<Target = Vec<u8>> where Self: 'a;
    /// Should implement [`std::error::Error`] and [`FSError`],
    /// and `From<InnerFileError>` should be implemented for [`std::io::Error`].
    ///
    /// [`FSError`]: crate::util_traits::FSError
    type InnerFileError;

    /// Returns an `InnerFile` referencing a new, empty buffer.
    fn new() -> Self;

    /// Returns the length of this file in bytes.
    ///
    /// # Errors
    ///
    /// Errors are implementation-dependent, and may include, for instance, a mutex being poisoned.
    fn len(&self) -> Result<usize, Self::InnerFileError>;

    /// Checks whether the file has a length of zero bytes.
    ///
    /// # Errors
    ///
    /// Errors are implementation-dependent, and may include, for instance, a mutex being poisoned.
    fn is_empty(&self) -> Result<bool, Self::InnerFileError>;

    /// Access the inner buffer.
    ///
    /// # Panics or Deadlocks
    /// If an `InnerFile` referencing the same inner buffer is accessed before a
    /// returned [`InnerBufRef`] is dropped, a panic or deadlock is extremely likely to occur,
    /// depending on the implementation.
    ///
    /// If no `MemoryFS`-related structs are accessed before the reference is dropped, a panic or
    /// deadlock should not occur. Ideally, no access to a `MemoryFile` or `InnerFile` should occur
    /// while the returned reference is live.
    ///
    /// # Errors
    ///
    /// Errors are implementation-dependent, and may include, for instance, a mutex being poisoned.
    ///
    /// [`InnerBufRef`]: MemoryFileInner::InnerBufRef
    fn inner_buf(&self) -> Result<Self::InnerBufRef<'_>, Self::InnerFileError>;

    /// Mutably access the inner buffer.
    ///
    /// # Panics or Deadlocks
    /// If an `InnerFile` referencing the same inner buffer is accessed before a
    /// returned [`InnerBufRefMut`] is dropped, a panic or deadlock is extremely likely to occur,
    /// depending on the implementation.
    ///
    /// If no `MemoryFS`-related structs are accessed before the reference is dropped, a panic or
    /// deadlock should not occur. Ideally, no access to a `MemoryFile` or `InnerFile` should occur
    /// while the returned reference is live.
    ///
    /// # Errors
    ///
    /// Errors are implementation-dependent, and may include, for instance, a mutex being poisoned.
    ///
    /// [`InnerBufRefMut`]: MemoryFileInner::InnerBufRefMut
    fn inner_buf_mut(&self) -> Result<Self::InnerBufRefMut<'_>, Self::InnerFileError>;
}

impl MemoryFileInner for Rc<RefCell<Vec<u8>>> {
    type InnerBufRef<'a>    = Ref<'a, Vec<u8>>;
    type InnerBufRefMut<'a> = RefMut<'a, Vec<u8>>;
    type InnerFileError     = Never;

    /// Returns a wrapper around a new, empty buffer.
    #[inline]
    fn new() -> Self {
        Self::default()
    }

    /// Infallibly returns the length of the inner buffer.
    #[inline]
    fn len(&self) -> Result<usize, Self::InnerFileError> {
        Ok(self.borrow().len())
    }

    /// Infallibly returns whether the length of the inner buffer is zero.
    #[inline]
    fn is_empty(&self) -> Result<bool, Self::InnerFileError> {
        Ok(self.borrow().is_empty())
    }

    /// Infallibly access the inner buffer.
    ///
    /// # Panics
    /// If mutable access to the inner buffer is attempted before the returned `InnerBufRef` is
    /// dropped, via a `Rc<RefCell<Vec<u8>>>` referencing the same inner buffer, then a panic will
    /// occur.
    ///
    /// If no `MemoryFS`-related structs are accessed before the reference is dropped, a panic
    /// should not occur. Ideally, no access to a `MemoryFile` or `InnerFile` should occur
    /// while the returned reference is live.
    // TODO: link to InnerBufRef above
    #[inline]
    fn inner_buf(&self) -> Result<Self::InnerBufRef<'_>, Self::InnerFileError> {
        Ok(self.borrow())
    }

    /// Infallibly and mutably access the inner buffer.
    ///
    /// # Panics
    /// If access to the inner buffer is attempted before the returned `InnerBufRefMut` is
    /// dropped, via a `Rc<RefCell<Vec<u8>>>` referencing the same inner buffer, then a panic will
    /// occur.
    ///
    /// If no `MemoryFS`-related structs are accessed before the reference is dropped, a panic
    /// should not occur. Ideally, no access to a `MemoryFile` or `InnerFile` should occur
    /// while the returned reference is live.
    // TODO: link to InnerBufRefMut above
    #[inline]
    fn inner_buf_mut(&self) -> Result<Self::InnerBufRefMut<'_>, Self::InnerFileError> {
        Ok(self.borrow_mut())
    }
}

impl MemoryFileInner for Arc<Mutex<Vec<u8>>> {
    type InnerBufRef<'a>    = MutexGuard<'a, Vec<u8>>;
    type InnerBufRefMut<'a> = MutexGuard<'a, Vec<u8>>;
    type InnerFileError     = MutexPoisoned;

    /// Returns a wrapper around a new, empty buffer.
    #[inline]
    fn new() -> Self {
        Self::default()
    }

    /// Returns the length of the inner buffer.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if the internal mutex is poisoned.
    #[inline]
    fn len(&self) -> Result<usize, Self::InnerFileError> {
        Ok(self.lock()?.len())
    }

    /// Returns whether the length of the inner buffer is zero.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if the internal mutex is poisoned.
    #[inline]
    fn is_empty(&self) -> Result<bool, Self::InnerFileError> {
        Ok(self.lock()?.is_empty())
    }

    /// Access the inner buffer.
    ///
    /// # Deadlocks
    /// If access to the inner buffer is attempted on the same thread before the returned
    /// `InnerBufRef` is dropped, via an `Arc<Mutex<Vec<u8>>>` referencing the same inner buffer,
    /// then a deadlock will occur.
    ///
    /// If no `MemoryFS`-related structs are accessed before the reference is dropped, a deadlock
    /// should not occur. Ideally, no access to a `MemoryFile` or `InnerFile` should occur
    /// while the returned reference is live.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if the internal mutex is poisoned.
    // TODO: link to InnerBufRef above
    #[inline]
    fn inner_buf(&self) -> Result<Self::InnerBufRef<'_>, Self::InnerFileError> {
        Ok(self.lock()?)
    }

    /// Mutably access the inner buffer.
    ///
    /// # Deadlocks
    /// If access to the inner buffer is attempted on the same thread before the returned
    /// `InnerBufRefMut` is dropped, via an `Arc<Mutex<Vec<u8>>>` referencing the same inner buffer,
    /// then a deadlock will occur.
    ///
    /// If no `MemoryFS`-related structs are accessed before the reference is dropped, a deadlock
    /// should not occur. Ideally, no access to a `MemoryFile` or `InnerFile` should occur
    /// while the returned reference is live.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if the internal mutex is poisoned.
    // TODO: link to InnerBufRefMut above
    #[inline]
    fn inner_buf_mut(&self) -> Result<Self::InnerBufRefMut<'_>, Self::InnerFileError> {
        Ok(self.lock()?)
    }
}
