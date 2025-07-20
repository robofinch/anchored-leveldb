use generic_container::FragileTryMutContainer;


// TODO: link to explanations about what `InnerFile` means

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
pub trait MemoryFileInner: FragileTryMutContainer<Vec<u8>> + Clone {
    type InnerFileError: From<Self::RefError> + From<Self::RefMutError>;

    /// Returns an `InnerFile` referencing a new, empty buffer.
    #[inline]
    #[must_use]
    fn new() -> Self {
        Self::new_container(Vec::new())
    }

    /// Returns the length of this file in bytes.
    ///
    /// # Errors
    ///
    /// Errors are implementation-dependent, and may include, for instance, a mutex being poisoned.
    #[inline]
    fn len(&self) -> Result<usize, Self::InnerFileError> {
        Ok(self.try_get_ref()?.len())
    }

    /// Checks whether the file has a length of zero bytes.
    ///
    /// # Errors
    ///
    /// Errors are implementation-dependent, and may include, for instance, a mutex being poisoned.
    #[inline]
    fn is_empty(&self) -> Result<bool, Self::InnerFileError> {
        Ok(self.try_get_ref()?.is_empty())
    }

    /// Access the inner buffer, and coerce the error type.
    ///
    /// Thin wrapper around [`try_get_ref`].
    ///
    /// # Fragility: Potential Panics or Deadlocks
    ///
    /// See [`try_get_ref`] for information.
    ///
    /// # Errors
    ///
    /// Errors are implementation-dependent. See [`try_get_ref`] for information.
    ///
    /// [`try_get_ref`]: generic_container::FragileTryContainer::try_get_ref
    #[inline]
    fn inner_buf(&self) -> Result<Self::Ref<'_>, Self::InnerFileError> {
        self.try_get_ref().map_err(Into::into)
    }

    /// Mutably access the inner buffer, and coerce the error type.
    ///
    /// Thin wrapper around [`try_get_mut`].
    ///
    /// # Fragility: Potential Panics or Deadlocks
    ///
    /// See [`try_get_mut`] for information.
    ///
    /// # Errors
    ///
    /// Errors are implementation-dependent. See [`try_get_mut`] for information.
    ///
    /// [`try_get_mut`]: FragileTryMutContainer::try_get_mut
    #[inline]
    fn inner_buf_mut(&mut self) -> Result<Self::RefMut<'_>, Self::InnerFileError> {
        self.try_get_mut().map_err(Into::into)
    }
}

impl<T: FragileTryMutContainer<Vec<u8>> + Clone> MemoryFileInner for T
where
    Self::RefMutError: From<Self::RefError>,
{
    type InnerFileError = Self::RefMutError;
}
