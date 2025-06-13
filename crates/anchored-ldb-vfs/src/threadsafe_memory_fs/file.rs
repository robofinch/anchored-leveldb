use std::{
    io::{Read, Result as IoResult, Write},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::util_traits::{RandomAccess, WritableFile};
use super::error::MutexPoisoned;


#[derive(Default, Debug, Clone)]
pub(super) struct ThreadsafeFileInner(Arc<Mutex<Vec<u8>>>);

impl ThreadsafeFileInner {
    /// Return a `ThreadsafeMemoryFileInner` referencing a new, empty buffer.
    #[inline]
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Return the length of the buffer in bytes.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if the internal mutex is poisoned.
    #[inline]
    pub(super) fn len(&self) -> Result<usize, MutexPoisoned> {
        Ok(self.0.lock()?.len())
    }

    /// # Deadlocks
    ///
    /// Deadlocks if this thread has already locked this file, which may occur if we're inside
    /// a user-provided callback in some function accessing this file.
    ///
    /// Care must be taken to not deadlock within internal functions here.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if the internal mutex is poisoned.
    #[inline]
    pub(super) fn inner_buf_mut(&self) -> Result<MutexGuard<'_, Vec<u8>>, MutexPoisoned> {
        Ok(self.0.lock()?)
    }
}

impl Write for ThreadsafeFileInner {
    /// # Deadlocks
    ///
    /// Calling a user-given callback while a lock is held may allow a deadlock, and returning
    /// the `MutexGuard` to the user should be avoided for the same reason.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal mutex is poisoned.
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.inner_buf_mut()?.write(buf)
    }

    /// Does nothing, as the file is already backed by an in-memory buffer.
    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ThreadsafeMemoryFile {
    // TODO: check (benchmark) if it's worth wrapping `inner` in a `BufWriter`.
    inner:  ThreadsafeFileInner,
    offset: usize,
}

impl ThreadsafeMemoryFile {
    /// Mutably access the buffer backing the `ThreadsafeMemoryFile`.
    ///
    /// # Deadlocks
    /// If the provided callback accesses a `ThreadsafeMemoryFile` referencing the same inner
    /// buffer, the callback is extremely likely to trigger a deadlock. Such an access can occur if
    /// the callback utilizes the `ThreadsafeMemoryFS` which this `ThreadsafeMemoryFile` is a part
    /// of.
    ///
    /// So long as the callback does not have access to any `ThreadsafeMemoryFS`-related structs, a
    /// deadlock will not occur.
    ///
    /// Because this function takes `&mut self`, the callback cannot (safely) use a reference to
    /// the same `ThreadsafeMemoryFile` handle, but a pathological callback which accesses the inner
    /// buffer of this `ThreadsafeMemoryFile` via a different handle cannot be prevented at compile
    /// time.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if an internal mutex was poisoned.
    #[inline]
    pub fn access_file<T, F>(&mut self, callback: F) -> Result<T, MutexPoisoned>
    where
        F: FnOnce(&mut Vec<u8>) -> T,
    {
        Ok(callback(&mut *self.inner_buf_mut()?))
    }

    /// Returns the length of this file in bytes.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if the internal mutex is poisoned.
    #[inline]
    pub fn len(&self) -> Result<usize, MutexPoisoned> {
        self.inner.len()
    }

    /// Checks whether this file is empty.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if the internal mutex is poisoned.
    #[inline]
    pub fn is_empty(&self) -> Result<bool, MutexPoisoned> {
        self.inner.len().map(|len| len == 0)
    }
}

impl ThreadsafeMemoryFile {
    /// Return an empty `ThreadsafeMemoryFile`, with its file cursor/offset set to the start of the
    /// file.
    #[expect(dead_code, reason = "consistency (impl both `Default` and `new`)")]
    #[inline]
    pub(super) fn new() -> Self {
        Self {
            inner:  ThreadsafeFileInner::new(),
            offset: 0,
        }
    }

    /// Return a new `ThreadsafeMemoryFile` referencing the provided file buffer,
    /// with its file cursor/offset set to the start of the file.
    #[inline]
    pub(super) fn open(inner: &ThreadsafeFileInner) -> Self {
        Self {
            inner:  inner.clone(),
            offset: 0,
        }
    }

    /// Truncate the provided file buffer, and return a new `ThreadsafeMemoryFile` referencing that
    /// buffer, with its file cursor/offset set to the start of the file.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if an internal mutex is poisoned.
    pub(super) fn open_and_truncate(inner: &ThreadsafeFileInner) -> Result<Self, MutexPoisoned> {
        let cloned = inner.clone();
        cloned.inner_buf_mut()?.clear();

        Ok(Self {
            inner:  cloned,
            offset: 0,
        })
    }

    /// Return a new `ThreadsafeMemoryFile` referencing the provided file buffer,
    /// with its file cursor/offset set to the end of the file.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if an internal mutex is poisoned.
    pub(super) fn open_append(inner: &ThreadsafeFileInner) -> Result<Self, MutexPoisoned> {
        let cloned = inner.clone();
        let len = cloned.len()?;

        Ok(Self {
            inner:  cloned,
            offset: len,
        })
    }
}

impl ThreadsafeMemoryFile {
    /// # Deadlocks
    ///
    /// Deadlocks if this thread has already locked this file, which may occur if we're inside
    /// a user-provided callback in some function accessing this file.
    ///
    /// Care must be taken to not deadlock within internal functions here.
    ///
    /// # Errors
    ///
    /// Returns a [`MutexPoisoned`] error if the internal mutex is poisoned.
    #[inline]
    fn inner_buf_mut(&self) -> Result<MutexGuard<'_, Vec<u8>>, MutexPoisoned> {
        self.inner.inner_buf_mut()
    }

    /// Read into `buf` from `inner`, starting at offset `offset` within offset.
    /// Returns the number of bytes read.
    fn read_at_offset(offset: usize, inner: &[u8], buf: &mut [u8]) -> usize {

        if offset >= inner.len() {
            // There's no work to do. We read zero bytes.
            0
        } else {
            // We know this does not overflow, by the above check.
            #[expect(
                clippy::indexing_slicing,
                reason = "in this branch, `offset < inner.len()`",
            )]
            let source = &inner[offset..];

            // Truncating the buffers to `read_len` does not panic, since it's shorter than either
            // length.
            let read_len = source.len().min(buf.len());
            #[expect(
                clippy::indexing_slicing,
                reason = "`read_len` is less than either slice's length",
            )]
            buf[..read_len].copy_from_slice(&source[..read_len]);

            read_len
        }
    }
}

impl Read for ThreadsafeMemoryFile {
    /// Read up to `buf.len()`-many bytes into the provided buffer.
    ///
    /// The number of bytes read is returned, and the file cursor/offset of the
    /// `ThreadsafeMemoryFile` is moved forwards by that number.
    ///
    /// # Errors
    ///
    /// Returns an error if and only if an internal mutex was poisoned.
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let read_len = Self::read_at_offset(self.offset, &self.inner_buf_mut()?, buf);

        self.offset += read_len;
        Ok(read_len)
    }
}

impl RandomAccess for ThreadsafeMemoryFile {
    /// Read up to `buf.len()`-many bytes into the provided buffer, beginning from
    /// the indicated offset within this `ThreadsafeMemoryFile`.
    ///
    /// The number of bytes read is returned.
    ///
    /// The file cursor/offset of the `ThreadsafeMemoryFile` is unaffected.
    ///
    /// # Errors
    ///
    /// Returns an error if and only if an internal mutex was poisoned.
    #[inline]
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        let Ok(offset) = usize::try_from(offset) else {
            // If the offset is larger than `usize::MAX`, then it must be well past EOF for
            // our inner buffer.
            return Ok(0);
        };

        Ok(Self::read_at_offset(offset, &self.inner_buf_mut()?, buf))
    }
}

impl Write for ThreadsafeMemoryFile {
    /// Writes the full buffer to the end of the `ThreadsafeMemoryFile`, and returns the length of the buffer.
    ///
    /// Does not affect the file cursor/offset of this `ThreadsafeMemoryFile`.
    ///
    /// # Errors
    ///
    /// Returns an error if and only if an internal mutex was poisoned.
    #[inline]
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.inner_buf_mut()?.extend(buf);
        Ok(buf.len())
    }

    /// As `ThreadsafeMemoryFile` is backed by a buffer, it already writes directly to that buffer;
    /// therefore, this method does nothing.
    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl WritableFile for ThreadsafeMemoryFile {
    /// As `ThreadsafeMemoryFile` has no persistent filesystem to sync data to, and no extra buffer
    /// to flush, this method does nothing.
    #[inline]
    fn sync_data(&mut self) -> IoResult<()> {
        Ok(())
    }
}
