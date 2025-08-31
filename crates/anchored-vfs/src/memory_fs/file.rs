use std::io::{Error as IoError, Read, Result as IoResult, Write};

use crate::util_traits::{RandomAccess, WritableFile};
use super::file_inner::MemoryFileInner;


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryFileWithInner<InnerFile> {
    // TODO: check (benchmark) if it's worth wrapping `inner` in a `BufWriter`,
    // or having a buffer of our own. It would make things more complicated, and perhaps
    // more error-prone if multiple threads are reading and writing the same file: data races
    // would become a problem. Though they're already a problem for `StandardFS`, so maybe that's
    // just not an issue.
    // However, it could be more performant to not constantly need to lock/unlock or do reference
    // count checks on the thing.
    inner:  InnerFile,
    offset: usize,
}

impl<InnerFile: MemoryFileInner> MemoryFileWithInner<InnerFile> {
    /// Returns the length of this file in bytes.
    ///
    /// # Errors
    ///
    /// Propagates any error from accessing the inner buffer of the `InnerFile` file.
    #[inline]
    pub fn len(&self) -> Result<usize, InnerFile::InnerFileError> {
        self.inner.len()
    }

    /// Checks whether the file has a length of zero bytes.
    ///
    /// # Errors
    ///
    /// Propagates any error from accessing the inner buffer of the `InnerFile` file.
    #[inline]
    pub fn is_empty(&self) -> Result<bool, InnerFile::InnerFileError> {
        self.inner.is_empty()
    }

    /// Access the buffer backing the `MemoryFile`.
    ///
    /// # Panics or Deadlocks
    /// If the provided callback accesses a `MemoryFile` referencing the same inner
    /// buffer, the callback is extremely likely to trigger a panic or deadlock, depending on the
    /// `InnerFile` generic's implementation.
    ///
    /// If the callback does not have access to any `MemoryFS`-related structs, a panic or deadlock
    /// should not occur. Ideally, the callback should not capture any `MemoryFile`, or be capable
    /// of producing any `MemoryFile`.
    ///
    /// # Errors
    ///
    /// Propagates any error from accessing the inner buffer of the `InnerFile` file.
    #[inline]
    pub fn access_file<T, F>(&self, callback: F) -> Result<T, InnerFile::InnerFileError>
    where
        F: FnOnce(&Vec<u8>) -> T,
    {
        let buf_ref = self.inner.inner_buf()?;
        Ok(callback(&buf_ref))
    }

    /// Mutably access the buffer backing the `MemoryFile`.
    ///
    /// # Panics or Deadlocks
    /// If the provided callback accesses a `MemoryFile` referencing the same inner
    /// buffer, the callback is extremely likely to trigger a panic or deadlock, depending on the
    /// `InnerFile` generic's implementation.
    ///
    /// If the callback does not have access to any `MemoryFS`-related structs, a panic or deadlock
    /// should not occur. Ideally, the callback should not capture any `MemoryFile`, or be capable
    /// of producing any `MemoryFile`.
    ///
    /// # Errors
    ///
    /// Propagates any error from mutably accessing the inner buffer of the `InnerFile` file.
    #[inline]
    pub fn access_file_mut<T, F>(&mut self, callback: F) -> Result<T, InnerFile::RefMutError>
    where
        F: FnOnce(&mut Vec<u8>) -> T,
    {
        let mut buf_mut = self.inner.try_get_mut()?;
        Ok(callback(&mut buf_mut))
    }
}

impl<InnerFile: MemoryFileInner> MemoryFileWithInner<InnerFile> {
    /// Return an empty `MemoryFile`, with its file cursor/offset set to the start of the file.
    #[inline]
    pub(super) fn new() -> Self {
        Self {
            inner:  InnerFile::new(),
            offset: 0,
        }
    }

    /// Return a new `MemoryFile` referencing the provided file buffer, with its file cursor/offset
    /// set to the start of the file.
    #[inline]
    pub(super) fn open(inner: &InnerFile) -> Self {
        Self {
            inner:  inner.clone(),
            offset: 0,
        }
    }

    /// Truncate the provided file buffer, and return a new `MemoryFile` referencing that
    /// buffer, with its file cursor/offset set to the start of the file.
    ///
    /// # Errors
    ///
    /// Propagates any error from mutably accessing the inner buffer of the `InnerFile` file.
    pub(super) fn open_and_truncate(inner: &InnerFile) -> Result<Self, InnerFile::InnerFileError> {
        let inner = inner.clone();
        inner.inner_buf_mut()?.clear();

        Ok(Self {
            inner,
            offset: 0,
        })
    }
}

impl<InnerFile: MemoryFileInner> MemoryFileWithInner<InnerFile> {
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

impl<InnerFile: MemoryFileInner> Default for MemoryFileWithInner<InnerFile> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<InnerFile: MemoryFileInner> Read for MemoryFileWithInner<InnerFile>
where
    IoError: From<InnerFile::InnerFileError>,
{
    /// Read up to `buf.len()`-many bytes into the provided buffer, starting from the current
    /// file offset of the `MemoryFile`.
    ///
    /// The number of bytes read is returned, and the file cursor/offset of the
    /// `MemoryFile` is moved forwards by that number.
    ///
    /// # Errors
    ///
    /// Propagates any error from accessing the inner buffer of the `InnerFile` file.
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let inner_buf = self.inner.inner_buf()?;

        let read_len = Self::read_at_offset(self.offset, &inner_buf, buf);

        self.offset += read_len;
        Ok(read_len)
    }
}

impl<InnerFile: MemoryFileInner> RandomAccess for MemoryFileWithInner<InnerFile>
where
    IoError: From<InnerFile::InnerFileError>,
{
    /// Read up to `buf.len()`-many bytes into the provided buffer, beginning from
    /// the indicated offset within this `MemoryFile`.
    ///
    /// The number of bytes read is returned.
    ///
    /// The file cursor/offset of the `MemoryFile` is unaffected.
    ///
    /// # Errors
    ///
    /// Propagates any error from accessing the inner buffer of the `InnerFile` file.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        let Ok(offset) = usize::try_from(offset) else {
            // If the offset is larger than `usize::MAX`, then it must be well past EOF for
            // our inner buffer.
            return Ok(0);
        };

        let inner_buf_mut = &self.inner.inner_buf_mut()?;

        Ok(Self::read_at_offset(offset, inner_buf_mut, buf))
    }
}

impl<InnerFile: MemoryFileInner> Write for MemoryFileWithInner<InnerFile>
where
    IoError: From<InnerFile::InnerFileError>,
{
    /// Writes the full buffer to the end of the `MemoryFile`, and returns the length of the
    /// buffer.
    ///
    /// Does not affect the file cursor/offset of this `MemoryFile`.
    ///
    /// # Errors
    ///
    /// Propagates any error from mutably accessing the inner buffer of the `InnerFile` file.
    #[inline]
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.inner.inner_buf_mut()?.extend(buf);
        Ok(buf.len())
    }

    /// Does nothing, as the file is already backed by an in-memory buffer.
    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl<InnerFile: MemoryFileInner> WritableFile for MemoryFileWithInner<InnerFile>
where
    IoError: From<InnerFile::InnerFileError>,
{
    /// As the `MemoryFile` has no persistent filesystem to sync data to, and no extra buffer
    /// to flush, this method does nothing.
    #[inline]
    fn sync_data(&mut self) -> IoResult<()> {
        Ok(())
    }
}
