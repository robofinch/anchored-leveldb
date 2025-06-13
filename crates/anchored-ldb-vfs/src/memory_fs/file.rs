use std::rc::Rc;
use std::{
    cell::{Ref, RefCell, RefMut},
    io::{Read, Result as IoResult, Write},
};

use crate::util_traits::{RandomAccess, WritableFile};


#[derive(Default, Debug, Clone)]
pub(super) struct MemoryFileInner(Rc<RefCell<Vec<u8>>>);

impl MemoryFileInner {
    /// Return a `MemoryFileInner` referencing a new, empty buffer.
    #[inline]
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Return the length of the buffer.
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.0.borrow().len()
    }

    /// # Panics
    ///
    /// Panics if the inner `RefCell` is already mutably borrowed. Since `Rc` is not `Send` or
    /// `Sync`, we only need to worry about a mutable borrow persisting for too long or a borrow
    /// being stacked within the same thread.
    ///
    /// Calling a user-given callback while a borrow is active may cause a panic, and returning
    /// a `cell::Ref` to the user should be avoided for the same reason.
    #[inline]
    pub(super) fn inner_buf(&self) -> Ref<'_, Vec<u8>> {
        self.0.borrow()
    }

    /// # Panics
    ///
    /// Panics if the inner `RefCell` is already borrowed. Since `Rc` is not `Send` or
    /// `Sync`, we only need to worry about a mutable borrow persisting for too long or a borrow
    /// being stacked within the same thread.
    ///
    /// Calling a user-given callback while a borrow is active may cause a panic, and returning
    /// a `cell::RefMut` to the user should be avoided for the same reason.
    #[inline]
    pub(super) fn inner_buf_mut(&self) -> RefMut<'_, Vec<u8>> {
        self.0.borrow_mut()
    }
}

impl Write for MemoryFileInner {
    /// # Panics
    ///
    /// Panics if the inner buffer of `MemoryFileInner` is already borrowed.
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.0.borrow_mut().write(buf)
    }

    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MemoryFile {
    // TODO: check (benchmark) if it's worth wrapping `inner` in a `BufWriter`.
    inner:  MemoryFileInner,
    offset: usize,
}

impl MemoryFile {
    /// Mutably access the buffer backing the `MemoryFile`.
    ///
    /// # Panics
    /// If the provided callback accesses a `MemoryFile` referencing the same inner buffer,
    /// the callback is extremely likely to trigger a panic. Such an access can occur if the
    /// callback utilizes the `MemoryFS` which this `MemoryFile` is a part of.
    ///
    /// So long as the callback does not have access to any `MemoryFS`-related structs, a panic
    /// will not occur.
    ///
    /// Because this function takes `&mut self`, the callback cannot (safely) use a reference to
    /// the same `MemoryFile` handle, but a pathological callback which accesses the inner buffer
    /// of this `MemoryFile` via a different handle cannot be prevented at compile time.
    pub fn access_file<T, F>(&mut self, callback: F) -> T
    where
        F: FnOnce(&mut Vec<u8>) -> T,
    {
        callback(&mut self.inner_buf_mut())
    }
}

impl MemoryFile {
    /// Return an empty `MemoryFile`, with its file cursor/offset set to the start of the file.
    #[expect(dead_code, reason = "consistency (impl both `Default` and `new`)")]
    #[inline]
    pub(super) fn new() -> Self {
        Self {
            inner:  MemoryFileInner::new(),
            offset: 0,
        }
    }

    /// Return a new `MemoryFile` referencing the provided file buffer,
    /// with its file cursor/offset set to the start of the file.
    #[inline]
    pub(super) fn open(inner: &MemoryFileInner) -> Self {
        Self {
            inner:  inner.clone(),
            offset: 0,
        }
    }

    /// Truncate the provided file buffer, and return a new `MemoryFile` referencing that buffer,
    /// with its file cursor/offset set to the start of the file.
    pub(super) fn open_and_truncate(inner: &MemoryFileInner) -> Self {
        let cloned = inner.clone();
        cloned.inner_buf_mut().clear();

        Self {
            inner:  cloned,
            offset: 0,
        }
    }

    /// Return a new `MemoryFile` referencing the provided file buffer,
    /// with its file cursor/offset set to the end of the file.
    pub(super) fn open_append(inner: &MemoryFileInner) -> Self {
        let cloned = inner.clone();
        let len = cloned.len();

        Self {
            inner:  cloned,
            offset: len,
        }
    }
}

impl MemoryFile {
    /// # Panics
    ///
    /// Panics if the inner `RefCell` is already mutably borrowed. Since `Rc` is not `Send` or
    /// `Sync`, we only need to worry about a mutable borrow persisting for too long or a borrow
    /// being stacked within the same thread.
    ///
    /// Calling a user-given callback while a borrow is active may cause a panic, and returning
    /// a `cell::Ref` to the user should be avoided for the same reason.
    ///
    /// A sufficient condition to *not* panic is thus to call `inner_buf` at most *once* within
    /// the methods of `MemoryFile`, and to not call other `self`-taking methods of `MemoryFile`
    /// within any implementation of a method.
    #[inline]
    fn inner_buf(&self) -> Ref<'_, Vec<u8>> {
        self.inner.inner_buf()
    }

    /// # Panics
    ///
    /// Panics if the inner `RefCell` is already borrowed. Since `Rc` is not `Send` or
    /// `Sync`, we only need to worry about a mutable borrow persisting for too long or a borrow
    /// being stacked within the same thread.
    ///
    /// Calling a user-given callback while a borrow is active may cause a panic, and returning
    /// a `cell::RefMut` to the user should be avoided for the same reason.
    ///
    /// A sufficient condition to *not* panic is thus to call `inner_buf` at most *once* within
    /// the methods of `MemoryFile`, and to not call other `self`-taking methods of `MemoryFile`
    /// within any implementation of a method.
    #[inline]
    fn inner_buf_mut(&self) -> RefMut<'_, Vec<u8>> {
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

impl Read for MemoryFile {
    /// Infallibly read up to `buf.len()`-many bytes into the provided buffer.
    ///
    /// The number of bytes read is returned, and the file cursor/offset of the `MemoryFile`
    /// is moved forwards by that number.
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let read_len = Self::read_at_offset(self.offset, &self.inner_buf(), buf);

        self.offset += read_len;
        Ok(read_len)
    }
}

impl RandomAccess for MemoryFile {
    /// Infallibly read up to `buf.len()`-many bytes into the provided buffer, beginning from
    /// the indicated offset within this `MemoryFile`.
    ///
    /// The number of bytes read is returned.
    ///
    /// The file cursor/offset of the `MemoryFile` is unaffected.
    #[inline]
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        let Ok(offset) = usize::try_from(offset) else {
            // If the offset is larger than `usize::MAX`, then it must be well past EOF for
            // our inner buffer.
            return Ok(0);
        };

        Ok(Self::read_at_offset(offset, &self.inner_buf(), buf))
    }
}

impl Write for MemoryFile {
    /// Writes the full buffer to the end of the `MemoryFile`, and returns the length of the buffer.
    ///
    /// Infallible, provided that the potential allocation succeeds,
    /// and does not affect the file cursor/offset of this `MemoryFile`.
    #[inline]
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.inner_buf_mut().extend(buf);
        Ok(buf.len())
    }

    /// As `MemoryFile` is backed by a buffer, it already writes directly to that buffer;
    /// therefore, this method does nothing.
    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl WritableFile for MemoryFile {
    /// As `MemoryFile` has no persistent filesystem to sync data to, and no extra buffer to flush,
    /// this method does nothing.
    #[inline]
    fn sync_data(&mut self) -> IoResult<()> {
        Ok(())
    }
}
