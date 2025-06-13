use std::error::Error;
use std::io::{Error as IoError, ErrorKind, Result as IoResult, Write};


/// A file interface exposing the combination of [`Seek`] and [`Read`] sometimes necessary for
/// LevelDB.
///
/// [`Read`]: std::io::Read
/// [`Seek`]: std::io::Seek
pub trait RandomAccess {
    /// Read up to `buf.len()`-many bytes from the file, starting at `offset`.
    ///
    /// On success, the number of bytes read is returned; this has the same semantics as the
    /// return value of [`read`].
    ///
    /// This method might not be threadsafe, as a thread performing a seek followed by a read
    /// could be interrupted, with a different thread seeking elsewhere in the file in the
    /// meantime. If an implementation *is* threadsafe, then the marker trait
    /// [`SyncRandomAccessFile`] should be implemented as well.
    ///
    ///
    /// [`read`]: std::io::Read::read
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> IoResult<usize>;

    /// Attempt to read exactly `buf.len()`-many bytes from the file, starting at `offset`.
    ///
    /// The function repeatedly calls [`read_at`] until either `buf.len()`-many bytes have been,
    /// or an error condition is met. Calls to [`read_at`] are retried when
    /// [`ErrorKind::Interrupted`] is returned.
    ///
    /// An error is returned both when [`read_at`] fails with a non-[`ErrorKind::Interrupted`]
    /// error, as well as when end-of-file is reached before `buf.len()`-many bytes are read.
    /// On error, the contents of `buf` are unspecified.
    /// This has the same semantics as as [`read_exact`], aside from the below caveat that will
    /// likely never apply:
    ///
    /// The buffer's length must fit in a u64;
    /// if the buffer isn't less than 16 exabytes in length, an error is returned.
    ///
    /// [`read_at`]: RandomAccess::read_at
    /// [`read_exact`]: std::io::Read::read_exact
    fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> IoResult<()> {

        if u64::try_from(buf.len()).is_err() {
            return Err(IoError::other(
                "cannot read into a buffer with a length of 16 exabytes or more",
            ));
        }

        let mut bytes_read = 0_usize;

        while bytes_read < buf.len() {
            // By the above check on `buf.len()`, and since `bytes_read` is smaller,
            // we know that `bytes_read` fits in a `u64`.
            #[expect(
                clippy::as_conversions,
                reason = "`bytes_read` is known to fit in u64",
            )]
            let bytes_read_u64 = bytes_read as u64;

            #[expect(
                clippy::indexing_slicing,
                reason = "`bytes_read < buf.len()`, so this does not panic",
            )]
            match self.read_at(offset + bytes_read_u64, &mut buf[bytes_read..]) {
                Ok(0) => break,
                Ok(additional_bytes) => {
                    bytes_read += additional_bytes;
                }
                #[expect(
                    clippy::ref_patterns,
                    reason = "the `Read::read_exact` impl uses `ref`",
                )]
                Err(ref err) if err.kind() == ErrorKind::Interrupted => {}
                Err(other_err) => return Err(other_err),
            }
        }

        if bytes_read == buf.len() {
            Ok(())
        } else {
            Err(ErrorKind::UnexpectedEof.into())
        }
    }
}

/// Implementing `SyncRandomAccess` asserts that the implementations of [`read_at`] and
/// [`read_exact_at`] are threadsafe.
///
/// As an example where this does not hold, a type implementing only [`Seek`] and [`Read`]
/// which does not use some form of synchronization cannot simply seek to the offset and then
/// read data; a different thread could seek elsewhere in the middle of those two calls.
///
/// In particular, a [`File`] only implements `SyncRandomAccess` on Unix or Windows; other
/// platforms require an `Arc<Mutex<File>>`. (If you wish to use a non-`std` arc or mutex,
/// you might need to make a newtype to implement this trait.)
///
/// [`File`]: std::fs::File
/// [`Read`]: std::io::Read
/// [`Seek`]: std::io::Seek
/// [`read_at`]: RandomAccess::read_at
/// [`read_exact_at`]: RandomAccess::read_exact_at
pub trait SyncRandomAccess: RandomAccess {}

/// A file obtained from a [`WritableFilesystem`] from either [`open_writable`] or
/// [`open_appendable`].
///
/// The implementation should provide buffering, likely with [`BufWriter`].
///
/// [`BufWriter`]: std::io::BufWriter
/// [`WritableFilesystem`]: crate::fs_traits::WritableFilesystem
/// [`open_writable`]: crate::fs_traits::WritableFilesystem::open_writable
/// [`open_appendable`]: crate::fs_traits::WritableFilesystem::open_appendable
pub trait WritableFile: Write {
    /// Ensures that data is flushed to disk, if the filesystem implementation is backed by
    /// an on-disk filesystem rather than being solely in-memory, to provide durability (ACID's D).
    ///
    /// Note that this can be quite expensive; ordinarily,
    /// the operating system is allowed to buffer writes and batch expensive writes to disk.
    /// (Note that performing syscalls to the operating system *also* has overhead,
    /// which is why wrapping a file in [`BufWriter`] is useful.)
    ///
    /// See [`File::sync_data`], which can be used to implement this method.
    ///
    /// [`File::sync_data`]: std::fs::File::sync_data
    fn sync_data(&mut self) -> IoResult<()>;
}

/// Basic interface for the [`ReadableFilesystem::Error`] associated type.
///
/// [`ReadableFilesystem::Error`]: crate::fs_traits::ReadableFilesystem::Error
pub trait FSError: Error {
    /// Whether the error occurred because a file, directory, or other filesystem entry
    /// could not be found at a given path.
    fn is_not_found(&self) -> bool;
    /// Whether the error occurred because a read, write, or other process was interrupted.
    fn is_interrupted(&self) -> bool;
    /// Whether the error occurred because a mutex was poisoned.
    fn is_poison_error(&self) -> bool;
}


/// Basic interface for the [`ReadableFilesystem::LockError`] associated type.
///
/// [`ReadableFilesystem::LockError`]: crate::fs_traits::ReadableFilesystem::LockError
pub trait FSLockError: Error {
    /// Whether the error occurred because the lockfile had already been locked.
    fn is_already_locked(&self) -> bool;
    /// Whether the error occurred because a file, directory, or other filesystem entry
    /// could not be found at a given path.
    fn is_not_found(&self) -> bool;
    /// Whether the error occurred because a read, write, or other process was interrupted.
    fn is_interrupted(&self) -> bool;
    /// Whether the error occurred because a mutex was poisoned.
    fn is_poison_error(&self) -> bool;
}
