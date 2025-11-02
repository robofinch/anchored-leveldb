use std::{error::Error as StdError, path::PathBuf};
use std::io::{Error as IoError, ErrorKind, Result as IoResult, Write};


/// A file interface exposing the combination of [`Seek`] and [`Read`] sometimes necessary for
/// LevelDB.
///
/// However, using [`Seek`] and [`Read`] is not necessarily threadsafe[^1], due to the file
/// cursor being shared across multiple threads, and changed by seeks or reads.
///
/// Implementations of [`RandomAccess`], however, are required to be logically threadsafe.
/// The results of [`RandomAccess::read_at`] and [`RandomAccess::read_exact_at`] must be correct
/// when called by any number of threads.
///
/// This may require some form of synchronization, or in the case of [`std::fs::File`],
/// operating system-specific support.
///
/// [^1]: Rust already requires all operations to be threadsafe in the sense of memory safety.
/// [`RandomAccess`] requires that the implementations are logically correct when accessed from
/// multiple threads, not merely memory safe.
///
/// [`Read`]: std::io::Read
/// [`Seek`]: std::io::Seek
pub trait RandomAccess {
    /// Read up to `buf.len()`-many bytes from the file, starting at `offset`.
    ///
    /// On success, the number of bytes read is returned; this has the same semantics as the
    /// return value of [`read`].
    ///
    /// This method must be threadsafe. In general, a thread performing a seek
    /// followed by a read could be interrupted, with a different thread seeking elsewhere in the
    /// file in the meantime. Implementors of `RandomAccess` must not allow this to occur.
    ///
    /// [`read`]: std::io::Read::read
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<usize>;

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
    /// (Technically, this refers not to 16 decimal exabytes but 16 exbibytes or binary exabytes.)
    ///
    /// [`read_at`]: RandomAccess::read_at
    /// [`read_exact`]: std::io::Read::read_exact
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<()> {

        if u64::try_from(buf.len()).is_err() {
            return Err(IoError::other(
                "cannot read into a buffer with a length of 16 binary exabytes (EiB) or more",
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
    /// Ensures that data is flushed to persistent storage, if the filesystem implementation can be
    /// persistent and not solely in-memory, to provide durability (ACID's D).
    ///
    /// Note that this can be quite expensive; ordinarily,
    /// the operating system is allowed to buffer writes and batch expensive writes to disk.
    /// (Note that performing syscalls to the operating system *also* has overhead,
    /// which is why wrapping a file in [`BufWriter`] is useful.)
    ///
    /// See [`File::sync_data`], which can be used to implement this method. Not all file-related
    /// metadata must be written to crash-proof persistent storage, but at least the file data
    /// and file size must be flushed to persistent storage.
    ///
    /// [`BufWriter`]: std::io::BufWriter
    /// [`File::sync_data`]: std::fs::File::sync_data
    fn sync_data(&mut self) -> IoResult<()>;
}

/// Provides an iterator over the immediate children of a directory, for
/// [`ReadableFilesystem::children`].
///
/// [`ReadableFilesystem::children`]: crate::fs_traits::ReadableFilesystem::children
pub trait IntoDirectoryIterator {
    /// Error type for the iterator returned by [`dir_iter`].
    ///
    /// [`dir_iter`]: IntoDirectoryIterator::dir_iter
    type DirIterError: StdError;

    /// Iterator over the immediate children of a directory, for [`ReadableFilesystem::children`].
    ///
    /// [`ReadableFilesystem::children`]: crate::fs_traits::ReadableFilesystem::children
    fn dir_iter(self) -> impl Iterator<Item = Result<PathBuf, Self::DirIterError>>;
}

/// Basic interface for the [`ReadableFilesystem::Error`] associated type.
///
/// [`ReadableFilesystem::Error`]: crate::fs_traits::ReadableFilesystem::Error
pub trait FSError: StdError {
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
pub trait FSLockError: StdError {
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
