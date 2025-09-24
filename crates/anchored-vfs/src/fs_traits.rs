use std::{io::Read, path::Path};

use crate::util_traits::{FSError, FSLockError, IntoDirectoryIterator, RandomAccess, WritableFile};


// TODO: provide macros that test a RFS or WFS implementation.
// TODO: make these dyn-compatible, or make dyn-compatible sibling traits.

// ================================================================
//  Main filesystem traits
// ================================================================

pub trait ReadableFilesystem {
    /// A file which can be read from sequentially.
    type ReadFile:               Read;
    /// A file which may be read from at arbitrary positions.
    type RandomAccessFile:       RandomAccess;
    /// Error type for most operations. If possible, individual methods should document what errors
    /// the method may return; however, a method returning a new type of error may be considered
    /// a minor change, especially if this `Error` type (or some part of it) is marked
    /// `#[non_exhaustive]`.
    type Error:                  FSError;

    /// Provides an iterator over the immediate children of a directory, for
    /// [`ReadableFilesystem::children`].
    type IntoDirectoryIter<'a>:  IntoDirectoryIterator where Self: 'a;

    /// A file acting as an advisory lock, such as a `LOCK` file for LevelDB, which can indicate to
    /// other programs using the same lockfile that some resource is being used.
    ///
    /// Should not be [`Clone`]able, in order to avoid misuse.
    type Lockfile;
    /// Error type for lockfile-related operations. If possible, individual methods should
    /// document what errors the method may return; however, a method returning a new type of error
    /// may be considered a minor change, especially if this `Error` type (or some part of it) is
    /// marked `#[non_exhaustive]`.
    type LockError:              FSLockError;

    /// Open a file which can be read from sequentially.
    ///
    /// Analogous to [`File::open`], though the resulting file might not be seekable.
    ///
    /// [`File::open`]: std::fs::File::open
    fn open_sequential(&self, path: &Path) -> Result<Self::ReadFile, Self::Error>;

    /// Open a file which may be read from at arbitrary positions.
    ///
    /// Analogous to [`File::open`], though the [`RandomAccess`] trait exposes less functionality.
    ///
    /// [`File::open`]: std::fs::File::open
    fn open_random_access(&self, path: &Path) -> Result<Self::RandomAccessFile, Self::Error>;

    /// Checks whether a filesystem entity (e.g. file or directory) exists at the provided path.
    ///
    /// Analogous to [`fs::exists`].
    ///
    /// [`fs::exists`]: std::fs::exists
    fn exists(&self, path: &Path) -> Result<bool, Self::Error>;

    /// Returns an iterator over the paths of entries directly contained in the directory at the
    /// provided path. The returned paths are relative to the provided path.
    ///
    /// Analogous to [`fs::read_dir`].
    ///
    /// [`fs::read_dir`]: std::fs::read_dir
    fn children(&self, path: &Path) -> Result<Self::IntoDirectoryIter<'_>, Self::Error>;

    /// Returns the size of the file at the provided path in bytes.
    ///
    /// Analogous to using [`fs::metadata`] and getting the file length.
    ///
    /// [`fs::metadata`]: std::fs::metadata
    fn size_of(&self, path: &Path) -> Result<u64, Self::Error>;

    // TODO: function which opens a file or directory at the provided path and calls
    // either fsync or fcntl(FULL_SYNC) on it.
    // fn fsync(&self, path: &Path) -> Result<(), Self::Error>;

    /// Attempt to open a file at the provided path and lock it, for use as an advisory
    /// [`Lockfile`].
    ///
    /// Returns an error if the lock is already held or does not exist, and may return other
    /// errors depending on the implementation.
    ///
    /// Depending on the implementation, it may or may not be possible for this process to open
    /// the locked file as readable or writable.
    ///
    /// [`Lockfile`]: ReadableFilesystem::Lockfile
    fn open_and_lock(&mut self, path: &Path) -> Result<Self::Lockfile, Self::LockError>;

    /// Unlock and close a [`Lockfile`]. This does not delete the lockfile.
    ///
    /// [`Lockfile`]: ReadableFilesystem::Lockfile
    fn unlock_and_close(&mut self, lockfile: Self::Lockfile) -> Result<(), Self::LockError>;
}

pub trait WritableFilesystem: ReadableFilesystem {
    /// A file which can be written to sequentially, beginning at the start of the newly
    /// created or truncated file.
    ///
    /// Analogous to a file opened by [`File::create`], but with no exposed capability to seek.
    ///
    /// [`File::create`]: std::fs::File::create
    type WriteFile:  WritableFile;
    /// A file whose end may be appended to sequentially.
    ///
    /// Analogous to a file opened with both the [`append`] and [`create`] options enabled,
    /// but with no exposed capability to seek.
    ///
    /// [`append`]: std::fs::OpenOptions::append
    /// [`create`]: std::fs::OpenOptions::create
    type AppendFile: WritableFile;

    /// Open a file for writing. This creates the file if it did not exist, and truncates the file
    /// if it does.
    ///
    /// If `create_dir` is set, any missing parent directories are created.
    ///
    /// Analogous to [`File::create`], with additional functionality for `create_dir`.
    ///
    /// [`File::create`]: std::fs::File::create
    fn open_writable(
        &mut self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::WriteFile, Self::Error>;

    /// Open a file for appending, so that writes will always occur at the end of files.
    /// This creates the file if it did not exist, and leaves previous contents unchanged if it
    /// did exist.
    ///
    /// If `create_dir` is set, any missing parent directories are created.
    ///
    /// Analogous to opening a file with both the [`append`] and [`create`] options enabled,
    /// with additional functionality for `create_dir`.
    ///
    /// [`append`]: std::fs::OpenOptions::append
    /// [`create`]: std::fs::OpenOptions::create
    fn open_appendable(
        &mut self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::AppendFile, Self::Error>;

    /// Delete a file at the indicated path.
    ///
    /// Analogous to [`fs::remove_file`].
    ///
    /// [`fs::remove_file`]: std::fs::remove_file
    fn delete(&mut self, path: &Path) -> Result<(), Self::Error>;

    /// Create an empty directory at the indicated path.
    ///
    /// Does not create parent directories; for most purposes, [`create_dir_all`] is likely better.
    ///
    /// Analogous to [`fs::create_dir`], or to `mkdir` on Unix.
    ///
    /// [`fs::create_dir`]: std::fs::create_dir
    /// [`create_dir_all`]: WritableFilesystem::create_dir_all
    fn create_dir(&mut self, path: &Path) -> Result<(), Self::Error>;

    /// Create an empty directory at the indicated path, and creates any missing parent directories.
    ///
    /// Analogous to [`fs::create_dir_all`].
    ///
    /// [`fs::create_dir_all`]: std::fs::create_dir_all
    fn create_dir_all(&mut self, path: &Path) -> Result<(), Self::Error>;

    /// Remove an empty directory at the indicated path.
    ///
    /// Analogous to [`fs::remove_dir`], or to `rmdir` on Unix.
    ///
    /// [`fs::remove_dir`]: std::fs::remove_dir
    fn remove_dir(&mut self, path: &Path) -> Result<(), Self::Error>;

    /// Rename a file or directory. If a file or directory already exists at `new`, it may
    /// be silently deleted, or an error may be returned.
    ///
    /// See [`fs::rename`] for platform-specific behavior. A custom, virtual filesystem should
    /// document its behavior, and at least support the Unix convention: renaming a file to the
    /// path of an existing file is permitted (and deletes the file previously at `new`), and
    /// renaming a directory to the path of an empty directory is permitted (and overwrites that
    /// empty directory).
    ///
    /// [`fs::rename`]: std::fs::rename
    fn rename(&mut self, old: &Path, new: &Path) -> Result<(), Self::Error>;

    /// Attempt to open a file at the provided path and lock it.
    ///
    /// If the file does not exist, it is created, and if `create_dir` is `true`, then its parent
    /// directories are created first (if they do not exist).
    ///
    /// Returns an error if the lock is already held, and may return other errors depending on the
    /// implementation.
    ///
    /// Depending on the implementation, it may or may not be possible for this process to open
    /// the locked file as readable or writable.
    fn create_and_lock(
        &mut self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::Lockfile, Self::LockError>;
}
