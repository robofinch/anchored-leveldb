use std::{io::Read, path::Path};

use crate::util_traits::{FSError, FSLockError, IntoChildFileIterator, RandomAccess, WritableFile};


// TODO: provide macros that test FS implementations.

#[derive(Debug, Clone, Copy)]
pub enum CreateParentDir {
    True,
    False,
}

impl From<bool> for CreateParentDir {
    #[inline]
    fn from(value: bool) -> Self {
        if value { Self::True } else { Self::False }
    }
}

impl From<CreateParentDir> for bool {
    #[inline]
    fn from(value: CreateParentDir) -> Self {
        matches!(value, CreateParentDir::True)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SyncParentDir {
    True,
    False,
}

impl From<bool> for SyncParentDir {
    #[inline]
    fn from(value: bool) -> Self {
        if value { Self::True } else { Self::False }
    }
}

impl From<SyncParentDir> for bool {
    #[inline]
    fn from(value: SyncParentDir) -> Self {
        matches!(value, SyncParentDir::True)
    }
}

// ================================================================
//  Main filesystem traits
// ================================================================

// pub trait ReadOnlyLevelDBFilesystem {

// }

/// # Opening files
/// There are many implementation-specific restrictions. Note in particular that this trait
/// does not guarantee that a writable handle to a file can be coexist with other handles to
/// that file; this limitation is imposed to improve the performance of [`MemoryFS`].
pub trait LevelDBFilesystem {
    /// A file which can be read from sequentially.
    type ReadFile:         Read;
    /// A file which may be read from at arbitrary positions.
    type RandomAccessFile: RandomAccess;
    /// A file which can be written to, created by [`open_writable`] or [`open_appendable`].
    ///
    /// Analogous to a file opened by [`File::create`] or a file opened with both the [`append`]
    /// and [`create`] options enabled, but with no exposed capability to seek.
    ///
    /// [`File::create`]: std::fs::File::create
    /// [`append`]: std::fs::OpenOptions::append
    /// [`create`]: std::fs::OpenOptions::create
    /// [`open_writable`]: LevelDBFilesystem::open_writable
    /// [`open_appendable`]: LevelDBFilesystem::open_appendable
    type WriteFile:        WritableFile;
    /// Provides an iterator over the paths of files directly contained in a directory, for
    /// [`LevelDBFilesystem::child_files`].
    ///
    /// The child paths are relative to the directory path.
    type ChildFiles<'a>:   IntoChildFileIterator where Self: 'a;
    /// A file acting as an advisory lock, such as a `LOCK` file for LevelDB, which can indicate to
    /// other programs using the same lockfile that some resource is being used.
    ///
    /// Should not be [`Clone`]able, in order to avoid misuse.
    type Lockfile;
    /// Error type for lockfile-related operations. If possible, individual methods should
    /// document what errors the method may return; however, a method returning a new type of error
    /// may be considered a minor change, especially if this `Error` type (or some part of it) is
    /// marked `#[non_exhaustive]`.
    type LockError:        FSLockError;
    /// Error type for most operations. If possible, individual methods should document what errors
    /// the method may return; however, a method returning a new type of error may be considered
    /// a minor change, especially if this `Error` type (or some part of it) is marked
    /// `#[non_exhaustive]`.
    type Error:            FSError;
    // TODO: memory-mapped files

    // Open a file which can be read from sequentially.
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

    /// Open a file for writing. This creates the file if it did not exist, and truncates the file
    /// if it does.
    ///
    /// If `create_dir` is set, any missing parent directories are created.
    ///
    /// If `sync_dir` is set, then the directory entries of filesystem entities created by this
    /// function will be flushed to persistent storage (if this filesystem implementation
    /// is persistent) to ensure that the created file will not be lost after a crash.
    /// See [`WritableFile::sync_data`].
    ///
    /// Analogous to [`File::create`], with additional functionality for `create_dir`.
    ///
    /// # Panics
    /// If another handle to the file at `path` is open when this function is called or is later
    /// opened before the returned `WriteFile` is dropped, a panic may occur. That is, Rust's
    /// "aliasing XOR mutation" rules apply.
    ///
    /// [`File::create`]: std::fs::File::create
    fn open_writable(
        &self,
        path:       &Path,
        create_dir: CreateParentDir,
        sync_dir:   SyncParentDir,
    ) -> Result<Self::WriteFile, Self::Error>;

    /// Returns `true` when the filesystem is known to efficiently support appendable files.
    ///
    /// Some filesystems, such as the Origin Private File System on the web, may have substantial
    /// overhead for appendable files compared to truncated writable files.
    #[must_use]
    fn supports_efficient_appendable(&self) -> bool;

    /// Open a file for appending, so that writes will always occur at the end of files.
    /// This creates the file if it did not exist, and leaves previous contents unchanged if it
    /// did exist.
    ///
    /// If `create_dir` is set, any missing parent directories are created.
    ///
    /// If `sync_dir` is set, then the directory entries of filesystem entities created by this
    /// function will be flushed to persistent storage (if this filesystem implementation
    /// is persistent) to ensure that the created file will not be lost after a crash.
    /// See [`WritableFile::sync_data`].
    ///
    /// Calling this function is analogous to opening a file with both the [`append`] and [`create`]
    /// options enabled, with additional functionality for `create_dir`.
    ///
    /// # Panics
    /// If another handle to the file at `path` is open when this function is called or is later
    /// opened before the returned `WriteFile` is dropped, a panic may occur. That is, Rust's
    /// "aliasing XOR mutation" rules apply.
    ///
    /// [`append`]: std::fs::OpenOptions::append
    /// [`create`]: std::fs::OpenOptions::create
    fn open_appendable(
        &self,
        path:       &Path,
        create_dir: CreateParentDir,
        sync_dir:   SyncParentDir,
    ) -> Result<Self::WriteFile, Self::Error>;

    /// Rename a file or directory. If `from` and `to` are existing files, the original file at
    /// `to` should be replaced with the file at `from`.
    ///
    /// If a directory exists at `to`, an error may or may not be returned. See [`fs::rename`]
    /// for platform-specific behavior. A custom, virtual filesystem should document its behavior,
    /// and at least support the Unix convention: renaming a directory to the path of an empty
    /// directory is permitted (and overwrites that empty directory).
    ///
    /// If `sync_dir` is set, then data of the parent directory of `to` will be flushed to
    /// persistent storage (if this filesystem implementation is persistent) to ensure that
    /// a successful rename will not be forgotten after a crash. See [`WritableFile::sync_data`].
    ///
    /// When renaming a file to replace an existing file, the implementation should ensure that
    /// either the old or new file is always visible at the `to` path; that is, if this
    /// filesystem supports concurrent accesses, this function does not simply execute
    /// [`remove_file`] and then move the `from` file to `to`. However, this guarantee does not
    /// extend to crash consistency; on some filesystems, a crash at the wrong time
    /// is permitted to cause no file to exist at the `to` path (and perhaps not at the `from` path
    /// either).
    ///
    /// [`fs::rename`]: std::fs::rename
    /// [`remove_file`]: LevelDBFilesystem::remove_file
    fn rename(&self, from: &Path, to: &Path, sync_dir: SyncParentDir) -> Result<(), Self::Error>;

    /// Delete a file at the indicated path.
    ///
    /// Analogous to [`fs::remove_file`].
    ///
    /// [`fs::remove_file`]: std::fs::remove_file
    fn remove_file(&self, path: &Path) -> Result<(), Self::Error>;

    /// Remove an empty directory at the indicated path.
    ///
    /// Analogous to [`fs::remove_dir`], or to `rmdir` on Unix.
    ///
    /// [`fs::remove_dir`]: std::fs::remove_dir
    fn remove_dir(&self, path: &Path) -> Result<(), Self::Error>;

    /// Returns the size of the file at the provided path in bytes.
    ///
    /// Analogous to using [`fs::metadata`] and getting the file length.
    ///
    /// [`fs::metadata`]: std::fs::metadata
    fn size_of_file(&self, path: &Path) -> Result<u64, Self::Error>;

    /// Checks whether a normal file exists at the provided path.
    ///
    /// Analogous to [`fs::exists`], but filtered for only regular files. An error is returned
    /// not only if the filesystem entry cannot be confirmed to exist or not exist, but also
    /// if its type cannot be determined.
    ///
    /// [`fs::exists`]: std::fs::exists
    fn file_exists(&self, path: &Path) -> Result<bool, Self::Error>;

    /// Returns an iterator over the paths of files directly contained in the directory at the
    /// provided path. The returned paths are relative to the provided path.
    ///
    /// Symlinks are traversed.
    ///
    /// Analogous to [`fs::read_dir`], filtered to only iterate over files.
    ///
    /// [`fs::read_dir`]: std::fs::read_dir
    fn child_files(&mut self, path: &Path) -> Result<Self::ChildFiles<'_>, Self::Error>;

    /// Attempt to open a file at the provided path and lock it.
    ///
    /// Depending on the implementation, it may or may not be possible for this process to open
    /// the locked file as readable or writable.
    ///
    /// If the file does not exist, it is created, and if `create_dir` is `true`, then its parent
    /// directories are created first (if they do not exist).
    ///
    /// If `sync_dir` is set, then the directory entries of filesystem entities created by this
    /// function will be flushed to persistent storage (if this filesystem implementation
    /// is persistent) to ensure that the created file will not be lost after a crash.
    /// See [`WritableFile::sync_data`].
    ///
    /// # Errors
    /// Returns an error if the lock is already held, and may return other errors depending on the
    /// implementation.
    fn create_and_lock(
        &mut self,
        path:       &Path,
        create_dir: CreateParentDir,
        sync_dir:   SyncParentDir,
    ) -> Result<Self::Lockfile, Self::LockError>;

    /// Unlock and close a [`Lockfile`]. This does not delete the lockfile.
    ///
    /// [`Lockfile`]: ReadableFilesystem::Lockfile
    fn unlock_and_close(&mut self, lockfile: Self::Lockfile) -> Result<(), Self::LockError>;
}
