#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to satisfy NLL borrowck"),
)]

use std::io::Error as IoError;
use std::path::{Path, PathBuf};

use hashbrown::{HashMap, HashSet};
use normalize_path::NormalizePath as _;

use crate::util_traits::FSError;
use crate::fs_traits::{ReadableFilesystem, WritableFilesystem};
use super::{
    error::Error,
    file::MemoryFileWithInner,
    file_inner::MemoryFileInner,
    iter::IntoDirectoryIter,
};
use super::{
    aliases::{MemoryFSErr, MemoryFSFile, MemoryFSResult},
    lockfile::{LockError, Lockfile, Locks},
};


// NOTE: SOME ASSUMPTIONS IN CODE BELOW ARE LIKELY INCORRECT.
// Code saying "Checking invariants:" is allowed to make the assumption that there is only one
// root directory and there are no differences between relative paths from root and absolute paths
// starting at root. I know this is currently wrong, but rather than noting that at each
// "Checking invariants:" message, I will fix the problem at its root in the next commit.
// Right now, both `` and `/` might be treated as a root directory, or one or the other might
// return unexpected errors.

// TODO: make NormalizedPathBuf and NormalizedPath types, making use of `NormalizePath`,
// such that any user-given relative path is treated as a relative path from root;
// that is, an absolute path.

// TODO: explicitly document that any given path has any `..` and `.` components normalized away
// (noting that in the root directory, `..` refers to the root directory), has any
// Windows prefix ignored (e.g. `C:`), and that any relative path is evaluated starting from root;
// that is, relative paths and absolute paths are treated as the same, a leading `/` or backslash
// does not matter.


// TODO: document precisely what any error conditions are.
// TODO: link to `### "MemoryFS" and "MemoryFile"` from each instance of those terms.


#[expect(
    clippy::doc_markdown,
    reason = "There are no backticks around MemoryFS and MemoryFile in the header, as they are \
              already in quotes and look better in the header without both IMO; and they \
              semantically need quotes, as the terms themselves are discussed.",
)]
/// An in-memory virtual filesystem. Supports files and directories, but not links.
///
/// May be threadsafe or only suited for single-threaded purposes, depending on how the `InnerFile`
/// provides interior mutability to implement [`MemoryFileInner`]. An additional wrapper around the
/// whole filesystem may be necessary to use the `&mut self` methods in a multithreaded context.
///
/// ### "MemoryFS" and "MemoryFile"
///
/// Note that in the documentation for this type, the terms "`MemoryFS`" and "`MemoryFile`" are used to
/// refer to [`MemoryFSWithInner<InnerFile>`] and [`MemoryFileWithInner<Inner>`], or particular
/// generic variants, with the generic implied by context.
#[derive(Debug, PartialEq, Eq)]
pub struct MemoryFSWithInner<InnerFile> {
    /// Invariants (should be checked when mutating, or on initial creation):
    ///     - If a directory or file exists at a certain path, then every recursive parent of that
    ///       path exists and is a directory.
    ///     - It holds by the nature of `HashSets` and `HashMaps` that no two directories and
    ///       no two files exist at the same path; an invariant we must enforce is that no path
    ///       corresponds to both a file and directory.
    ///     - The root directory (whose path is the empty string) should always exist and cannot
    ///       be removed.
    directories: HashSet<PathBuf>,
    /// Invariants (should be checked when mutating, or on initial creation):
    ///     - If a directory or file exists at a certain path, then every recursive parent of that
    ///       path exists and is a directory.
    ///     - It holds by the nature of `HashSets` and `HashMaps` that no two directories and
    ///       no two files exist at the same path; an invariant we must enforce is that no path
    ///       corresponds to both a file and directory.
    ///     - Each `InnerFile` value has a different backing buffer. They shouldn't be clones of
    ///       each other. We must not expose the ability to directly mutate an `InnerFile` to the
    ///       user, lest they replace it with a clone of a different `InnerFile` in the `MemoryFS`.
    ///       Seems like I don't actually need to return an `&mut InnerFile` from any functions
    ///       here (whether in `fs.rs` or `file.rs`), so the only possible problem is when
    ///       inserting a file: inserted files should not have the same backing buffer as any other
    ///       file. Newly creating an `InnerFile` for insertion would satisfy this.
    files:       HashMap<PathBuf, InnerFile>,
    /// Invariants (should be checked when mutating):
    ///     - `locks` does not know what files actually exist. Thus, `self.locks.try_lock` must
    ///       only be called on files which are confirmed to exist.
    ///
    /// Any other invariants are enforced by `Locks` itself.
    ///
    /// Note that it *is* currently considered permissible for a lockfile to be removed from the
    /// filesystem, even if it is locked; in such a case, the file will still be considered to be
    /// locked. Moreover, a lockfile may be opened as a normal, readable or writable file,
    /// regardless of whether it's locked. That is to say, the locking functionality is somewhat
    /// independent of the rest of the filesystem. They're only advisory locks, after all.
    locks:       Locks,
}

impl<InnerFile> MemoryFSWithInner<InnerFile> {
    /// Create an empty `MemoryFS` with no files, and only the root directory.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            directories: HashSet::from([PathBuf::new()]),
            files:       HashMap::new(),
            locks:       Locks::new(),
        }
    }
}

impl<InnerFile: MemoryFileInner> MemoryFSWithInner<InnerFile> {
    /// Write `contents` to a new file at `path`, replacing the previous file contents if the file
    /// already existed.
    ///
    /// If `create_dir` is true, any missing parent directories are created.
    ///
    /// # Errors
    ///
    /// Errors with `IsADirectory` if a directory exists at the given path.
    ///
    /// If `create_dir` is not set and the parent of the given path is not an existing directory,
    /// then a `ParentNotFound` or `ParentIsAFile` error is returned, depending on whether the
    /// parent path refers to an existing file or not.
    ///
    /// If `create_dir` is set and any parent of the given path is a file, then a `ParentIsAFile`
    /// error is returned.
    ///
    /// Propagates any error from getting mutable access to the inner buffer of an `InnerFile`.
    pub fn write_file(
        &mut self,
        path:       &Path,
        create_dir: bool,
        contents:   Vec<u8>,
    ) -> MemoryFSResult<(), Self> {
        let path = path.normalize();

        let file = self.open_inner_file(path, create_dir)?;
        let mut file_buf_mut = file.inner_buf_mut()?;

        *file_buf_mut = contents;

        Ok(())
    }

    /// Access the buffer backing the file at the indicated path.
    ///
    /// # Panics or Deadlocks
    /// If the provided callback accesses the same file via a different handle (i.e., accesses a
    /// `MemoryFile` referencing the same inner buffer), the callback is extremely likely to
    /// trigger a panic or deadlock, depending on the `InnerFile` generic's implementation.
    ///
    /// If the callback does not have access to any `MemoryFS`-related structs, a
    /// panic or deadlock should not occur. Ideally, the callback should not capture any
    /// `MemoryFile`, or be capable of producing any `MemoryFile`.
    ///
    /// # Errors
    ///
    /// Returns a `NotFound` or `IsADirectory` error if a file does not exist at the given path.
    ///
    /// Propagates any error from getting access to the inner buffer of an `InnerFile`.
    pub fn access_file<T, F>(&mut self, path: &Path, callback: F) -> MemoryFSResult<T, Self>
    where
        F: FnOnce(&Vec<u8>) -> T,
    {
        let path = path.normalize();

        let file = self.get_inner_file(path)?;
        let file_buf = file.inner_buf()?;

        Ok(callback(&file_buf))
    }

    /// Mutably access the buffer backing the file at the indicated path.
    ///
    /// # Panics or Deadlocks
    /// If the provided callback accesses the same file via a different handle (i.e., accesses a
    /// `MemoryFile` referencing the same inner buffer), the callback is extremely likely to
    /// trigger a panic or deadlock, depending on the `InnerFile` generic's implementation.
    ///
    /// If the callback does not have access to any `MemoryFS`-related structs, a
    /// panic or deadlock should not occur. Ideally, the callback should not capture any
    /// `MemoryFile`, or be capable of producing any `MemoryFile`.
    ///
    /// # Errors
    ///
    /// Returns a `NotFound` or `IsADirectory` error if a file does not exist at the given path.
    ///
    /// Propagates any error from getting mutable access to the inner buffer of an `InnerFile`.
    pub fn access_file_mut<T, F>(&mut self, path: &Path, callback: F) -> MemoryFSResult<T, Self>
    where
        F: FnOnce(&mut Vec<u8>) -> T,
    {
        let path = path.normalize();

        let file = self.get_inner_file(path)?;
        let mut file_buf_mut = file.inner_buf_mut()?;

        Ok(callback(&mut file_buf_mut))
    }

    /// Iterate over the path of every file in the `MemoryFS`. The order in which paths are visited
    /// is not consistent.
    #[inline]
    pub fn file_paths(&mut self) -> impl Iterator<Item = &Path> {
        self.files
            .keys()
            .map(|path| &**path)
    }

    /// Iterate over every file in the `MemoryFS`. The order in which files are returned is not
    /// consistent.
    #[inline]
    pub fn files(&mut self) -> impl Iterator<Item = (&Path, MemoryFSFile<Self>)> {
        self.files
            .iter()
            .map(|(path, inner_file)| {
                let file = MemoryFileWithInner::open(inner_file);
                (&**path, file)
            })
    }

    /// Access the buffers backing each file in the `MemoryFS`. The order in which files
    /// are accessed is not consistent.
    ///
    /// If the provided callback or an `InnerFile` returns an error, iteration is halted early,
    /// and the error is returned.
    ///
    /// # Panics or Deadlocks
    /// If the provided callback accesses the same file via a different handle (i.e., accesses a
    /// `MemoryFile` referencing the same inner buffer), the callback is extremely likely to
    /// trigger a panic or deadlock, depending on the `InnerFile` generic's implementation.
    ///
    /// If the callback does not have access to any `MemoryFS`-related structs, a
    /// panic or deadlock should not occur. Ideally, the callback should not capture any
    /// `MemoryFile`, or be capable of producing any `MemoryFile`.
    ///
    /// # Errors
    ///
    /// If the callback ever returns an error, that error is returned.
    ///
    /// Propagates any error from getting access to the inner buffer of an `InnerFile`.
    pub fn access_files<Err, F>(&mut self, mut callback: F) -> Result<(), Err>
    where
        F:   FnMut(&Path, &Vec<u8>) -> Result<(), Err>,
        Err: From<InnerFile::InnerFileError>,
    {
        self.files
            .iter()
            .try_for_each(|(path, inner_file)| {
                let file_buf = inner_file.inner_buf()?;
                callback(path, &file_buf)
            })
    }

    /// Mutably access the buffers backing each file in the `MemoryFS`. The order in which files
    /// are accessed is not consistent.
    ///
    /// If the provided callback or an `InnerFile` returns an error, iteration is halted early,
    /// and the error is returned.
    ///
    /// # Panics or Deadlocks
    /// If the provided callback accesses the same file via a different handle (i.e., accesses a
    /// `MemoryFile` referencing the same inner buffer), the callback is extremely likely to
    /// trigger a panic or deadlock, depending on the `InnerFile` generic's implementation.
    ///
    /// If the callback does not have access to any `MemoryFS`-related structs, a
    /// panic or deadlock should not occur. Ideally, the callback should not capture any
    /// `MemoryFile`, or be capable of producing any `MemoryFile`.
    ///
    /// # Errors
    ///
    /// If the callback ever returns an error, that error is returned.
    ///
    /// Propagates any error from getting mutable access to the inner buffer of an `InnerFile`.
    pub fn access_files_mut<Err, F>(&mut self, mut callback: F) -> Result<(), Err>
    where
        F:   FnMut(&Path, &mut Vec<u8>) -> Result<(), Err>,
        Err: From<InnerFile::InnerFileError>,
    {
        self.files
            .iter()
            .try_for_each(|(path, inner_file)| {
                let mut file_buf_mut = inner_file.inner_buf_mut()?;
                callback(path, &mut file_buf_mut)
            })
    }

    /// Iterate over the path of every directory in the `MemoryFS`. The order in which paths are
    /// visited is not consistent.
    #[inline]
    pub fn directory_paths(&mut self) -> impl Iterator<Item = &Path> {
        self.directories
            .iter()
            .map(|path| &**path)
    }
}

impl<InnerFile: MemoryFileInner> MemoryFSWithInner<InnerFile> {
    /// Get the inner file at the given normalized path.
    ///
    /// # Errors
    ///
    /// Returns a `NotFound` or `IsADirectory` error if a file does not exist at the given
    /// normalized path.
    fn get_inner_file(&self, normalized_path: PathBuf) -> MemoryFSResult<&InnerFile, Self> {
        if let Some(file) = self.files.get(&normalized_path) {
            Ok(file)

        } else if self.directories.contains(&normalized_path) {
            Err(Error::IsADirectory(normalized_path))

        } else {
            Err(Error::NotFound(normalized_path))
        }
    }

    /// Confirms that a file exists at the given normalized path.
    ///
    /// # Errors
    ///
    /// Returns a `NotFound` or `IsADirectory` error if a file does not exist at the given
    /// normalized path.
    fn confirm_file_exists(&self, normalized_path: &Path) -> MemoryFSResult<(), Self> {
        if self.files.contains_key(normalized_path) {
            Ok(())
        } else if self.directories.contains(normalized_path) {
            Err(Error::IsADirectory(normalized_path.to_owned()))
        } else {
            Err(Error::NotFound(normalized_path.to_owned()))
        }
    }

    /// Confirms that a directory exists at the given normalized path.
    ///
    /// # Errors
    ///
    /// Returns a `NotFound` or `IsAFile` error if a directory does not exist at the given
    /// normalized path.
    fn confirm_directory_exists(&self, normalized_path: &Path) -> MemoryFSResult<(), Self> {
        if self.directories.contains(normalized_path) {
            Ok(())
        } else if self.files.contains_key(normalized_path) {
            Err(Error::IsAFile(normalized_path.to_owned()))
        } else {
            Err(Error::NotFound(normalized_path.to_owned()))
        }
    }

    /// Confirms that either the given path is the root directory, or the parent directory of the
    /// given path exists.
    ///
    /// # Errors
    ///
    /// If the given normalized path is not the root directory, and the path's parent does not
    /// refer to an existing directory, then either a `ParentNotFound` or `ParentIsAFile` error
    /// is returned. The former occurs if nothing exists at the parent path, and the latter if
    /// the parent path refers to a file.
    fn confirm_parent_dir_exists(&self, normalized_path: &Path) -> MemoryFSResult<(), Self> {
        let Some(parent) = normalized_path.parent() else {
            // This is the root directory.
            return Ok(());
        };

        if self.directories.contains(parent) {
            Ok(())
        } else if self.files.contains_key(parent) {
            Err(Error::ParentIsAFile(normalized_path.to_owned()))
        } else {
            Err(Error::ParentNotFound(normalized_path.to_owned()))
        }
    }

    /// Confirms that the given path, if it is a directory, has no children.
    ///
    /// The path might not refer to any existing entry, or be a file, but if the path is a
    /// directory and this function returns `Ok(())`, then the directory is empty.
    ///
    /// # Errors
    ///
    /// If there are any filesystem entries which are children of the provided path, a
    /// `NonemptyDirectory` error is returned.
    fn confirm_no_children(&self, normalized_dir_path: &Path) -> MemoryFSResult<(), Self> {
        // Check if there are any files which are children
        let contains_a_file = self.files
            .keys()
            .any(|file_path| file_path.starts_with(normalized_dir_path));

        if contains_a_file {
            return Err(Error::NonemptyDirectory(normalized_dir_path.to_owned()));
        }

        // Check if there are any directories which are children
        let contains_a_dir = self.directories
            .iter()
            .any(|dir_path| {
                dir_path.starts_with(normalized_dir_path) && dir_path != normalized_dir_path
            });

        if contains_a_dir {
            return Err(Error::NonemptyDirectory(normalized_dir_path.to_owned()));
        }

        // There are no children, we're good.
        Ok(())
    }

    /// Performs the same task as `WritableFilesystem::create_dir`, but without normalizing the
    /// input path.
    ///
    /// # Errors
    ///
    /// Errors with `DirectoryExists` or `FileExists` if a new directory cannot be created
    /// due to an existing entry being present.
    ///
    /// If a file exists at the parent path of `normalized_path`, returns a `ParentIsAFile` error,
    /// and if nothing exists at the parent path, a `ParentNotFound` error is returned.
    fn inner_create_dir(&mut self, normalized_path: PathBuf) -> MemoryFSResult<(), Self> {
        if self.directories.contains(&normalized_path) {
            Err(Error::DirectoryExists(normalized_path))

        } else if self.files.contains_key(&normalized_path) {
            Err(Error::FileExists(normalized_path))

        } else {
            // Nothing exists there yet. Check whether its parent exists and is a directory.
            self.confirm_parent_dir_exists(&normalized_path)?;

            // Checking invariants:
            //   - The direct parent directory of the path exists and is a directory.
            //     (Note that this path can't be the root directory,
            //     we'd have returned a `DirectoryExists` error.)
            //     By the invariants, we then know that any parents of that parent are existing
            //     directories.
            //   - We would have exited earlier if `self.files` contained a file at the target
            //     path; therefore, no file corresponds to `normalized_path`.
            //   - We aren't attempting to remove the root directory.
            self.directories.insert(normalized_path);
            Ok(())
        }
    }

    /// Performs the same task as `WritableFilesystem::create_dir_all`, but without normalizing the
    /// input path. Note that an error is not returned if a directory already exists at the target
    /// path.
    ///
    /// See [`std::fs::DirBuilder::create`] (and its private `create_dir_all` method).
    ///
    /// # Errors
    ///
    /// If a file exists at the given normalized path, an `IsAFile` error is returned, and if a
    /// file exists at any parent of that path, a `ParentIsAFile` error is returned (containing the
    /// path whose parent refers to a file).
    fn inner_create_dir_all(&mut self, normalized_path: PathBuf) -> MemoryFSResult<(), Self> {
        let normalized_path = match self.inner_create_dir(normalized_path) {
            Ok(()) | Err(Error::DirectoryExists(_)) => return Ok(()),
            // We can try to recover from this case by recursively creating parents.
            Err(Error::ParentNotFound(path))        => path,
            // To `inner_create_dir`, either a file or a directory existing constitutes an error.
            // To us, though, it would be fine for a directory to exist, so an `IsAFile` error
            // is the semantically correct choice.
            Err(Error::FileExists(file_path))       => return Err(Error::IsAFile(file_path)),
            // The only other error returned by `self.inner_create_dir` is `ParentIsAFile`,
            // at least at the moment.
            // (This matters for correctness of documentation, but not for correctness of behavior
            // of this function itself.)
            Err(other_err)                          => return Err(other_err),
        };

        // Further elaboration for the below `reason`: by `self.directories`' invariants, the root
        // directory always exists, so if `normalized_path` were the root directory, we would have
        // received a `DirectoryExists` error, and thus have returned `Ok(())`, and not gotten here.
        #[expect(
            clippy::unwrap_used,
            reason = "only the root path has no parent, and if we get here, it's not the root",
        )]
        let parent = normalized_path.parent().unwrap();

        // TODO: turn this recursion into iteration. Should be very straightforward.

        // Note: this recursive call cannot return a `IsAFile` error, since if the parent were
        // to refer to a file, then we would have returned a `ParentIsAFile` error above.
        // That is, this can only possibly error with `ParentIsAFile`, or succeed.
        self.inner_create_dir_all(parent.to_owned())?;

        // Checking invariants:
        //   - The parent directory of `normalized_path` exists or was created
        //     (else, the above `inner_create_dir_all` call would return an error).
        //   - A file does not exist at the path we want to create a directory
        //     (the first call to `self.inner_create_dir` would have caught that).
        //   - We are not attempting to remove the root directory.
        self.directories.insert(normalized_path);
        Ok(())
    }

    /// Internal function for getting a file at the given normalized path.
    ///
    /// If the file did not exist, it is created if possible. If `create_dir` is set, any
    /// missing parent directories are created.
    ///
    /// # Errors
    ///
    /// Errors with `IsADirectory` if a directory exists at the given path.
    ///
    /// If `create_dir` is not set and the parent of `normalized_path` is not an existing directory,
    /// then a `ParentNotFound` or `ParentIsAFile` error is returned, depending on whether the
    /// parent path refers to an existing file or not.
    ///
    /// If `create_dir` is set and any parent of the given normalized path is a file, then a
    /// `ParentIsAFile` error is returned.
    fn open_inner_file(
        &mut self,
        normalized_path: PathBuf,
        create_dir:      bool,
    ) -> MemoryFSResult<&InnerFile, Self> {
        // Return early if the file already exists, since usually, it probably will.
        // if let Some(inner_file) = self.files.get(&path) {
        //     return Ok(inner_file);
        // }

        // Though, unfortunately this is a case where Rust's current NLL borrow checker is overly
        // conservative; the newer, in-progress Polonius borrow checker accepts it.
        // To get this to work on stable Rust requires unsafe code.
        {
            #[cfg(feature = "polonius")]
            let this = &*self;
            #[cfg(not(feature = "polonius"))]
            let this = {
                let this: *const Self = self;
                // TODO: check that the safety comment lint gets triggered if I remove the
                // below comment.

                // SAFETY:
                // Because `this` came from a `&mut Self`...
                // - the pointer is properly aligned
                // - it is non-null
                // - it is dereferenceable, because the allocation pointed to
                //   is at least size `size_of::<Self>()`
                // - the value pointed to has not been mangled, it's still a valid value for `Self`
                // - the aliasing rules are satisfied, as proven by how the code compiles fine
                //   under Polonius; we don't use the `this` reference after the if-let block.
                unsafe { &*this }
            };

            if let Some(inner_file) = this.files.get(&normalized_path) {
                return Ok(inner_file);
            }
        }

        // The file doesn't already exist, so we need to try to create it.

        if self.directories.contains(&normalized_path) {
            return Err(Error::IsADirectory(normalized_path));
        }

        if create_dir {
            // By the invariants of `self.directories`, the root directory always exists.
            #[expect(
                clippy::unwrap_used,
                reason = "Only the root path has no parent, and if the path were root, we'd error \
                         with `IsADirectory` in the above check",
            )]
            let parent = normalized_path.parent().unwrap();

            match self.inner_create_dir_all(parent.to_owned()) {
                Ok(())                 => {},
                // This indicates that `parent` refers to an existing file. In this context, then,
                // the correct error is not to return a `IsAFile` (referring to parent), but
                // `ParentIsAFile` (referring to the given path).
                Err(Error::IsAFile(_)) => return Err(Error::ParentIsAFile(normalized_path)),
                // The only other error which `inner_create_dir_all` may return is `ParentIsAFile`,
                // at least at the moment.
                // This is only important for documentation (and perhaps semantic meaning of the
                // errors), not for correctness of this function.
                Err(other_err)         => return Err(other_err),
            }

        } else {
            // Note: if the given path were the root directory, then we would have returned an
            // `IsADirectory` error. Therefore, we need not mention the "if the given path
            // is not the root directory" condition in the error documentation.
            self.confirm_parent_dir_exists(&normalized_path)?;
        }

        // Checking invariants:
        //   - Any parent directories exist; either we confirmed the parent directory exists, or we
        //     successfully created its parent directory.
        //   - A directory does not exist at this path, since we checked that above,
        //     and we pass the *parent* of `normalized_path` to `inner_create_dir_all`, so
        //     we haven't created a directory at `normalized_path` within this function.
        //   - We insert a newly-created file.
        let file = self.files
            .entry(normalized_path)
            .or_insert(InnerFile::new());

        Ok(file)
    }
}

impl<InnerFile> Default for MemoryFSWithInner<InnerFile> {
    /// Create an empty `MemoryFS` with no files, and only the root directory.
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<InnerFile: MemoryFileInner> ReadableFilesystem for MemoryFSWithInner<InnerFile>
where
    InnerFile::InnerFileError: FSError,
    IoError:                   From<InnerFile::InnerFileError>,
{
    type ReadFile               = MemoryFSFile<Self>;
    type RandomAccessFile       = MemoryFSFile<Self>;
    type Error                  = MemoryFSErr<Self>;
    // I've got a feeling that this implies the requirement `InnerFile: 'static`, but I could
    // be wrong. In any case, the actual `InnerFile` wrappers I'm mainly concerned about
    // (namely, `Rc<RefCell<T>>` and `Arc<Mutex<T>>`) are 'static, so I won't worry *too* much.
    // That said...
    // TODO: what happens if I try to make an `InnerFile` which isn't 'static?
    type IntoDirectoryIter<'a>  = IntoDirectoryIter<'a, InnerFile> where InnerFile: 'a;
    type Lockfile               = Lockfile;
    type LockError              = LockError<MemoryFSErr<Self>>;

    /// Open a file which can be read from sequentially.
    ///
    /// Analogous to [`File::open`], though the resulting file might not be seekable.
    ///
    /// # Errors
    ///
    /// Returns a `NotFound` or `IsADirectory` error if a file does not exist at the given path.
    ///
    /// [`File::open`]: std::fs::File::open
    fn open_sequential(&mut self, path: &Path) -> Result<Self::ReadFile, Self::Error> {
        self.get_inner_file(path.normalize())
            .map(MemoryFileWithInner::open)
    }

    /// Open a file which may be read from at arbitrary positions.
    ///
    /// Analogous to [`File::open`], though the [`RandomAccess`] trait exposes less functionality.
    ///
    /// # Errors
    ///
    /// Returns a `NotFound` or `IsADirectory` error if a file does not exist at the given path.
    ///
    /// [`File::open`]: std::fs::File::open
    /// [`RandomAccess`]: crate::util_traits::RandomAccess
    fn open_random_access(&mut self, path: &Path) -> Result<Self::RandomAccessFile, Self::Error> {
        self.get_inner_file(path.normalize())
            .map(MemoryFileWithInner::open)
    }

    /// Infallibly check if a file or directory exists at the given path.
    ///
    /// Analogous to [`fs::exists`].
    ///
    /// [`fs::exists`]: std::fs::exists
    fn exists(&mut self, path: &Path) -> Result<bool, Self::Error> {
        let path = &path.normalize();

        Ok(self.files.contains_key(path) || self.directories.contains(path))
    }

    /// Returns the direct children of the directory at the provided path.
    /// The returned paths are relative to the provided path.
    ///
    /// If an iterator is successfully returned, that iterator is infallible (aside from
    /// pervasive issues like allocation errors).
    ///
    /// Analogous to [`fs::read_dir`].
    ///
    /// # Errors
    ///
    /// Returns a `NotFound` or `IsAFile` error if a directory does not exist at the given path.
    ///
    /// [`fs::read_dir`]: std::fs::read_dir
    fn children(&mut self, path: &Path) -> Result<
        Self::IntoDirectoryIter<'_>,
        Self::Error,
    > {
        let path = path.normalize();

        self.confirm_directory_exists(&path)?;

        Ok(Self::IntoDirectoryIter::<'_>::new(path, &self.files, &self.directories))
    }

    /// Returns the size of the file at the provided path in bytes.
    ///
    /// Analogous to using [`fs::metadata`] and getting the file length.
    ///
    /// # Errors
    ///
    /// Returns a `NotFound` or `IsADirectory` error if a file does not exist at the given path,
    /// or (hypothetically) a `FileTooLong` error if the file is at or above 16 exabytes in length.
    ///
    /// Propagates any error from getting access to the inner buffer of an `InnerFile`.
    ///
    /// [`fs::metadata`]: std::fs::metadata
    fn size_of(&mut self, path: &Path) -> Result<u64, Self::Error> {
        let path = path.normalize();

        let file = self.get_inner_file(path)?;
        let len = file.len()?;

        #[expect(
            clippy::map_err_ignore,
            reason = "the only possible error is that `len` is greater than `u64::MAX`",
        )]
        u64::try_from(len).map_err(|_| Error::FileTooLong(len))
    }

    /// Attempt to open a file at the provided path and lock it, for use as an advisory
    /// [`Lockfile`].
    ///
    /// Returns an error if the lock is already held or does not exist, and may return other
    /// errors depending on the implementation.
    ///
    /// Note that it *is* currently considered permissible for a lockfile to be removed from the
    /// filesystem, even if it is locked; in such a case, the file will still be considered to be
    /// locked. Moreover, a lockfile may be opened as a normal, readable or writable file,
    /// regardless of whether it's locked. That is to say, the locking functionality is somewhat
    /// independent of the rest of the filesystem. They're only advisory locks, after all.
    ///
    /// # Errors
    ///
    /// Returns an `AlreadyLocked` lock error if the file at that path was already locked, and
    /// a `NotFound` or `IsADirectory` error if a file does not exist at the given path.
    fn open_and_lock(&mut self, path: &Path) -> Result<Self::Lockfile, Self::LockError> {
        let path = path.normalize();

        self.confirm_file_exists(&path)?;
        // Checking invariants:
        //   - We checked the file exists before trying to lock it.
        self.locks.try_lock(path)
    }

    /// Unlock and close a [`Lockfile`]. This does not delete the lockfile.
    ///
    /// Note that it *is* currently considered permissible for a lockfile to be removed from the
    /// filesystem, even if it is locked; in such a case, the file will still be considered to be
    /// locked. Moreover, a lockfile may be opened as a normal, readable or writable file,
    /// regardless of whether it's locked. That is to say, the locking functionality is somewhat
    /// independent of the rest of the filesystem. They're only advisory locks, after all.
    /// Therefore, the given `lockfile` is not confirmed to exist (since it might not anymore).
    ///
    /// If multiple `MemoryFS` structs are created, and a [`Lockfile`] is created in
    /// one and is attempted to be unlocked in a different `MemoryFS`,
    /// then that attempt (or a following attempt with a [`Lockfile`] at the same path) may fail.
    /// This method only fails for such pathological uses, or if a lockfile is in some way cloned.
    /// If lockfiles are always unlocked in the `MemoryFS` they came from, and are not duplicated
    /// in some way, then this method is infallible.
    ///
    /// # Errors
    ///
    /// May return a `NotLocked` error, due to the reason described above.
    ///
    /// [`Lockfile`]: ReadableFilesystem::Lockfile
    fn unlock_and_close(&mut self, lockfile: Self::Lockfile) -> Result<(), Self::LockError> {
        // Checking invariants:
        //   - Not applicable, the invariant is only about `self.locks.try_lock`.
        self.locks.unlock(lockfile)
    }
}

impl<InnerFile: MemoryFileInner> WritableFilesystem for MemoryFSWithInner<InnerFile>
where
    InnerFile::InnerFileError: FSError,
    IoError:                   From<InnerFile::InnerFileError>,
{
    type WriteFile  = MemoryFSFile<Self>;
    type AppendFile = MemoryFSFile<Self>;

    /// Open a file for writing. This creates the file if it did not exist, and truncates the file
    /// if it does.
    ///
    /// If `create_dir` is set, any missing parent directories are created.
    ///
    /// Analogous to [`File::create`], with additional functionality for `create_dir`.
    ///
    /// # Errors
    ///
    /// Errors with `IsADirectory` if a directory exists at the given path.
    ///
    /// If `create_dir` is not set and the parent of the given path is not an existing directory,
    /// then a `ParentNotFound` or `ParentIsAFile` error is returned, depending on whether the
    /// parent path refers to an existing file or not.
    ///
    /// If `create_dir` is set and any parent of the given path is a file, then a `ParentIsAFile`
    /// error is returned.
    ///
    /// Propagates any error from mutably accessing the inner buffer of the `InnerFile` file.
    ///
    /// [`File::create`]: std::fs::File::create
    fn open_writable(
        &mut self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::WriteFile, Self::Error> {
        let path = path.normalize();

        let inner_file = self.open_inner_file(path, create_dir)?;
        MemoryFileWithInner::open_and_truncate(inner_file).map_err(Into::into)
    }

    /// Open a file for appending, so that writes will always occur at the end of files.
    /// This creates the file if it did not exist, and leaves previous contents unchanged if it
    /// did exist.
    ///
    /// If `create_dir` is set, any missing parent directories are created.
    ///
    /// Analogous to opening a file with both the [`append`] and [`create`] options enabled,
    /// with additional functionality for `create_dir`.
    ///
    /// # Errors
    ///
    /// Errors with `IsADirectory` if a directory exists at the given path.
    ///
    /// If `create_dir` is not set and the parent of the given path is not an existing directory,
    /// then a `ParentNotFound` or `ParentIsAFile` error is returned, depending on whether the
    /// parent path refers to an existing file or not.
    ///
    /// If `create_dir` is set and any parent of the given path is a file, then a `ParentIsAFile`
    /// error is returned.
    ///
    /// Propagates any error from accessing the inner buffer of the `InnerFile` file.
    ///
    /// [`append`]: std::fs::OpenOptions::append
    /// [`create`]: std::fs::OpenOptions::create
    fn open_appendable(
        &mut self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::WriteFile, Self::Error> {
        let path = path.normalize();

        let inner_file = self.open_inner_file(path, create_dir)?;
        MemoryFileWithInner::open_append(inner_file).map_err(Into::into)
    }

    /// Delete a file at the indicated path. Note that this does not invalidate existing file
    /// handles to this path.
    ///
    /// Analogous to [`fs::remove_file`].
    ///
    /// # Errors
    ///
    /// Returns a `NotFound` or `IsADirectory` error if a file does not exist at the given path.
    ///
    /// [`fs::remove_file`]: std::fs::remove_file
    fn delete(&mut self, path: &Path) -> Result<(), Self::Error> {
        let path = &path.normalize();

        self.confirm_file_exists(path)?;
        // Checking invariants:
        //   - It would be bad both semantically and for the invariants to remove a random
        //     directory. We checked that this path is a file, so we're fine.
        //   - This removes a file, doesn't create a file at the same path as a directory.
        //   - This removes and does not create a file, again doesn't apply.
        self.files.remove(path);
        Ok(())
    }

    /// Create an empty directory at the indicated path.
    ///
    /// Does not create parent directories; for most purposes, [`create_dir_all`] is likely better.
    ///
    /// Analogous to [`fs::create_dir`], or to `mkdir` on Unix.
    ///
    /// # Errors
    ///
    /// Errors with `DirectoryExists` or `FileExists` if a new directory cannot be created
    /// due to an existing entry being present.
    ///
    /// If a file exists at the parent of the given path, returns a `ParentIsAFile` error,
    /// and if nothing exists at the parent path, a `ParentNotFound` error is returned.
    ///
    /// [`fs::create_dir`]: std::fs::create_dir
    /// [`create_dir_all`]: MemoryFSWithInner::create_dir_all
    // TODO: does this link to the MemoryFSWithInner implementation of create_dir_all,
    // or to the trait entry for it?
    fn create_dir(&mut self, path: &Path) -> Result<(), Self::Error> {
        self.inner_create_dir(path.normalize())
    }

    /// Create an empty directory at the indicated path, and creates any missing parent directories.
    ///
    /// Analogous to [`fs::create_dir_all`].
    ///
    /// # Errors
    ///
    /// If a file exists at the given path, an `IsAFile` error is returned, and if a file exists at
    /// any parent of that path, a `ParentIsAFile` error is returned (containing the path whose
    /// parent refers to a file).
    ///
    /// [`fs::create_dir_all`]: std::fs::create_dir_all
    fn create_dir_all(&mut self, path: &Path) -> Result<(), Self::Error> {
        self.inner_create_dir_all(path.normalize())
    }

    /// Remove an empty directory at the indicated path. The root directory may not be removed.
    ///
    /// Analogous to [`fs::remove_dir`], or to `rmdir` on Unix.
    ///
    /// # Errors
    ///
    /// Returns a `NotFound` or `IsAFile` error if a directory does not exist at the given path.
    /// If the directory is found but is the root directory or a directory with children, returns
    /// a `RootDirectory` or `NonemptyDirectory` error, respectively, as only empty non-root
    /// directories may be removed.
    ///
    /// [`fs::remove_dir`]: std::fs::remove_dir
    fn remove_dir(&mut self, path: &Path) -> Result<(), Self::Error> {
        let path = path.normalize();

        self.confirm_directory_exists(&path)?;

        if path == Path::new("") {
            return Err(Error::RootDirectory);
        }
        self.confirm_no_children(&path)?;

        // Checking invariants:
        //   - This directory has no children (we checked just above), so removing it does not
        //     violate that each filesystem entry's parents should exist.
        //   - This removes and does not create a directory, so the second invariant
        //     isn't at risk.
        //   - If `path` were the root directory, we'd have returned with an error above.
        //     Therefore, we are not removing the root directory.
        self.directories.remove(&path);
        Ok(())
    }

    /// Rename a file or directory. Mostly follows the Unix convention for errors.
    ///
    /// Analogous to [`fs::rename`].
    ///
    /// Provided that a filesystem entry exists at `old`, and an entry could be opened or created
    /// at `new`:
    ///   - It is allowed to move a file to a path which either does not exist or is a file.
    ///     **WARNING**: this silently removes any previously-existing file at the `new` path.
    ///     Note that this does not invalidate any file handles to the file previously at `new`,
    ///     nor does it change those file handles to refer to the moved file.
    ///   - It is allowed to move a directory to a path which either does not exist or is an empty
    ///     directory, unless the new directory path is a child of the old directory path.
    ///   - Renaming any existing filesystem entry to itself is permitted (and does not mutate the
    ///     filesystem).
    ///
    /// Note that moving a directory is ***expensive***, as the filesystem is not implemented with
    /// a linked structure; it must move every entry within the old directory to its new location.
    ///
    /// # Errors
    ///
    /// - If `old` is a file:
    ///   - if `new` is a directory, returns a `DirectoryExists` error.
    ///   - if the parent path of `new` is not an existing directory,
    ///     returns a `ParentNotFound` or `ParentIsAFile` error depending on whether
    ///     the parent path refers to an existing file.
    /// - If `old` is a directory:
    ///   - if `new` is a recursive child of `old` (which excludes `old` itself), then a
    ///     `MoveIntoSelf` error is returned. This error takes precedence over the below errors.
    ///   - if `new` is a file, returns a `FileExists` error.
    ///   - if the parent path of `new` is not an existing directory, returns a `ParentNotFound` or
    ///     `ParentIsAFile` error depending on whether the parent path refers to an existing file.
    ///   - if `new` refers to an existing, nonempty directory, then a `NonemptyDirectory` error
    ///     is returned.
    /// - Otherwise, `old` does not exist, and a `NotFound` error is returned.
    ///
    /// [`fs::rename`]: std::fs::rename
    fn rename(&mut self, old: &Path, new: &Path) -> Result<(), Self::Error> {
        let old = old.normalize();
        let new = new.normalize();

        if self.files.contains_key(&old) {
            // Can't hurt to optimize this trivial case.
            if old == new {
                Ok(())

            } else if self.directories.contains(&new) {
                Err(Error::DirectoryExists(new))

            } else {
                // Overwrite any file at `new` with the file at `old`
                self.confirm_parent_dir_exists(&new)?;

                // Checking invariants:
                //   - We aren't removing a directory, so this can't violate the first.
                //   - We aren't creating a file or directory, so the second isn't at risk.
                //   - We aren't creating a file, so the third isn't at risk.
                #[expect(
                    clippy::unwrap_used,
                    reason = "we checked that `files` contains the key `old`",
                )]
                let file = self.files.remove(&old).unwrap();

                // Checking invariants:
                //   - We checked that the parent directory of `new` exists, and by the invariants
                //     of `directories`, that implies that any recursive parents exist.
                //   - We're creating a file at `new`, so we need to know that no directory exists
                //     at `new`. We checked that above.
                //   - By the invariants of `files`, the `InnerFile` at `old` (which is `file`)
                //     was not duplicated anywhere else inside the filesystem. Therefore, nothing
                //     else in the filesystem has the same backing buffer as `file`, so we can
                //     insert `file`.
                self.files.insert(new, file);
                Ok(())
            }

        } else if self.directories.contains(&old) {
            // Can't hurt to optimize this trivial case. Moving a directory is somewhat expensive,
            // after all.
            if old == new {
                return Ok(());
            }
            // We can't move a directory into itself
            if new.starts_with(&old) {
                return Err(Error::MoveIntoSelf(old));
            }
            if self.files.contains_key(&new) {
                return Err(Error::FileExists(new));
            }

            self.confirm_parent_dir_exists(&new)?;
            // Only an empty directory may be overwritten. This checks that if `new` is a directory,
            // then it is empty.
            self.confirm_no_children(&new)?;

            // We've confirmed that `new` either doesn't exist, or is an empty directory.
            // In the former case, we can create a directory at `new`, since its parent directory
            // exists. We can proceed.

            // Overwrite any (empty) directory at `new` with the directory at `old`....
            // which requires moving a bunch of other entries.

            // Move files
            // Checking invariants:
            //   - We're removing only files, so there's no risk of leaving a dangling filesystem
            //     entry with no parent directory.
            //   - We're not creating a file, so we can't create a file at the same path
            //     as a directory.
            //   - We're not creating a file, so there's no concerns with backing buffers.
            #[allow(
                clippy::needless_collect,
                reason = "false positive: the `collect` is needed to later mutate `files`",
            )]
            let files_to_move = self.files
                .extract_if(|file_path, _| file_path.starts_with(&old))
                .collect::<Vec<_>>();

            let renamed_files = files_to_move
                .into_iter()
                .map(|(old_path, file)| {
                    // TODO: the below note is not currently true, see the top of the file.
                    // But it will be.

                    // NOTE: we normalize paths to be absolute paths, so `old` has a leading `/`.
                    // Therefore, even if `old` has no other components, it at least strips the
                    // leading `/` from `old_path`, and thus `rel_path` is actually a relative
                    // path.
                    #[expect(
                        clippy::unwrap_used,
                        reason = "These paths are filtered to start with the `old` prefix",
                    )]
                    let rel_path = old_path.strip_prefix(&old).unwrap();

                    (new.join(rel_path), file)
                });

            // Checking invariants:
            //   - WE ARE TEMPORARILY VIOLATING THIS INVARIANT (for the purpose of better memory
            //     efficiency: the `Vec` moved into `renamed_files` can be dropped before we
            //     create the next `dirs_to_move` vector).
            //     Below, after we have moved the directories, the invariant will be restored:
            //     we will have moved an entire branch of the filesystem tree to a new position,
            //     at `new`. The `new` directory's parent exists, so the moved branch will not
            //     have any dangling files or directories which have no parent.
            //     For now, there are dangling files.
            //   - The `new` directory had no children, and since every file path in `renamed_files`
            //     comes from joining `new` with a relative path (which came from part of a
            //     normalized path which has no `..` in it), it follows that every file path
            //     in `renamed_files` is a path starting with `new`. Moreover, since `old` was
            //     a directory, no to-be-renamed file could have been at `old`, and thus could not
            //     be renamed to `new`. Therefore, since `new` had no children, and every file path
            //     in `renamed_files` is *strictly* a child of `new` (and not `new` itself),
            //     creating these files causes no conflict between files and directories.
            //   - Each file being renamed was previously in the filesystem, and thus had/has a
            //     different backing buffer from anything else in the filesystem. Readding these
            //     files is thus fine; their backing buffers remain unique in the filesystem.
            self.files.extend(renamed_files);

            // Move directories - this includes all (recursive) children, as well as the
            // directory we're moving.
            // Checking invariants:
            //    - Note that this variant is currently violated, in the `new` part of the FS.
            //      However, this removal does not further violate the invariant: we already
            //      removed any file which is a recursive child of `old`, so by removing all
            //      directories which are `old` or any of its recursive children, no files or
            //      directories are left without a parent in the `old` branch of the filesystem.
            //    - We are removing, not adding, directories.
            //    - We are not attempting to remove the root directory; this needs to be justified.
            //      The root directory is not the child of anything, so if this step were to remove
            //      the root directory, then `old` would need to be root. If we were moving the
            //      root directory to itself, then `old == new` (and both are root), and we would
            //      have successfully returned by now. If we were moving the root directory to a
            //      file, we would have returned an error. If we were moving the root directory to
            //      a different directory, then that directory must be inside root; thus, we'd
            //      have returned a `MoveIntoSelf` error. Thus, if we get here, we will not remove
            //      the root directory.
            #[allow(
                clippy::needless_collect,
                reason = "false positive: the `collect` is needed to then mutate `directories`",
            )]
            let dirs_to_move = self.directories
                .extract_if(|dir_path| dir_path.starts_with(&old))
                .collect::<Vec<_>>();

            let renamed_dirs = dirs_to_move
                .into_iter()
                .map(|old_path| {
                    // TODO: the below note is not currently true, see the top of the file.
                    // But it will be.

                    // NOTE: we normalize paths to be absolute paths, so `old` has a leading `/`.
                    // Therefore, even if `old` has no other components, it at least strips the
                    // leading `/` from `old_path`, and thus `rel_path` is actually a relative
                    // path.
                    #[expect(
                        clippy::unwrap_used,
                        reason = "These paths are filtered to start with the `old` prefix",
                    )]
                    let rel_path = old_path.strip_prefix(&old).unwrap();

                    new.join(rel_path)
                });

            // Checking invariants:
            //   - Note that `new` is among the paths in `renamed_dirs`. This operation
            //     creates the `new` directory (if it didn't already exist), and we checked that
            //     the parent directory of `new` exists. Note that we did remove directories after
            //     checking that the parent directory of `new` exists, but we also checked that
            //     `new` does not start with `old` (and we only removed directories starting with
            //     `old`), so the parent directory of `new` does not start with `old` either,
            //     and thus the parent directory of `new` exists. The newly-created `new` directory
            //     will thus not be dangling without a parent.
            //     All other files and directories created in this `new` directory were recursive
            //     children of `old`, and are moved to their corresponding positions within `new`.
            //     This operation completes a move of the entire branch of the filesystem starting
            //     in `old` to a branch starting in `new`; so since the start of the branch will
            //     have a parent, it will hold that every newly-created directory or file created
            //     above will have a parent directory.
            //     TLDR:
            //     the invariant is restored after this operation. The all-caps warning above will
            //     no longer apply.
            //   - We're creating directories in relative paths starting at `new` which were their
            //     relative paths starting at `old`. We did the same thing for files. Next, by
            //     the invariants of `directories` and `files`, there was no relative path starting
            //     at `old` which would refer to both a file and directory. Thus, `renamed_dirs`
            //     does not contain the path of any file newly created above. The `new` directory,
            //     if it existed, had no children, so there were no other files already in this
            //     branch of the filesystem. So, we're good.
            //   - We are creating directories, so we are not attempting to remove the root
            //     directory.
            self.directories.extend(renamed_dirs);

            Ok(())

        } else {
            Err(Error::NotFound(old))
        }
    }

    /// Attempt to open a file at the provided path and lock it.
    ///
    /// If the file does not exist, it is created, and if `create_dir` is `true`, then its parent
    /// directories are created first (if they do not exist).
    ///
    /// Note that it *is* currently considered permissible for a lockfile to be removed from the
    /// filesystem, even if it is locked; in such a case, the file will still be considered to be
    /// locked. Moreover, a lockfile may be opened as a normal, readable or writable file,
    /// regardless of whether it's locked. That is to say, the locking functionality is somewhat
    /// independent of the rest of the filesystem. They're only advisory locks, after all.
    ///
    /// # Errors
    ///
    /// Returns an `AlreadyLocked` error if the path was already locked.
    ///
    /// Errors with `IsADirectory` if a directory exists at the given path.
    ///
    /// If `create_dir` is not set and the parent of the given path is not an existing directory,
    /// then a `ParentNotFound` or `ParentIsAFile` error is returned, depending on whether the
    /// parent path refers to an existing file or not.
    ///
    /// If `create_dir` is set and any parent of the given path is a file, then a `ParentIsAFile`
    /// error is returned.
    fn create_and_lock(
        &mut self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::Lockfile, Self::LockError> {
        let path = path.normalize();

        self.open_inner_file(path.clone(), create_dir)?;
        // Checking invariants:
        //   - We just created the file, so it exists.
        self.locks.try_lock(path)
    }
}
