use std::{cell::RefCell, convert::Infallible};
use std::path::{Path, PathBuf};

use hashbrown::{HashMap, HashSet};
use normalize_path::NormalizePath as _;

use crate::fs_traits::{ReadableFilesystem, WritableFilesystem};
use super::error::Error;
use super::{
    file::{MemoryFile, MemoryFileInner},
    lockfile::{LockError, Lockfile, Locks},
};

// TODO: document precisely what any error conditions are.

#[derive(Default, Debug)]
pub struct MemoryFS {
    directories: RefCell<HashSet<PathBuf>>,
    files:       RefCell<HashMap<PathBuf, MemoryFileInner>>,
    locks:       Locks,
}

impl MemoryFS {
    /// Create an empty `MemoryFS`, with no files or directories.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Write `contents` to a new file at `path`, replacing the previous file contents if the file
    /// already existed.
    ///
    /// If `create_dir` is true, any missing parent directories are created.
    pub fn write_file(
        &self,
        path:       PathBuf,
        create_dir: bool,
        contents:   Vec<u8>,
    ) -> Result<(), Error> {
        self.create_inner_file(path, create_dir, |file| {
            *file.inner_buf_mut() = contents;

            Ok(())
        })
    }

    /// Mutably access the buffer backing the file at the indicated path.
    ///
    /// # Panics
    /// If the provided callback accesses the same file via a different handle, the callback is
    /// extremely likely to trigger a panic. To avoid such an access and panic, using any
    /// `MemoryFS`-related functions or structs should be avoided inside the callback.
    ///
    /// Because this function takes `&mut self`, the callback cannot (without UB) use a reference to
    /// the `MemoryFS` itself, but a pathological callback could capture a `MemoryFile` referencing
    /// the file at `path`.
    pub fn access_file<T, F>(&mut self, path: &Path, callback: F) -> Result<T, Error>
    where
        F: FnOnce(&mut Vec<u8>) -> T,
    {
        let path = path.normalize();

        self.access_file_inner(path, |file| {
            Ok(callback(&mut file.inner_buf_mut()))
        })
    }

    /// Iterate over the path of every file in the `MemoryFS`. The order of paths is not consistent.
    pub fn file_paths(&mut self) -> impl Iterator<Item = &Path> {
        self.files
            .get_mut()
            .keys()
            .map(|path| &**path)
    }

    /// Iterate over the path of every directory in the `MemoryFS`. The order of paths is not
    /// consistent.
    pub fn directory_paths(&mut self) -> impl Iterator<Item = &Path> {
        self.directories
            .get_mut()
            .iter()
            .map(|path| &**path)
    }

    /// Iterate over every file in the `MemoryFS`. The order in which files are returned is not
    /// consistent.
    pub fn files(&mut self) -> impl Iterator<Item = (&Path, MemoryFile)> {
        self.files
            .get_mut()
            .iter()
            .map(|(path, inner)| (&**path, MemoryFile::open(inner)))
    }

    /// Mutably access the buffers backing each file in the `MemoryFS`. The order in which files are
    /// accessed is not consistent.
    ///
    /// # Panics
    /// If the provided callback accesses the `MemoryFS` or any `MemoryFile` contained in it
    /// (through means other than the provided file path and buffer), a panic is extremely likely
    /// to occur. To avoid such an access and panic, any `MemoryFS`-related functions or structs
    /// should be avoided inside the callback.
    ///
    /// Because this function takes `&mut self`, the callback cannot (without UB) use a reference to
    /// the `MemoryFS` itself, but a pathological callback could capture a `MemoryFile`.
    pub fn access_files<F>(&mut self, mut callback: F)
    where
        F: FnMut(&Path, &mut Vec<u8>),
    {
        self.files
            .get_mut()
            .iter()
            .for_each(|(path, inner_file)| {
                callback(path, &mut inner_file.inner_buf_mut());
            });
    }

    /// Mutably access the buffers backing each file in the `MemoryFS`. The order in which files are
    /// accessed is not consistent.
    ///
    /// If the provided callback returns an error, iteration is halted early, and the error is
    /// returned.
    ///
    /// # Panics
    /// If the provided callback accesses the `MemoryFS` or any `MemoryFile` contained in it
    /// (through means other than the provided file path and buffer), a panic is extremely likely
    /// to occur. To avoid such an access and panic, any `MemoryFS`-related functions or structs
    /// should be avoided inside the callback.
    ///
    /// Because this function takes `&mut self`, the callback cannot (without UB) use a reference to
    /// the `MemoryFS` itself, but a pathological callback could capture a `MemoryFile`.
    pub fn try_access_files<Err, F>(&mut self, mut callback: F) -> Result<(), Err>
    where
        F: FnMut(&Path, &mut Vec<u8>) -> Result<(), Err>,
    {
        self.files
            .get_mut()
            .iter()
            .try_for_each(|(path, inner_file)| {
                callback(path, &mut inner_file.inner_buf_mut())
            })
    }
}

impl MemoryFS {
    /// Call a function on the file at the given normalized path.
    ///
    /// # Panics
    /// If the provided callback accesses the same file via a different handle, the callback is
    /// extremely likely to trigger a panic. To avoid such an access and panic, using any
    /// `MemoryFS`-related functions or structs should be avoided inside the callback.
    fn access_file_inner<T, F>(&self, path: PathBuf, callback: F) -> Result<T, Error>
    where
        F: FnOnce(&MemoryFileInner) -> Result<T, Error>,
    {
        if let Some(file) = self.files.borrow().get(&path) {
            callback(file)

        } else if self.directories.borrow().contains(&path) {
            Err(Error::IsADirectory(path))

        } else {
            Err(Error::NotFound(path))
        }
    }

    /// Errors if a file does not exist at the given normalized path.
    fn assert_file_exists(&self, path: &Path) -> Result<(), Error> {
        if self.files.borrow().contains_key(path) {
            Ok(())
        } else if self.directories.borrow().contains(path) {
            Err(Error::IsADirectory(path.to_owned()))
        } else {
            Err(Error::NotFound(path.to_owned()))
        }
    }

    /// Errors if a directory does not exist at the given normalized path.
    fn assert_directory_exists(&self, path: &Path) -> Result<(), Error> {
        if self.directories.borrow().contains(path) {
            Ok(())
        } else if self.files.borrow().contains_key(path) {
            Err(Error::IsAFile(path.to_owned()))
        } else {
            Err(Error::NotFound(path.to_owned()))
        }
    }

    /// Performs the same task as `WritableFilesystem::create_dir`, but without normalizing the
    /// input path.
    fn inner_create_dir(&self, path: PathBuf) -> Result<(), Error> {
        let mut directories = self.directories.borrow_mut();

        if directories.contains(&path) {
            Err(Error::DirectoryExists(path))
        } else if self.files.borrow().contains_key(&path) {
            Err(Error::FileExists(path))
        } else {
            // Nothing exists there yet. Check whether its parent exists.
            let parent_exists = path.parent().is_none_or(|parent| {
                directories.contains(parent)
            });

            if parent_exists {
                directories.insert(path);
                Ok(())
            } else {
                Err(Error::NotFound(path))
            }
        }
    }

    /// Performs the same task as `WritableFilesystem::create_dir_all`, but without normalizing the
    /// input path.
    ///
    /// See [`std::fs::DirBuilder::create`] (and its private `create_dir_all` method).
    fn inner_create_dir_all(&self, path: PathBuf) -> Result<(), Error> {
        if path == Path::new("") {
            return Ok(());
        }

        let path = match self.inner_create_dir(path) {
            Ok(()) | Err(Error::DirectoryExists(_)) => return Ok(()),
            Err(Error::NotFound(path))              => path,
            Err(other_err)                          => return Err(other_err),
        };
        match path.parent() {
            Some(parent) => self.inner_create_dir_all(parent.to_owned())?,
            None         => return Err(Error::TreePartiallyCreated),
        }
        match self.inner_create_dir(path) {
            Ok(()) | Err(Error::DirectoryExists(_)) => Ok(()),
            Err(other_err)                          => Err(other_err),
        }
    }

    /// Internal function for opening a file at the normalized path `path` (creating the file if
    /// it did not exist, and creating any  missing parent directories if `create_dir` is set),
    /// and calling a function on the opened file.
    fn create_inner_file<T, F>(
        &self,
        path:       PathBuf,
        create_dir: bool,
        callback:   F,
    ) -> Result<T, Error>
    where
        F: FnOnce(&mut MemoryFileInner) -> Result<T, Error>,
    {
        if self.directories.borrow().contains(&path) {
            return Err(Error::IsADirectory(path));
        }

        if create_dir {
            self.inner_create_dir_all(path.clone())?;

        } else if let Some(parent) = path.parent() {
            self.assert_directory_exists(parent)?;

        } else {
            // The file is in root, its parent directory isn't missing.
        }

        let mut files = self.files.borrow_mut();
        let file = files
            .entry(path)
            .or_insert(MemoryFileInner::new());

        callback(file)
    }
}

impl ReadableFilesystem for MemoryFS {
    type ReadFile               = MemoryFile;
    type RandomAccessFile       = MemoryFile;
    type Error                  = Error;
    /// An owned `Vec` in order to avoid potential panics with internal `RefCell`s.
    type DirectoryChildren      = Vec<Result<PathBuf, Infallible>>;
    type DirectoryChildrenError = Infallible;
    type Lockfile               = Lockfile;
    type LockError              = LockError;

    fn open_sequential(&self, path: &Path) -> Result<Self::ReadFile, Self::Error> {
        let path = path.normalize();

        self.access_file_inner(path, |file| {
            Ok(MemoryFile::open(file))
        })
    }

    fn open_random_access(&self, path: &Path) -> Result<Self::RandomAccessFile, Self::Error> {
        let path = path.normalize();

        self.access_file_inner(path, |file| {
            Ok(MemoryFile::open(file))
        })
    }

    /// Infallibly check if a file or directory exists at the given path.
    fn exists(&self, path: &Path) -> Result<bool, Self::Error> {
        let path = &path.normalize();

        Ok(self.files.borrow().contains_key(path) || self.directories.borrow().contains(path))
    }

    /// Returns the direct children of the directory at the provided path.
    /// The returned paths are relative to the provided path.
    ///
    /// If an iterator is successfully returned, that iterator is infallible (aside from
    /// allocation errors).
    fn children(&self, path: &Path) -> Result<Self::DirectoryChildren, Self::Error> {
        let path = &path.normalize();

        self.assert_directory_exists(path)?;

        let children = self.files.borrow().keys()
            .chain(self.directories.borrow().iter())
            .filter_map(|entry_path| {
                entry_path
                    .strip_prefix(path)
                    .ok()
                    .map(|rel_path| Ok(rel_path.to_owned()))
            })
            .collect();

        Ok(children)
    }

    fn size_of(&self, path: &Path) -> Result<u64, Self::Error> {
        let path = path.normalize();

        self.access_file_inner(path, |file| {
            let len = file.len();
            #[expect(
                clippy::map_err_ignore,
                reason = "the only possible error is that `len` is greater than `u64::MAX`",
            )]
            u64::try_from(len).map_err(|_| Error::FileTooLong(len))
        })
    }

    fn open_and_lock(&self, path: &Path) -> Result<Self::Lockfile, Self::LockError> {
        let path = path.normalize();

        self.assert_file_exists(&path)?;
        self.locks.try_lock(path)
    }

    fn unlock_and_close(&self, lockfile: Self::Lockfile) -> Result<(), Self::LockError> {
        self.assert_file_exists(lockfile.inner_path())?;
        self.locks.unlock(lockfile)
    }
}

impl WritableFilesystem for MemoryFS {
    type WriteFile  = MemoryFile;
    type AppendFile = MemoryFile;

    fn open_writable(
        &self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::WriteFile, Self::Error> {
        let path = path.normalize();

        self.create_inner_file(path, create_dir, |file| {
            Ok(MemoryFile::open_and_truncate(file))
        })
    }

    fn open_appendable(
        &self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::AppendFile, Self::Error> {
        let path = path.normalize();

        self.create_inner_file(path, create_dir, |file| {
            Ok(MemoryFile::open_append(file))
        })
    }

    fn delete(&self, path: &Path) -> Result<(), Self::Error> {
        let path = &path.normalize();

        self.assert_file_exists(path)?;
        self.files.borrow_mut().remove(path);
        Ok(())
    }

    #[inline]
    fn create_dir(&self, path: &Path) -> Result<(), Self::Error> {
        let path = path.normalize();

        self.inner_create_dir(path)
    }

    #[inline]
    fn create_dir_all(&self, path: &Path) -> Result<(), Self::Error> {
        let path = path.normalize();

        self.inner_create_dir_all(path)
    }

    fn remove_dir(&self, path: &Path) -> Result<(), Self::Error> {
        let path = &path.normalize();

        self.assert_directory_exists(path)?;

        let has_children = self
            .files
            .borrow()
            .keys()
            .any(|filepath| filepath.starts_with(path));

        if has_children {
            Err(Error::NonemptyDirectory(path.to_owned()))
        } else {
            self.directories.borrow_mut().remove(path);
            Ok(())
        }
    }

    /// Rename a file or directory. Follows the Unix convention for errors.
    ///
    /// Returns an error if `old` does not exist. If `new` exists and `old` is a directory,
    /// then `new` must be an empty directory. If `new` exists and `old` is a file, then `new`
    /// must be a file. In either such case, the previous directory or file at `new`
    /// is silently deleted, overwritten by the directory or file that was at `old`.
    ///
    /// Moving a directory is ***expensive***, as it must move every entry within that directory
    /// (in a `HashMap`).
    fn rename(&self, old: &Path, new: &Path) -> Result<(), Self::Error> {
        let old = &old.normalize();
        let new = &new.normalize();

        let mut files       = self.files.borrow_mut();
        let mut directories = self.directories.borrow_mut();

        if files.contains_key(old) {
            if directories.contains(new) {
                Err(Error::DirectoryExists(new.to_owned()))
            } else {
                // Overwrite the file at `new` with the file at `old`
                #[expect(
                    clippy::unwrap_used,
                    reason = "this struct is single-threaded, and `files` contains the key `old`",
                )]
                let file = files.remove(old).unwrap();

                files.insert(new.to_owned(), file);
                Ok(())
            }

        } else if directories.contains(old) {
            if files.contains_key(new) {
                return Err(Error::FileExists(new.to_owned()));
            }

            let new_contains_a_file = files
                .keys()
                .any(|file_path| file_path.starts_with(new));
            if new_contains_a_file {
                return Err(Error::NonemptyDirectory(new.to_owned()));
            }

            let new_contains_a_dir = directories
                .iter()
                .any(|dir_path| {
                    dir_path.starts_with(new) && dir_path != new
                });
            if new_contains_a_dir {
                return Err(Error::NonemptyDirectory(new.to_owned()));
            }

            // We've confirmed that `new` either doesn't exist, or is an empty directory.

            // Overwrite any (empty) directory at `new` with the directory at `old`....
            // which requires moving a bunch of other entries.

            // Move files
            #[allow(
                clippy::needless_collect,
                reason = "false positive: the `collect` is needed to then mutate `files`",
            )]
            let files_to_move = files
                .extract_if(|file_path, _| file_path.starts_with(old))
                .collect::<Vec<_>>();

            let renamed_files = files_to_move
                .into_iter()
                .map(|(old_path, file)| {
                    #[expect(
                        clippy::unwrap_used,
                        reason = "These paths are filtered to start with the `old` prefix",
                    )]
                    let rel_path = old_path.strip_prefix(old).unwrap();

                    (new.join(rel_path), file)
                });

            files.extend(renamed_files);

            // Move directories - this includes all (recursive) children, as well as the
            // directory we're moving.
            #[allow(
                clippy::needless_collect,
                reason = "false positive: the `collect` is needed to then mutate `directories`",
            )]
            let dirs_to_move = directories
                .extract_if(|dir_path| dir_path.starts_with(old))
                .collect::<Vec<_>>();

            let renamed_dirs = dirs_to_move
                .into_iter()
                .map(|old_path| {
                    #[expect(
                        clippy::unwrap_used,
                        reason = "These paths are filtered to start with the `old` prefix",
                    )]
                    let rel_path = old_path.strip_prefix(old).unwrap();

                    new.join(rel_path)
                });

            directories.extend(renamed_dirs);

            Ok(())

        } else {
            Err(Error::NotFound(old.to_owned()))
        }
    }

    fn create_and_lock(
        &self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::Lockfile, Self::LockError> {
        let path = path.normalize();

        self.create_inner_file(path.clone(), create_dir, |_| Ok(()))?;
        self.locks.try_lock(path)
    }
}
