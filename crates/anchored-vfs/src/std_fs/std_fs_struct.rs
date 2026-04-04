use std::fs;
use std::{fs::File, path::Path};
use std::io::{BufWriter, Error as IoError, ErrorKind};

use crate::fs_traits::{CreateParentDir, LevelDBFilesystem, SyncParentDir};
use super::std_fs_sys;
use super::std_fs_utils::{IntoChildFileIter, LockError, Lockfile};


// TODO: improve errors

/// The standard library's file system.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandardFS;

impl LevelDBFilesystem for StandardFS {
    type ReadFile         = File;
    type RandomAccessFile = File;
    type WriteFile        = BufWriter<File>;
    type ChildFiles<'a>   = IntoChildFileIter;
    type Lockfile         = Lockfile;
    type LockError        = LockError;
    type Error            = IoError;

    #[inline]
    fn open_sequential(&self, path: &Path) -> Result<Self::ReadFile, Self::Error> {
        File::open(path)
    }

    #[inline]
    fn open_random_access(&self, path: &Path) -> Result<Self::RandomAccessFile, Self::Error> {
        File::open(path)
    }

    fn open_writable(
        &self,
        path:       &Path,
        create_dir: CreateParentDir,
        sync_dir:   SyncParentDir,
    ) -> Result<Self::WriteFile, Self::Error> {
        if create_dir.into() {
            if let Some(parent_path) = path.parent() {
                std_fs_sys::create_dir_all(parent_path, sync_dir.into())?;
            }
        }

        File::create(path).map(BufWriter::new)
    }

    fn supports_efficient_appendable(&self) -> bool {
        // Assume that operating systems efficiently support this
        true
    }

    fn open_appendable(
        &self,
        path:       &Path,
        create_dir: CreateParentDir,
        sync_dir:   SyncParentDir,
    ) -> Result<Self::WriteFile, Self::Error> {
        if create_dir.into() {
            if let Some(parent) = path.parent() {
                std_fs_sys::create_dir_all(parent, sync_dir.into())?;
            }
        }

        File::options()
            .append(true)
            .create(true)
            .open(path)
            .map(BufWriter::new)
    }

    #[inline]
    fn rename(&self, from: &Path, to: &Path, sync_dir: SyncParentDir) -> Result<(), Self::Error> {
        fs::rename(from, to)?;

        if sync_dir.into() {
            if let Some(parent_path) = to.parent() {
                std_fs_sys::sync_dir_after_rename(parent_path)?;
            }
        }

        Ok(())
    }

    #[inline]
    fn remove_file(&self, path: &Path) -> Result<(), Self::Error> {
        fs::remove_file(path)
    }

    #[inline]
    fn remove_dir(&self, path: &Path) -> Result<(), Self::Error> {
        fs::remove_dir(path)
    }

    #[inline]
    fn size_of_file(&self, path: &Path) -> Result<u64, Self::Error> {
        path.metadata().map(|metadata| metadata.len())
    }

    #[inline]
    fn file_exists(&self, path: &Path) -> Result<bool, Self::Error> {
        match path.metadata() {
            Ok(meta) => Ok(meta.is_file()),
            Err(err) => if err.kind() == ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(err)
            },
        }
    }

    #[inline]
    fn child_files(&mut self, path: &Path) -> Result<
        Self::ChildFiles<'_>,
        Self::Error,
    > {
        path.read_dir().map(IntoChildFileIter::new)
    }

    fn open_and_lock(&mut self, path: &Path) -> Result<Self::Lockfile, Self::LockError> {
        // Open the lockfile with read-only access.
        let lockfile = File::open(path)?;
        Lockfile::new(lockfile)
    }

    fn create_and_lock(
        &mut self,
        path:       &Path,
        create_dir: CreateParentDir,
        sync_dir:   SyncParentDir,
    ) -> Result<Self::Lockfile, Self::LockError> {
        if create_dir.into() {
            if let Some(parent) = path.parent() {
                std_fs_sys::create_dir_all(parent, sync_dir.into())?;
            }
        }

        // Open with `create` in order to create the lock if it doesn't exist,
        // and open with `append` in order to avoid overwriting any previous contents of the file.
        let lockfile = File::options()
            .append(true)
            .create(true)
            .open(path)?;

        Lockfile::new(lockfile)
    }
}
