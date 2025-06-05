// TODO: provide some impls for dyn objects and containers holding filesystems
pub mod posix;
pub mod memory;
pub mod zip;
pub mod zip_readonly;


use std::fmt::Debug;
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
};


pub trait RandomAccess {
    type Error;

    fn read_at(&self, off: usize, dst: &mut [u8]) -> Result<usize, Self::Error>;
}

pub trait ReadableFileSystem: Debug {
    type Error:        Debug;
    type ReadFile:     Read;
    type RandomAccess: RandomAccess;

    fn open_sequential(&self, path: &Path) -> Result<Self::ReadFile, Self::Error>;
    fn open_random_access(&self, path: &Path) -> Result<Self::RandomAccess, Self::Error>;

    fn exists(&self, path: &Path) -> Result<bool, Self::Error>;
    fn children(&self, path: &Path) -> Result<Vec<PathBuf>, Self::Error>;
    fn size_of(&self, path: &Path) -> Result<usize, Self::Error>;
}

pub trait WriteableFilesystem: ReadableFileSystem {
    type WriteFile:  Write;
    type AppendFile: Write;

    fn open_writable(
        &self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::WriteFile, Self::Error>;

    fn open_appendable(
        &self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::AppendFile, Self::Error>;

    // fn sync_data

    fn delete(&self, path: &Path) -> Result<(), Self::Error>;
    fn mkdir(&self, path: &Path) -> Result<(), Self::Error>;
    fn rmdir(&self, path: &Path) -> Result<(), Self::Error>;

    fn rename(&self, old: &Path, new: &Path) -> Result<(), Self::Error>;
}

pub trait MaybeLockableFilesystem: Debug {
    type LockError: Debug;
    type FileLock:  Debug;

    fn lock(&self, path: &Path) -> Result<Self::FileLock, Self::LockError>;
    fn unlock(&self, lock: Self::FileLock) -> Result<(), Self::LockError>;
}

pub trait LockableFilesystem: MaybeLockableFilesystem {}

pub trait ReadOnlyFileSystem: ReadableFileSystem + MaybeLockableFilesystem {}
impl<T: ReadableFileSystem + MaybeLockableFilesystem> ReadOnlyFileSystem for T {}

pub trait FileSystem: WriteableFilesystem + MaybeLockableFilesystem {}
impl<T: WriteableFilesystem + MaybeLockableFilesystem> FileSystem for T {}
