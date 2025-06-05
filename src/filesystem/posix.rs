use std::{fs::File, path::Path};

use super::{MaybeLockableFilesystem, RandomAccess, ReadableFileSystem, WriteableFilesystem};


#[derive(Debug)]
pub struct PosixFS {

}

impl ReadableFileSystem for PosixFS {
    type Error        = ();
    type ReadFile     = File;
    type RandomAccess = File;

    fn open_sequential(&self, path: &Path) -> Result<Self::ReadFile, Self::Error> {
        todo!()
    }

    fn open_random_access(&self, path: &Path) -> Result<Self::RandomAccess, Self::Error> {
        todo!()
    }

    fn exists(&self, path: &Path) -> Result<bool, Self::Error> {
        todo!()
    }

    fn children(&self, path: &Path) -> Result<Vec<std::path::PathBuf>, Self::Error> {
        todo!()
    }

    fn size_of(&self, path: &Path) -> Result<usize, Self::Error> {
        todo!()
    }
}

impl WriteableFilesystem for PosixFS {
    type WriteFile  = File;
    type AppendFile = File;

    fn open_writable(
        &self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::WriteFile, Self::Error> {
        todo!()
    }

    fn open_appendable(
        &self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::AppendFile, Self::Error> {
        todo!()
    }

    fn delete(&self, path: &Path) -> Result<(), Self::Error> {
        todo!()
    }

    fn mkdir(&self, path: &Path) -> Result<(), Self::Error> {
        todo!()
    }

    fn rmdir(&self, path: &Path) -> Result<(), Self::Error> {
        todo!()
    }

    fn rename(&self, old: &Path, new: &Path) -> Result<(), Self::Error> {
        todo!()
    }
}

impl MaybeLockableFilesystem for PosixFS {
    type LockError = ();
    type FileLock  = ();

    fn lock(&self, path: &Path) -> Result<Self::FileLock, Self::LockError> {
        todo!()
    }

    fn unlock(&self, lock: Self::FileLock) -> Result<(), Self::LockError> {
        todo!()
    }
}

impl RandomAccess for File {
    type Error = ();

    fn read_at(&self, off: usize, dst: &mut [u8]) -> Result<usize, Self::Error> {
        todo!()
    }
}
