use std::path::PathBuf;
use std::{
    fs::{File, ReadDir},
    io::{BufWriter, Error as IoError, Result as IoResult},
};

use crate::util_traits::{IntoDirectoryIterator, WritableFile};


// ================
//  Impls for std
// ================

impl WritableFile for BufWriter<File> {
    #[inline]
    fn sync_data(&mut self) -> IoResult<()> {
        self.get_ref().sync_data()
    }
}

// ================
//  Structs
// ================

#[derive(Debug)]
pub struct IntoDirectoryIter {
    readdir_iter: ReadDir,
}

impl IntoDirectoryIter {
    #[expect(
        clippy::missing_const_for_fn,
        reason = "`MemoryFS` cannot be constructed in const contexts",
    )]
    #[inline]
    pub(super) fn new(readdir_iter: ReadDir) -> Self {
        Self {
            readdir_iter,
        }
    }
}

impl IntoDirectoryIterator for IntoDirectoryIter {
    type DirIterError = IoError;

    #[inline]
    fn dir_iter(self) -> impl Iterator<Item = Result<PathBuf, Self::DirIterError>> {
        self.readdir_iter
            .map(|dir_entry| {
                Ok(dir_entry?.file_name().into())
            })
    }
}


// ================
//  Macros
// ================

macro_rules! readable_core {
    () => {
        type ReadFile              = File;
        type RandomAccessFile      = File;
        type Error                 = IoError;
        type IntoDirectoryIter<'a> = IntoDirectoryIter;

        #[inline]
        fn open_sequential(&self, path: &Path) -> Result<Self::ReadFile, Self::Error> {
            File::open(path)
        }

        #[inline]
        fn open_random_access(&self, path: &Path) -> Result<Self::RandomAccessFile, Self::Error> {
            File::open(path)
        }

        #[inline]
        fn exists(&self, path: &Path) -> Result<bool, Self::Error> {
            path.try_exists()
        }

        #[inline]
        fn children(&self, path: &Path) -> Result<
            Self::IntoDirectoryIter<'_>,
            Self::Error,
        > {
            path.read_dir().map(IntoDirectoryIter::new)
        }

        #[inline]
        fn size_of(&self, path: &Path) -> Result<u64, Self::Error> {
            path.metadata().map(|metadata| metadata.len())
        }
    };
}

macro_rules! writable_core {
    () => {
        type WriteFile  = BufWriter<File>;
        type AppendFile = BufWriter<File>;

        fn open_writable(
            &mut self,
            path:       &Path,
            create_dir: bool,
        ) -> Result<Self::WriteFile, Self::Error> {
            if create_dir {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
            }

            File::create(path).map(BufWriter::new)
        }

        fn open_appendable(
            &mut self,
            path:       &Path,
            create_dir: bool,
        ) -> Result<Self::AppendFile, Self::Error> {
            if create_dir {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
            }

            File::options()
                .append(true)
                .create(true)
                .open(path)
                .map(BufWriter::new)
        }

        #[inline]
        fn delete(&mut self, path: &Path) -> Result<(), Self::Error> {
            fs::remove_file(path)
        }

        #[inline]
        fn create_dir(&mut self, path: &Path) -> Result<(), Self::Error> {
            #[expect(
                clippy::create_dir,
                reason = "yes, we really don't want `fs::create_dir_all` to implement `create_dir`",
            )]
            fs::create_dir(path)
        }

        #[inline]
        fn create_dir_all(&mut self, path: &Path) -> Result<(), Self::Error> {
            fs::create_dir_all(path)
        }

        #[inline]
        fn remove_dir(&mut self, path: &Path) -> Result<(), Self::Error> {
            fs::remove_dir(path)
        }

        #[inline]
        fn rename(&mut self, old: &Path, new: &Path) -> Result<(), Self::Error> {
            fs::rename(old, new)
        }
    };
}

pub(crate) use readable_core;
pub(crate) use writable_core;
