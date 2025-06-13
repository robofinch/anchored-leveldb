use std::{error::Error as StdError, path::PathBuf, sync::PoisonError};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    fs::{File, ReadDir},
    io::{BufWriter, Error as IoError, ErrorKind, Result as IoResult},
};

use crate::util_traits::{FSError, WritableFile};


// ================
//  Impls for std
// ================

impl WritableFile for BufWriter<File> {
    #[inline]
    fn sync_data(&mut self) -> IoResult<()> {
        self.get_ref().sync_data()
    }
}

impl FSError for IoError {
    #[inline]
    fn is_not_found(&self) -> bool {
        self.kind() == ErrorKind::NotFound
    }

    #[inline]
    fn is_interrupted(&self) -> bool {
        self.kind() == ErrorKind::Interrupted
    }

    #[inline]
    fn is_poison_error(&self) -> bool {
        false
    }
}

// ================
//  Structs
// ================

#[derive(Debug)]
pub struct DirectoryChildren {
    readdir_iter: ReadDir,
}

impl DirectoryChildren {
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

impl Iterator for DirectoryChildren {
    type Item = IoResult<PathBuf>;

    fn next(&mut self) -> Option<Self::Item> {
        let dir_entry = self.readdir_iter.next()?;

        Some(dir_entry.map(|dir_entry| dir_entry.file_name().into()))
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct MutexPoisoned;

impl<T> From<PoisonError<T>> for MutexPoisoned {
    #[inline]
    fn from(_err: PoisonError<T>) -> Self {
        Self
    }
}

impl From<MutexPoisoned> for IoError {
    fn from(err: MutexPoisoned) -> Self {
        Self::other(err)
    }
}

impl Display for MutexPoisoned {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "a mutex was poisoned")
    }
}

impl StdError for MutexPoisoned {}

// ================
//  Macros
// ================

macro_rules! readable_core {
    () => {
        type ReadFile               = File;
        type RandomAccessFile       = File;
        type Error                  = IoError;

        type DirectoryChildren      = DirectoryChildren;
        type DirectoryChildrenError = IoError;

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
        fn children(&self, path: &Path) -> Result<Self::DirectoryChildren, Self::Error> {
            path.read_dir().map(DirectoryChildren::new)
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
            &self,
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
            &self,
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
        fn delete(&self, path: &Path) -> Result<(), Self::Error> {
            fs::remove_file(path)
        }

        #[inline]
        fn create_dir(&self, path: &Path) -> Result<(), Self::Error> {
            #[expect(
                clippy::create_dir,
                reason = "yes, we really don't want `fs::create_dir_all` to implement `create_dir`",
            )]
            fs::create_dir(path)
        }

        #[inline]
        fn create_dir_all(&self, path: &Path) -> Result<(), Self::Error> {
            fs::create_dir_all(path)
        }

        #[inline]
        fn remove_dir(&self, path: &Path) -> Result<(), Self::Error> {
            fs::remove_dir(path)
        }

        #[inline]
        fn rename(&self, old: &Path, new: &Path) -> Result<(), Self::Error> {
            fs::rename(old, new)
        }
    };
}

pub(crate) use readable_core;
pub(crate) use writable_core;
