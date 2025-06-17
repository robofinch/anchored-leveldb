pub mod aliases;

mod fs;

mod error;
mod file;
mod file_inner;
mod iter;
mod lockfile;
mod path;

#[cfg(feature = "zip")]
mod zip_conversion;


pub use self::{
    error::Error,
    file::MemoryFileWithInner,
    file_inner::MemoryFileInner,
    fs::MemoryFSWithInner,
    iter::IntoDirectoryIter,
};
pub use self::{
    aliases::{ThreadLocalMemoryFS, ThreadsafeMemoryFS},
    lockfile::{LockError, Lockfile},
    path::{NormalizedPath, NormalizedPathBuf},
};
// #[cfg(feature = "zip")]
// pub use self::zip_conversion::{MemToZipError, ZipToMemError};
