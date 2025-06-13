mod fs;
mod error;
mod file;
mod lockfile;
// #[cfg(feature = "zip")]
// mod zip_conversion;


pub use self::{
    error::Error,
    file::MemoryFile,
    fs::MemoryFS,
};
pub use self::lockfile::{LockError, Lockfile};
// #[cfg(feature = "zip")]
// pub use self::zip_conversion::{MemToZipError, ZipToMemError};
