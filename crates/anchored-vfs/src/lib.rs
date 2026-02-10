// Main filesystem traits
mod fs_traits;
// Traits relied on by filesystem traits
mod util_traits;
// Two error types, and implementations for error traits.
mod error;


// TODO: use `clone_behavior` crate.


// ================================
//  Filesystem implementations
// ================================

// Currently, only unix and windows are supported.
// TODO: support WASI
#[cfg(any(unix, windows))]
pub mod std_fs;

// pub mod memory_fs;

// #[cfg(feature = "zip")]
// pub mod zip_readonly_fs;

// TODO: js_fs, or something like that: a filesystem primarily controlled by the JavaScript side,
// with an interface exposed to the WASM side.


// ================================
//  Re-exports
// ================================

pub use crate::{
    error::{MutexPoisoned, Never},
    fs_traits::{CreateParentDir, LevelDBFilesystem, SyncParentDir},
    // memory_fs::{ThreadLocalMemoryFS, ThreadsafeMemoryFS},
    util_traits::{FSError, FSLockError, IntoChildFileIterator, RandomAccess, WritableFile},
};

// Currently, only unix and windows are supported.
#[cfg(any(unix, windows))]
pub use self::std_fs::StandardFS;

// #[cfg(feature = "zip")]
// pub use self::zip_readonly_fs::
