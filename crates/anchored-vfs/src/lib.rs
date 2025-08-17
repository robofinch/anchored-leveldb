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

mod dyn_filesystems;

// Currently, only unix and windows are supported.
#[cfg(feature = "std-fs")]
#[cfg(any(unix, windows))]
pub mod std_fs;

pub mod memory_fs;

#[cfg(feature = "zip")]
pub mod zip_readonly_fs;

// TODO: js_fs, or something like that: a filesystem primarily controlled by the JavaScript side,
// with an interface exposed to the WASM side.


// ================================
//  Re-exports
// ================================

/// Module containing all the traits defined in this to-be-crate. May be used as a prelude.
pub mod traits {
    pub use crate::{
        fs_traits::{ReadableFilesystem, WritableFilesystem},
        util_traits::{
            IntoDirectoryIterator, FSError, FSLockError,
            RandomAccess, WritableFile,
        },
    };
}

pub use self::error::{MutexPoisoned, Never};

// Currently, only unix and windows are supported.
#[cfg(feature = "std-fs")]
#[cfg(any(unix, windows))]
pub use self::std_fs::StandardFS;

pub use self::memory_fs::{ThreadLocalMemoryFS, ThreadsafeMemoryFS};

// #[cfg(feature = "zip")]
// pub use self::zip_readonly_fs::

// We don't directly need the `time` crate, but `zip` uses it; and in order to work properly on
// web, time needs to have its `wasm-bindgen` feature enabled.
#[cfg(feature = "zip-time-js")]
use time as _;
