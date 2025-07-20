// TODO: is SyncRandomAccess needed? Should I provide ThreadsafeStandardFS
// (and likewise for others)? and should probably rename it to ThreadsafeRandomAccess.

// Main filesystem traits
mod fs_traits;
// Traits relied on by filesystem traits
mod util_traits;
// Two error types, and implementations for error traits.
mod error;


// ================================
//  Filesystem implementations
// ================================

mod dyn_filesystems;

#[cfg(feature = "std-fs")]
pub mod std_fs;
// pub mod threadsafe_std_fs; - probably not worth it if only SyncRandomAccess is added,
// since that only even matters for non-unix/windows targets.

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
            RandomAccess, SyncRandomAccess, WritableFile,
        },
    };
}

pub use self::error::{MutexPoisoned, Never};

#[cfg(feature = "std-fs")]
pub use self::std_fs::StandardFS;

// pub use self::memory_fs::

// #[cfg(feature = "zip")]
// pub use self::zip_readonly_fs::

// We don't directly need the `time` crate, but `zip` uses it; and in order to work properly on
// web, time needs to have its `wasm-bindgen` feature enabled.
#[cfg(feature = "zip-time-js")]
use time as _;
