// TODO: put this in its own crate, it'll probably get big enough to warrant it.
// TODO: is SyncRandomAccess needed? Should I provide ThreadsafeStandardFS
// (and likewise for others)? and should probably rename it to ThreadsafeRandomAccess.

// Main filesystem traits
mod fs_traits;
// Traits relied on by filesystem traits
mod util_traits;


// Implementations

#[cfg(feature = "std-fs")]
pub mod std_fs;
// pub mod threadsafe_std_fs; - probably not worth it if only SyncRandomAccess is added,
// since that only even matters for non-unix/windows targets.

pub mod memory_fs;
pub mod threadsafe_memory_fs;

pub mod zip_readonly_fs;


/// Module containing all the traits defined in this to-be-crate. May be used as a prelude.
pub mod traits {
    pub use crate::{
        fs_traits::{DebugReadableFS, DebugWritableFS, ReadableFilesystem, WritableFilesystem},
        util_traits::{FSError, FSLockError, RandomAccess, SyncRandomAccess, WritableFile},
    };
}

#[cfg(feature = "std-fs")]
pub use self::std_fs::StandardFS;

pub use self::memory_fs::MemoryFS;
pub use self::threadsafe_memory_fs::ThreadsafeMemoryFS;


// We don't directly need the `time` crate, but `zip` uses it; and in order to work properly on
// web, time needs to have its `wasm-bindgen` feature enabled.
#[cfg(feature = "zip-time-js")]
use time as _;
