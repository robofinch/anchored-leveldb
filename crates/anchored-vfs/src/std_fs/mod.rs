mod std_fs_utils;
mod std_fs_struct;

// Publicly export `StandardFS` and either `Lockfile` and `LockError` or `LockfileUnsupported`.
// Additionally, various traits are implemented for structs in `std`.
cfg_if::cfg_if! {
    if #[cfg(unix)] {
        mod std_fs_unix;

        use self::std_fs_unix as std_fs_sys;

    } else if #[cfg(windows)] {
        mod std_fs_windows;

        use self::std_fs_windows as std_fs_sys;
    } else {
        // Currently, only unix and windows are supported. This entire module is inside
        // `#[cfg(any(unix, windows))]`.
    }
}


pub use self::std_fs_struct::StandardFS;
pub use self::std_fs_utils::{IntoChildFileIter, LockError, Lockfile};
