use cfg_if::cfg_if;


/// Implements one trait in `util_traits`,
/// and provides macros used to implement traits in `fs_traits`.
mod std_fs_core;
pub use self::std_fs_core::DirectoryChildren;

// Publicly export `StandardFS` and either `Lockfile` and `LockError` or `LockfileUnsupported`.
// Additionally, various traits are implemented for structs in `std`.
cfg_if! {
    if #[cfg(unix)] {
        /// Implements traits in `fs_traits`.
        mod std_fs_either;
        /// Implements traits in `util_traits`.
        mod std_fs_unix;

        pub use self::std_fs_either::{StandardFS, Lockfile, LockError};

    } else if #[cfg(windows)] {
        /// Implements traits in `fs_traits`.
        mod std_fs_either;
        /// Implements traits in `util_traits`.
        mod std_fs_windows;

        pub use self::std_fs_either::{StandardFS, Lockfile, LockError};

    } else {
        /// Implements traits in `fs_traits` and `util_traits`.
        mod std_fs_neither;

        pub use std_fs_neither::{StandardFS, LockfileUnsupported};
    }
}

