mod dyn_impls;
mod file_logger;
mod threadsafe_file_logger;
mod stderr_logger;
mod log_crate_logger;

mod log_file_ctor;


pub use log::{Level, LevelFilter};

pub use self::{
    file_logger::{FileLogger, FileLoggerCtor},
    log_crate_logger::{LogCrateLogger, LogCrateLoggerCtor},
    log_file_ctor::{LogFileConstructionError, LogFileConstructor},
    stderr_logger::{StderrLogger, StderrLoggerCtor},
    threadsafe_file_logger::{ThreadsafeFileLogger, ThreadsafeFileLoggerCtor},
};


use std::fmt::Debug;

use crate::filesystem::FileSystem;


pub trait LoggerConstructor<FS: FileSystem>: Debug {
    type Logger: Logger;
    type Error:  Debug;

    fn construct(
        self,
        level_filter: LevelFilter,
        logfile_ctor: LogFileConstructor<FS>,
    ) -> Result<Self::Logger, Self::Error>;
}

/// Logs human-readable messages about what the database has done.
///
/// Note that this has nothing to do with the write-ahead logs used by LevelDB; these messages
/// are purely for diagnostic or debugging purposes, not for database-critical purposes.
///
/// Writes to a provided `Logger` are not wrapped in, for instance, a `BufWriter`.
/// Any necessary buffering (such as if logs are written to a file) needs to be performed
/// within an `Logger`.
///
/// See the [`log`](https://docs.rs/log/latest/log/#usage) crate for information.
///
/// The behavior of the original LevelDB implementation is to log messages to the `LOG` file,
/// and move the previous `LOG` file to `LOG.old`.
pub trait Logger: Debug {
    fn log(&self, level: Level, msg: &str);
}
