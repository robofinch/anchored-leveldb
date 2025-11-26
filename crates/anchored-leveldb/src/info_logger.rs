use std::fmt::{Debug, Formatter, Result as FmtResult};

use tracing::{Level as LogLevel, level_filters::LevelFilter};

use anchored_vfs::traits::WritableFile;


/// A level of verbosity for logging a message to a `LOG` info log file.
///
/// This crate logs messages using [`tracing`]. Additionally, to match the behavior of the original
/// LevelDB implementation, an option is available to log messages to a `LOG` file,
/// and move the previous `LOG` file (if one exists) to `LOG.old`.
///
/// When a database is opened, the previous `LOG` is untouched if [`InfoLogLevelFilter::Off`] is
/// used; otherwise, a new `LOG` file is opened (and the previous `LOG` is moved to `LOG.old`),
/// and this filter is used to decide which messages to persist to the file and which to log
/// with [`tracing`] alone.
///
/// A greater `InfoLogLevelFilter`, with respect to [`Ord`], indicates a greater verbosity level.
#[repr(u8)]
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InfoLogLevelFilter {
    /// Do not write to a `LOG` file.
    Off   = 0,
    /// Log only messages which describe very serious errors.
    Error = 1,
    /// Log messages which describe hazardous situations or very serious errors.
    Warn  = 2,
    /// Log messages which describe useful information, hazardous situations,
    /// or very serious errors.
    #[default]
    Info  = 3,
}

impl From<InfoLogLevelFilter> for LevelFilter {
    fn from(level_filter: InfoLogLevelFilter) -> Self {
        match level_filter {
            InfoLogLevelFilter::Off   => Self::OFF,
            InfoLogLevelFilter::Error => Self::ERROR,
            InfoLogLevelFilter::Warn  => Self::WARN,
            InfoLogLevelFilter::Info  => Self::INFO,
        }
    }
}

pub(crate) struct InfoLogger<File> {
    file:        Option<File>,
    file_filter: LevelFilter,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File: WritableFile> InfoLogger<File> {
    #[inline]
    #[must_use]
    pub fn new(info_log_file: File, filter: InfoLogLevelFilter) -> Self {
        Self {
            file:        Some(info_log_file),
            file_filter: LevelFilter::from(filter),
        }
    }

    #[inline]
    #[must_use]
    pub const fn new_without_log_file() -> Self {
        Self {
            file:        None,
            file_filter: LevelFilter::OFF,
        }
    }

    pub fn log_event<F: FnOnce() -> String>(&mut self, level: LogLevel, message: F) {
        let log_to_file = level <= self.file_filter;
        let something_enabled = log_to_file || match level {
            LogLevel::ERROR => tracing::event_enabled!(LogLevel::ERROR),
            LogLevel::WARN  => tracing::event_enabled!(LogLevel::WARN),
            LogLevel::INFO  => tracing::event_enabled!(LogLevel::INFO),
            LogLevel::DEBUG => tracing::event_enabled!(LogLevel::DEBUG),
            LogLevel::TRACE => tracing::event_enabled!(LogLevel::TRACE),
        };

        if something_enabled {
            let message = message();

            match level {
                LogLevel::ERROR => tracing::event!(LogLevel::ERROR, message = message),
                LogLevel::WARN  => tracing::event!(LogLevel::WARN,  message = message),
                LogLevel::INFO  => tracing::event!(LogLevel::INFO,  message = message),
                LogLevel::DEBUG => tracing::event!(LogLevel::DEBUG, message = message),
                LogLevel::TRACE => tracing::event!(LogLevel::TRACE, message = message),
            }

            if log_to_file {
                let Some(info_log_file) = self.file.as_mut() else { return; };

                // If writing the message fails, don't bother to flush
                let err = info_log_file
                    .write_all(message.as_bytes()).err()
                    .or_else(|| info_log_file.flush().err());

                if let Some(err) = err {
                    tracing::event!(
                        LogLevel::DEBUG,
                        "InfoLogger could not write to `LOG` file: {err}",
                    );
                }
            }
        }
    }
}

impl<File> Debug for InfoLogger<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InfoLogger")
            .field("file",        &self.file.as_ref().map(|_file| "<LOG file>"))
            .field("file_filter", &self.file_filter)
            .finish()
    }
}
