use tracing::{Level, level_filters::LevelFilter};

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
#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
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

pub(crate) struct InfoLogger<File: WritableFile> {
    file:   File,
    filter: LevelFilter,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File: WritableFile> InfoLogger<File> {
    #[inline]
    #[must_use]
    pub fn new(file: File, filter: InfoLogLevelFilter) -> Self {
        Self {
            file,
            filter: LevelFilter::from(filter),
        }
    }

    pub fn log<F: FnOnce() -> String>(&mut self, log_level: Level, message: F) {
        if log_level <= LevelFilter::INFO || log_level <= self.filter {
            let message = message();
            // If writing the message fails, don't bother to flush
            let err = self.file
                .write_all(message.as_bytes()).err()
                .or_else(|| self.file.flush().err());

            if let Some(err) = err {
                tracing::event!(Level::DEBUG, "InfoLogger could not write to `LOG` file: {err}");
            }
        }
    }
}
