use std::fmt::{Debug, Formatter, Result as FmtResult};

use tracing::level_filters::LevelFilter;

use crate::pub_traits::logger::Logger;


pub(crate) struct InternalLogger<File> {
    log_file:        Option<File>,
    log_file_filter: LevelFilter,
    custom_logger:   Box<dyn Logger + Send + Sync>,
    logger_filter:   LevelFilter,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File> InternalLogger<File> {
    #[inline]
    #[must_use]
    pub fn new(
        log_file:        Option<File>,
        log_file_filter: LevelFilter,
        custom_logger:   Box<dyn Logger + Send + Sync>,
        logger_filter:   LevelFilter,
    ) -> Self {
        Self { log_file, log_file_filter, custom_logger, logger_filter }
    }

    // TODO: logger interface.
    // Something like this:
    // pub fn log_event<F: FnOnce() -> String>(&mut self, level: LogLevel, message: F) {
    //     let log_to_file = level <= self.file_filter;
    //     let something_enabled = log_to_file || match level {
    //         LogLevel::ERROR => tracing::event_enabled!(LogLevel::ERROR),
    //         LogLevel::WARN  => tracing::event_enabled!(LogLevel::WARN),
    //         LogLevel::INFO  => tracing::event_enabled!(LogLevel::INFO),
    //         LogLevel::DEBUG => tracing::event_enabled!(LogLevel::DEBUG),
    //         LogLevel::TRACE => tracing::event_enabled!(LogLevel::TRACE),
    //     };

    //     if something_enabled {
    //         let message = message();

    //         match level {
    //             LogLevel::ERROR => tracing::event!(LogLevel::ERROR, message = message),
    //             LogLevel::WARN  => tracing::event!(LogLevel::WARN,  message = message),
    //             LogLevel::INFO  => tracing::event!(LogLevel::INFO,  message = message),
    //             LogLevel::DEBUG => tracing::event!(LogLevel::DEBUG, message = message),
    //             LogLevel::TRACE => tracing::event!(LogLevel::TRACE, message = message),
    //         }

    //         if log_to_file {
    //             let Some(info_log_file) = self.file.as_mut() else { return; };

    //             // If writing the message fails, don't bother to flush
    //             // let err = info_log_file
    //             //     .write_all(message.as_bytes()).err()
    //             //     .or_else(|| info_log_file.flush().err());

    //             // if let Some(err) = err {
    //             //     tracing::event!(
    //             //         LogLevel::DEBUG,
    //             //         "InfoLogger could not write to `LOG` file: {err}",
    //             //     );
    //             // }
    //         }
    //     }
    // }
}

impl<File> Debug for InternalLogger<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let log_file = if self.log_file.is_some() {
            "Some(<File>)"
        } else {
            "None"
        };

        f.debug_struct("InternalLogger")
            .field("log_file",        &log_file)
            .field("log_file_filter", &self.log_file_filter)
            .field("custom_logger",   &"Box<dyn Logger>")
            .field("logger_filter",   &self.logger_filter)
            .finish()
    }
}
