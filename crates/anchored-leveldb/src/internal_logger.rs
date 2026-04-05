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

    // TODO: logger interface
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
