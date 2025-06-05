use std::convert::Infallible;

use log::{Level, LevelFilter};

use crate::filesystem::FileSystem;

use super::{LogFileConstructor, Logger, LoggerConstructor};


#[derive(Debug, Clone, Copy)]
pub struct LogCrateLoggerCtor;

impl<FS: FileSystem> LoggerConstructor<FS> for LogCrateLoggerCtor {
    type Logger = LogCrateLogger;
    type Error  = Infallible;

    #[inline]
    fn construct(
        self,
        level_filter: LevelFilter,
        _: LogFileConstructor<FS>,
    ) -> Result<Self::Logger, Self::Error> {
        Ok(LogCrateLogger(level_filter))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LogCrateLogger(pub LevelFilter);

impl Logger for LogCrateLogger {
    fn log(&self, level: Level, msg: &str) {
        log::log!(level, "{msg}");
    }
}
