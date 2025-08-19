use std::convert::Infallible;

use log::{Level, LevelFilter};

use anchored_vfs::traits::WritableFilesystem;

use super::{LogFileConstructor, Logger, LoggerConstructor};


#[derive(Debug, Clone, Copy)]
pub struct StderrLoggerCtor;

impl<FS: WritableFilesystem> LoggerConstructor<FS> for StderrLoggerCtor {
    type Logger = StderrLogger;
    type Error  = Infallible;

    #[inline]
    fn construct(
        self,
        level_filter: LevelFilter,
        _: LogFileConstructor<FS>,
    ) -> Result<Self::Logger, Self::Error> {
        Ok(StderrLogger(level_filter))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StderrLogger(pub LevelFilter);

impl Logger for StderrLogger {
    fn log(&self, level: Level, msg: &str) {
        eprintln!("{}: {}", level.as_str(), msg)
    }
}
