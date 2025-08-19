use std::{cell::RefCell, path::Path, rc::Rc};
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    io::{BufWriter, Write},
};

use log::{Level, LevelFilter};

use anchored_vfs::traits::WritableFilesystem;

use super::log_file_ctor::LogFileConstructionError;
use super::{LogFileConstructor, Logger, LoggerConstructor};


#[derive(Debug, Clone, Copy)]
pub struct FileLoggerCtor;

impl<FS: WritableFilesystem> LoggerConstructor<FS> for FileLoggerCtor {
    type Logger = FileLogger<FS>;
    type Error  = LogFileConstructionError<FS::Error>;

    fn construct(
        self,
        level_filter: LevelFilter,
        logfile_ctor: LogFileConstructor<FS>,
    ) -> Result<Self::Logger, Self::Error> {
        let (logfile_path, logfile) = logfile_ctor.make_log_file()?;

        Ok(FileLogger {
            level_filter,
            logfile_path: logfile_path.into_boxed_path(),
            logfile:      Rc::new(RefCell::new(BufWriter::new(logfile))),
        })
    }
}

#[derive(Clone)]
pub struct FileLogger<FS: WritableFilesystem> {
    level_filter: LevelFilter,
    logfile_path: Box<Path>,
    logfile:      Rc<RefCell<BufWriter<FS::AppendFile>>>,
}

impl<FS: WritableFilesystem> Debug for FileLogger<FS> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "FileLogger (log filter level {}) writing to path {:?}",
            self.level_filter.as_str(),
            self.logfile_path,
        )
    }
}

impl<FS: WritableFilesystem> Logger for FileLogger<FS> {
    fn log(&self, level: Level, msg: &str) {
        if level > self.level_filter {
            // Filter out that level.
            // Note that "greater" == "more verbose" == "less important"
            // for the log crate's levels.
            return;
        }


        let Ok(mut logfile) = self.logfile.try_borrow_mut() else {
            log::warn!("FileLogger's log file was already borrowed, and could not be written to");
            return;
        };

        // Morally:
        // let log_msg = format!("{}: {}", level.as_str(), msg);

        let level_str = level.as_str();

        let mut log_msg = String::with_capacity(level_str.len() + 2 + msg.len());
        log_msg.push_str(level_str);
        log_msg.push_str(": ");
        log_msg.push_str(msg);

        if let Err(err) = logfile.write_all(log_msg.as_bytes()) {
            log::warn!("FileLogger encountered an IO error while writing to its log file: {err}");
            log::log!(level, "{msg}");
        }
    }
}
