use std::{error::Error, marker::PhantomData, path::PathBuf};
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use anchored_vfs::traits::WritableFilesystem;


#[derive(Debug)]
pub struct LogFileConstructor<FS: WritableFilesystem> {
    _future_proofing: PhantomData<fn() -> FS>,
}

impl<FS: WritableFilesystem> LogFileConstructor<FS> {
    pub fn make_log_file(self) -> Result<
        (PathBuf, FS::AppendFile),
        LogFileConstructionError<FS::Error>,
    > {
        todo!()
    }
}


#[derive(Debug, Clone)]
pub struct LogFileConstructionError<FSError>(pub FSError);

impl<FSError: Display> Display for LogFileConstructionError<FSError> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
         write!(f, "FileSystem error while constructing log file: {}", self.0)
    }
}

impl<FSError: Debug + Display> Error for LogFileConstructionError<FSError> {}
