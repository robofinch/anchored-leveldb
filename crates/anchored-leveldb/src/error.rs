use std::{error::Error as StdError, result::Result as StdResult, sync::PoisonError};
use std::fmt::{Display, Formatter, Result as FmtResult};


pub type Result<T> = StdResult<T, Error>;


#[derive(Debug, Clone)]
pub struct Error {
    pub code: ErrorCode,
    pub msg:  String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorCode {
    // Add variants as needed/relevant
}


#[derive(Debug, Clone, Copy)]
pub struct MutexPoisoned;

impl Display for MutexPoisoned {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "A mutex used by an ArcMutexContainer was poisoned")
    }
}

impl StdError for MutexPoisoned {}

impl<T> From<PoisonError<T>> for MutexPoisoned {
    #[inline]
    fn from(_err: PoisonError<T>) -> Self {
        Self
    }
}
