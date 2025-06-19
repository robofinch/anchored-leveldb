use std::{convert::Infallible, error::Error as StdError, sync::PoisonError};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    io::{Error as IoError, ErrorKind},
};

use crate::util_traits::FSError;


/// Yet another sorry reimplementation of the never type `!`, as `impl<T> From<!> for T` isn't
/// officially stable, and [`std::io::Error`] doesn't implement <code>From<[`Infallible`]></code>.
///
/// Although the never type is reachable in stable Rust (see [never-say-never]), some uses of it
/// *could* plausibly break someday, as it is still unstable.
///
/// [`Infallible`]: std::convert::Infallible
/// [never-say-never]: https://docs.rs/never-say-never
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Never {}

impl Display for Never {
    #[inline]
    fn fmt(&self, _f: &mut Formatter<'_>) -> FmtResult {
        // TLDR: This code compiles fine. This function is unreachable, and never triggers UB
        // (unless the caller *already* triggered UB by creating a `&Never`).
        // The lint against instances of types like `&Never` fired anyway.
        #[expect(
            clippy::uninhabited_references,
            reason = "We aren't the ones who created a `&Never`, any UB is the caller's fault",
        )]
        match *self {}
    }
}

impl StdError for Never {}

macro_rules! from_never {
    ($($other_ty:ty),*$(,)?) => {
        $(
            impl From<Never> for $other_ty {
                #[inline]
                fn from(never: Never) -> Self {
                    match never {}
                }
            }
        )*
    };
}

from_never!(IoError, Infallible, MutexPoisoned);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutexPoisoned;

impl<T> From<PoisonError<T>> for MutexPoisoned {
    #[inline]
    fn from(_err: PoisonError<T>) -> Self {
        Self
    }
}

impl From<MutexPoisoned> for IoError {
    fn from(err: MutexPoisoned) -> Self {
        Self::other(err)
    }
}

impl Display for MutexPoisoned {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "a mutex was poisoned")
    }
}

impl StdError for MutexPoisoned {}

impl FSError for MutexPoisoned {
    #[inline]
    fn is_not_found(&self) -> bool {
        false
    }

    #[inline]
    fn is_interrupted(&self) -> bool {
        false
    }

    #[inline]
    fn is_poison_error(&self) -> bool {
        true
    }
}

impl FSError for IoError {
    #[inline]
    fn is_not_found(&self) -> bool {
        self.kind() == ErrorKind::NotFound
    }

    #[inline]
    fn is_interrupted(&self) -> bool {
        self.kind() == ErrorKind::Interrupted
    }

    #[inline]
    fn is_poison_error(&self) -> bool {
        false
    }
}
