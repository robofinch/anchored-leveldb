use std::{convert::Infallible, error::Error as StdError};
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
    #[expect(clippy::uninhabited_references, reason = "function is unreachable")]
    #[inline]
    fn fmt(&self, _f: &mut Formatter<'_>) -> FmtResult {
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

from_never!(IoError, Infallible);

impl FSError for IoError {
    #[inline]
    fn is_not_found(&self) -> bool {
        self.kind() == ErrorKind::NotFound
    }

    #[inline]
    fn is_interrupted(&self) -> bool {
        self.kind() == ErrorKind::Interrupted
    }
}
