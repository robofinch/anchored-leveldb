/// Has length at most `u32::MAX - 8`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UserKey<'a>(&'a [u8]);

/// Has length at most `u32::MAX - 8`.
#[derive(Debug, Clone)]
pub(crate) struct OwnedUserKey(Vec<u8>);

/// Has length at most `u32::MAX`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UserValue<'a>(&'a [u8]);

/// Either a [`UserValue`] or some irrelevant byte slice (likely the empty slice).
///
/// Has length at most `u32::MAX`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MaybeUserValue<'a>(&'a [u8]);
