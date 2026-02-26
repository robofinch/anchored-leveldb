use super::user::{MaybeUserValue, UserKey};


#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalKey<'a>(UserKey<'a>, InternalKeyTag);

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct InternalKeyTag(u64);

#[derive(Debug, Clone, Copy)]
pub(crate) struct LookupKey<'a>(UserKey<'a>, CmpSequenceTag);

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct CmpSequenceTag(u64);

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalEntry<'a>(InternalKey<'a>, MaybeUserValue<'a>);

/// A user key followed by an 8-byte suffix from a little-endian [`InternalKeyTag`].
///
/// The user key *should* be comparable.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct EncodedInternalKey<'a>(&'a [u8]);

/// *Should* be an [`EncodedInternalKey`], but might not be.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct UnvalidatedInternalKey<'a>(&'a [u8]);
