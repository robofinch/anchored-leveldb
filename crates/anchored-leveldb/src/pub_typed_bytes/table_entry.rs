use crate::{pub_traits::pool::ByteBuffer, sstable::SSTableEntry};
use crate::typed_bytes::{UserKey, UserValue};
use super::short_slice::ShortSlice;


#[derive(Debug, Clone, Copy)]
pub struct TableEntry<'a> {
    pub key:   ShortSlice<'a>,
    pub value: ShortSlice<'a>,
}

impl<'a> TableEntry<'a> {
    #[inline]
    #[must_use]
    pub(crate) const fn from_user_tuple((key, value): (UserKey<'a>, UserValue<'a>)) -> Self {
        Self {
            key:   key.short(),
            value: value.short()
        }
    }

    #[inline]
    #[must_use]
    pub const fn new(key: ShortSlice<'a>, value: ShortSlice<'a>) -> Self {
        Self { key, value }
    }

    #[inline]
    #[must_use]
    pub const fn key_bytes(self) -> &'a [u8] {
        self.key.inner()
    }

    #[inline]
    #[must_use]
    pub const fn value_bytes(self) -> &'a [u8] {
        self.value.inner()
    }

    #[inline]
    #[must_use]
    pub const fn key_short(self) -> ShortSlice<'a> {
        self.key
    }

    #[inline]
    #[must_use]
    pub const fn value_short(self) -> ShortSlice<'a> {
        self.value
    }

    #[inline]
    #[must_use]
    pub const fn as_bytes_tuple(self) -> (&'a [u8], &'a [u8]) {
        (self.key.inner(), self.value.inner())
    }

    #[inline]
    #[must_use]
    pub const fn as_short_tuple(self) -> (ShortSlice<'a>, ShortSlice<'a>) {
        (self.key, self.value)
    }
}

#[derive(Debug)]
pub struct OwnedTableEntry<PooledBuffer>(SSTableEntry<PooledBuffer>);

impl<PooledBuffer> OwnedTableEntry<PooledBuffer> {
    /// # Correctness
    /// This should only be called on `Value` entries, not `Deletion` entries.
    #[inline]
    #[must_use]
    pub(crate) const fn new_not_deleted(entry: SSTableEntry<PooledBuffer>) -> Self {
        Self(entry)
    }
}

impl<PooledBuffer: ByteBuffer> OwnedTableEntry<PooledBuffer> {
    #[inline]
    #[must_use]
    pub fn borrow(&self) -> TableEntry<'_> {
        TableEntry::new(
            self.0.key().as_internal_key().0.short(),
            self.0.value().0,
        )
    }

    #[inline]
    #[must_use]
    pub fn key_bytes(&self) -> &[u8] {
        self.borrow().key_bytes()
    }

    #[inline]
    #[must_use]
    pub fn value_bytes(&self) -> &[u8] {
        self.borrow().value_bytes()
    }

    #[inline]
    #[must_use]
    pub fn key_short(&self) -> ShortSlice<'_> {
        self.borrow().key_short()
    }

    #[inline]
    #[must_use]
    pub fn value_short(&self) -> ShortSlice<'_> {
        self.borrow().value_short()
    }

    #[inline]
    #[must_use]
    pub fn as_bytes_tuple(&self) -> (&[u8], &[u8]) {
        self.borrow().as_bytes_tuple()
    }

    #[inline]
    #[must_use]
    pub fn as_short_tuple(&self) -> (ShortSlice<'_>, ShortSlice<'_>) {
        self.borrow().as_short_tuple()
    }
}
