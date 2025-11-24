use std::borrow::Borrow;

use crate::block::BlockIterImpl;


/// Refers to an entry in a [`Table`].
///
/// [`Table`]: super::Table
#[derive(Debug)]
pub struct TableEntry<DataBuffer> {
    buffer:  DataBuffer,
    iter:    BlockIterImpl,
}

impl<DataBuffer: Borrow<Vec<u8>>> TableEntry<DataBuffer> {
    /// Returns a [`TableEntry`] which refers to the current entry of `iter`, or `None` if
    /// `iter` is not `valid()`.
    #[inline]
    #[must_use]
    pub(super) fn new(buffer: DataBuffer, iter: BlockIterImpl) -> Option<Self> {
        if iter.valid() {
            Some(Self {
                buffer,
                iter,
            })
        } else {
            None
        }
    }
}

#[expect(
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    reason = "`iter` is `valid()` at construction, and never mutated afterwards",
)]
impl<DataBuffer: Borrow<Vec<u8>>,> TableEntry<DataBuffer> {
    #[inline]
    #[must_use]
    pub fn entry(&self) -> (&[u8], &[u8]) {
        self.iter.current(self.buffer.borrow()).unwrap()
    }

    #[inline]
    #[must_use]
    pub fn key(&self) -> &[u8] {
        self.iter.current(self.buffer.borrow()).unwrap().0
    }

    #[inline]
    #[must_use]
    pub fn value(&self) -> &[u8] {
        self.iter.current(self.buffer.borrow()).unwrap().1
    }
}
