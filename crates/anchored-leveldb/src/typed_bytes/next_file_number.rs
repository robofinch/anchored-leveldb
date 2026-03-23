use crate::{all_errors::types::OutOfFileNumbers, pub_typed_bytes::FileNumber};


/// The file number which should next be assigned to a log, table, or `MANIFEST` file.
///
/// It might not be up-to-date with the persisted data in the `MANIFEST` file and should be
/// written on every `MANIFEST` write.
///
/// Unfortunately, bugs in Google's leveldb implementation mean that file numbers are not
/// necessarily unique in a LevelDB database; this implementation can handle those non-unique
/// file numbers, while assigning unique file numbers itself.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NextFileNumber(FileNumber);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl NextFileNumber {
    #[inline]
    #[must_use]
    pub const fn new(next_file_number: FileNumber) -> Self {
        Self(next_file_number)
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> FileNumber {
        self.0
    }

    pub fn new_file_number(&mut self) -> Result<FileNumber, OutOfFileNumbers> {
        let new_file_number = self.0;
        self.0 = self.0.next()?;
        Ok(new_file_number)
    }

    /// Reuse the given file number if possible.
    ///
    /// If the passed `file_number` is not the newest file number (as returned by the most-recent
    /// call to [`Self::new_file_number`], for instance), nothing happens.
    pub const fn reuse_file_number(&mut self, file_number: FileNumber) {
        if self.0.0.saturating_sub(1) == file_number.0 {
            // Either `self.next_file_number == file_number` (...which shouldn't happen...)
            // and thus nothing changes, or `file_number` is one before the next file number
            // and was thus the newest in-use file number.
            self.0 = file_number;
        }
    }
}
