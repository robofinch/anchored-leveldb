use crate::all_errors::types::OutOfFileNumbers;


/// A number assigned to a file used by a LevelDB database.
///
/// It *should* be unique across all files in active use. Moreover, so long as previous accesses to
/// the database did not crash, it will indeed be unique across all files of the database (noting
/// that any files *not* in active use should not linger for long, especially if accesses to the
/// database do not crash and are given time to clean up unused files).
///
/// However, bugs in Google's leveldb may allow distinct database files to be assigned the same
/// file number in exceptional cases involving an untimely crash. (`anchored-leveldb` should not
/// have such bugs.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct FileNumber(pub u64);

impl FileNumber {
    #[inline]
    pub(crate) fn next(self) -> Result<Self, OutOfFileNumbers> {
        self.0.checked_add(1).map(Self).ok_or(OutOfFileNumbers)
    }
}

/// An offset (in bytes) into a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct FileOffset(pub u64);

/// The size (in bytes) of a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct FileSize(pub u64);

/// An offset (in bytes) into an uncompressed block of a table file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct TableBlockOffset(pub usize);

/// The size (in bytes) of a compressed block of a table file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct TableBlockSize(pub u64);

/// An offset (in bytes) into a logical record of a binary block log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct LogicalRecordOffset(pub usize);
