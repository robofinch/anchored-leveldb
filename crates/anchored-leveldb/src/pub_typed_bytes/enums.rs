use bijective_enum_map::injective_enum_map;

use super::simple_newtypes::FileNumber;


#[derive(Debug, Clone, Copy)]
pub enum BlockType {
    Metaindex,
    Filter,
    Index,
    Data,
}

#[derive(Debug, Clone, Copy)]
pub enum Close {
    /// Prevent any future read and write operations from succeeding, and stop any ongoing
    /// compaction as quickly as possible.
    ///
    /// If there are any active database iterators, the database will not be closed until all of
    /// them have been dropped.
    AsSoonAsPossible,
    /// Prevent any future read and write operations from succeeding, but allow any ongoing
    /// compaction to complete.
    ///
    /// If there are any active database iterators, the database will not be closed until all of
    /// them have been dropped.
    AfterCompaction,
}

#[derive(Debug, Clone, Copy)]
pub enum CloseStatus {
    /// The database has closed and its lockfile has been released.
    ///
    /// All reads, writes, and compactions have been completed, and all of its iterators have been
    /// dropped.
    Closed,
    /// The current ongoing compaction (if any) is being stopped as quickly as possible.
    ///
    /// The database will not accept new read or write operations, though any existing iterator
    /// are not invalidated (but they may begin returning `None`).
    ///
    /// The database cannot be completely closed until all ongoing reads (including via iterators)
    /// and writes (including compactions) have stopped.
    Closing,
    /// The current ongoing compaction (if any) will be completed, after which no further
    /// compactions will occur.
    ///
    /// The database will not accept new read or write operations, though any existing iterator
    /// are not invalidated (but they may begin returning `None`).
    ///
    /// The database cannot be completely closed until all ongoing reads (including via iterators)
    /// and writes (including compactions) have stopped.
    ClosingAfterCompaction,
    /// The database is open for reads. If no write errors or corruption errors have occurred,
    /// then it is also open for writes.
    Open,
}

#[derive(Debug, Clone, Copy)]
pub enum FlushWrites {
    /// Wait for all writes to be flushed to at least the write-ahead log, and sync them to
    /// persistent storage. Do not force the write-ahead log to be flushed to a table file.
    ToWriteAheadLog,
    /// Wait for all writes to be flushed to at least the write-ahead log and flush the write-ahead
    /// log to a table file (if the log is nonempty).
    ToTableFile,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum EntryType {
    Deletion = 0,
    Value    = 1,
}

impl EntryType {
    pub(crate) const MIN_TYPE: Self = Self::Deletion;
    pub(crate) const MAX_TYPE: Self = Self::Value;
}

injective_enum_map! {
    EntryType, u8,
    Deletion <=> 0,
    Value    <=> 1,
}

#[derive(Debug, Clone, Copy)]
pub enum PhysicalRecordType {
    Zero,
    Full,
    First,
    Middle,
    Last,
}

injective_enum_map! {
    PhysicalRecordType, u8,
    Zero   <=> 0,
    Full   <=> 1,
    First  <=> 2,
    Middle <=> 3,
    Last   <=> 4,
}

impl PhysicalRecordType {
    pub(crate) const ALL_TYPES: [Self; 5] = [
        Self::Zero, Self::Full, Self::First, Self::Middle, Self::Last,
    ];
}

pub(crate) trait IndexRecordTypes<T> {
    #[must_use]
    fn infallible_index(&self, record_type: PhysicalRecordType) -> &T;
}

impl<T> IndexRecordTypes<T> for [T; PhysicalRecordType::ALL_TYPES.len()] {
    fn infallible_index(&self, record_type: PhysicalRecordType) -> &T {
        // We need to ensure that `0 <= usize::from(u8::from(record_type)) < self.len()`.
        // This holds, since `self.len() == PhysicalRecordType::ALL_TYPES.len() == 5`,
        // and `0 <= usize::from(u8::from(record_type)) < 5`.
        #[expect(
            clippy::indexing_slicing,
            reason = "See above. Not pressing enough to use `unsafe`",
        )]
        &self[usize::from(u8::from(record_type))]
    }
}

/// The source of an invalid internal key in a version edit.
#[derive(Debug, Clone, Copy)]
pub enum VersionEditKeyType {
    CompactionPointer,
    /// The smallest key of a table file was invalid.
    ///
    /// # Data
    /// The file number of the table file.
    SmallestFileKey(FileNumber),
    /// The largest key of a table file was invalid.
    ///
    /// # Data
    /// The file number of the table file.
    LargestFileKey(FileNumber),
}
