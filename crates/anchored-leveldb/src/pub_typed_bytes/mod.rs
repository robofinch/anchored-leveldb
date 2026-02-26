/// `BlockHandle`.
pub(crate) mod block_handle;
/// `EntryType`, `PhysicalRecordType`.
pub(crate) mod enums;
/// `FileNumber`.
pub(crate) mod file_number;
/// `Level`, `NonZeroLevel`.
pub(crate) mod level;
/// `MinU32Usize`.
pub(crate) mod min_u32_usize;
/// `FileOffset`, `BlockOffset`, `LogicalRecordOffset`.
pub(crate) mod offsets;
/// `PrefixedBytes`.
pub(crate) mod prefixed_bytes;
/// `SequenceNumber`.
pub(crate) mod sequence_number;
/// `WriteBatch`, `WriteBatchData`, `WriteBatchIter`, `WriteEntry`.
///
/// Note that `WriteBatchIter` and `WriteEntry` are for the benefit of users. They aren't used
/// within this crate (excluding tests).
pub(crate) mod write_batch;
