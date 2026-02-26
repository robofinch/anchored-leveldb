/// `BlockHandle`.
mod block_handle;
/// `EntryType`, `PhysicalRecordType`.
mod enums;
/// `FileNumber`.
mod file_number;
/// `Level`, `NonZeroLevel`.
mod level;
/// `MinU32Usize`.
mod min_u32_usize;
/// `FileOffset`, `TableBlockOffset`, `LogicalRecordOffset`.
mod offsets;
/// `PrefixedBytes`.
mod prefixed_bytes;
/// `SequenceNumber`.
mod sequence_number;
/// `WriteBatch`, `WriteBatchData`, `WriteBatchIter`, `WriteEntry`.
///
/// Note that `WriteBatchIter` and `WriteEntry` are for the benefit of users. They aren't used
/// within this crate (excluding tests).
///
/// NOTE: I am still working out the `WriteBatch` format, and am leaning towards doing something
/// different than Google's `leveldb`.
mod write_batch;


pub use self::{
    block_handle::BlockHandle,
    enums::{EntryType, PhysicalRecordType},
    file_number::FileNumber,
    level::{Level, NonZeroLevel},
    min_u32_usize::MinU32Usize,
    offsets::{FileOffset, LogicalRecordOffset, TableBlockOffset},
    prefixed_bytes::PrefixedBytes,
    sequence_number::SequenceNumber,
    write_batch::WriteEntry,
};
