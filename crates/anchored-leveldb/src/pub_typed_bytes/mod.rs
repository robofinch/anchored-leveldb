/// `BlockHandle`.
mod block_handle;
/// `EntryType`, `PhysicalRecordType`.
mod enums;
/// `Level`, `NonZeroLevel`.
mod level;
/// `MinU32Usize`.
mod min_u32_usize;
/// `PrefixedBytes`.
mod prefixed_bytes;
/// `SequenceNumber`.
mod sequence_number;
/// `FileNumber`, `FileOffset`, `FileSize`, `TableBlockOffset`, `LogicalRecordOffset`.
mod simple_newtypes;


pub use self::{
    block_handle::BlockHandle,
    enums::{EntryType, PhysicalRecordType},
    level::{Level, NonZeroLevel, NUM_LEVELS},
    min_u32_usize::MinU32Usize,
    prefixed_bytes::PrefixedBytes,
    sequence_number::SequenceNumber,
    simple_newtypes::{
        FileNumber, FileOffset, FileSize, LogicalRecordOffset, TableBlockOffset, TableBlockSize,
    },
};
pub(crate) use self::{enums::IndexRecordTypes, prefixed_bytes::ReadPrefixedBytes};
