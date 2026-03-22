/// `BlockHandle`.
mod block_handle;
/// `BlockType`, `EntryType`, `PhysicalRecordType`.
mod enums;
/// `Level`, `NonZeroLevel`.
mod level;
/// `BinaryLogBlockSize`.
mod log_size;
/// `MinU32Usize`.
mod min_u32_usize;
/// `PrefixedBytes`.
mod prefixed_bytes;
/// `SequenceNumber`.
mod sequence_number;
/// `ShortSlice`.
mod short_slice;
/// `FileNumber`, `FileOffset`, `FileSize`, `TableBlockOffset`, `LogicalRecordOffset`.
mod simple_newtypes;


pub use self::{
    block_handle::BlockHandle,
    enums::{BlockType, EntryType, PhysicalRecordType, VersionEditKeyType},
    level::{
        Level, NonZeroLevel, NUM_LEVELS, NUM_LEVELS_USIZE, NUM_MIDDLE_LEVELS,
        NUM_MIDDLE_LEVELS_USIZE, NUM_NONZERO_LEVELS, NUM_NONZERO_LEVELS_USIZE,
    },
    log_size::BinaryLogBlockSize,
    min_u32_usize::MinU32Usize,
    prefixed_bytes::PrefixedBytes,
    sequence_number::SequenceNumber,
    simple_newtypes::{
        FileNumber, FileOffset, FileSize, LogicalRecordOffset, TableBlockOffset, TableBlockSize,
    },
    short_slice::ShortSlice,
};
pub(crate) use self::{enums::IndexRecordTypes, prefixed_bytes::ReadPrefixedBytes};
pub(crate) use self::level::{IndexLevel, IndexNonZeroLevel};
