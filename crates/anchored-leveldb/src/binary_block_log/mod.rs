mod reader;
mod writer;
mod slices;


/// The length of each physical record's header.
pub const HEADER_SIZE: u16 = const {
    let header_size: u16 = 7;

    #[expect(clippy::as_conversions, reason = "`usize::from` not available in const context")]
    {
        assert!(
            header_size as usize == size_of::<u32>() + size_of::<u16>() + size_of::<u8>(),
            "Show why it's the magic value 7",
        );
    };
    header_size
};

/// The size of blocks in the binary log format used by `MANIFEST-_` manifest files and `_.log`
/// write-ahead log files.
///
/// Note that the code requires for correctness that `WRITE_LOG_BLOCK_SIZE + HEADER_SIZE`
/// does not overflow a `u16`, so this is essentially the largest sensible value for the block size
/// (assuming that powers of 2 are preferred).
/// Therefore, there's not any advantage in making this configurable (especially since each reader
/// and writer of a given LevelDB database would need to use the same value for this block size,
/// and all existing LevelDB databases use `1 << 15`).
pub const WRITE_LOG_BLOCK_SIZE: usize = 1 << 15;

/// Equal to [`WRITE_LOG_BLOCK_SIZE`].
pub const WRITE_LOG_BLOCK_SIZE_U16: u16 = 1 << 15;


pub(crate) use self::{
    reader::{
        BinaryBlockLogReaderBuffers, LogReader, LogRecordResult, LogicalRecord, ManifestReader,
        ManifestRecordResult,
    },
    slices::Slices,
};
