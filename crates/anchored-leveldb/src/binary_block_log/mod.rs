mod reader;
mod writer;
mod slices;


/// The length of each physical record's header.
pub(crate) const BINARY_LOG_HEADER_SIZE: u16 = const {
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


pub(crate) use self::{slices::Slices, writer::WriteLogWriter};
pub(crate) use self::reader::{
    BinaryBlockLogReaderBuffers, LogReader, LogRecordResult, LogicalRecord, ManifestReader,
    ManifestRecordResult,
};
