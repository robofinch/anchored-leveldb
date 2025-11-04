use std::io::Error as IoError;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use thiserror::Error;

use anchored_vfs::traits::WritableFile;

use crate::config_constants::WRITE_LOG_BLOCK_SIZE;
use crate::format::{IndexRecordTypes as _, mask_checksum, WriteLogRecordType};


/// The header of each physical record is 7 bytes long.
const HEADER_SIZE: usize = size_of::<u32>() + size_of::<u16>() + size_of::<u8>();


/// A writer for the log format used by LevelDB to store serialized [`WriteBatch`]es, in the case
/// of write-ahead logs corresponding to memtables, or serialized [`VersionEdit`]s in the case
/// of MANIFEST files.
///
/// [`WriteBatch`]: crate::write_batch::WriteBatch
/// [`VersionEdit`]: crate::version::version_edit::VersionEdit
pub(crate) struct WriteLogWriter<File> {
    log_file:        File,
    type_checksums:  [u32; WriteLogRecordType::ALL_TYPES.len()],
    /// The space remaining in the current block of [`WRITE_LOG_BLOCK_SIZE`] bytes.
    ///
    /// This should be in the range `0..=WRITE_LOG_BLOCK_SIZE`, where `0` should be incremented
    /// to `WRITE_LOG_BLOCK_SIZE`.
    remaining_space: usize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File: WritableFile> WriteLogWriter<File> {
    #[must_use]
    pub fn new_empty(log_file: File) -> Self {
        let type_checksums = WriteLogRecordType::ALL_TYPES.map(|record_type| {
            crc32c::crc32c(&[u8::from(record_type)])
        });
        Self {
            log_file,
            type_checksums,
            remaining_space: WRITE_LOG_BLOCK_SIZE,
        }
    }

    #[must_use]
    pub fn new_with_offset(log_file: File, offset: u64) -> Self {
        #[expect(clippy::unwrap_used, reason = "WRITE_LOG_BLOCK_SIZE == 1 << 15 < u64::MAX")]
        let offset_into_block = offset % u64::try_from(WRITE_LOG_BLOCK_SIZE).unwrap();
        // Note that `0 <= offset_into_block < (WRITE_LOG_BLOCK_SIZE as u64)`
        #[expect(clippy::unwrap_used, reason = "WRITE_LOG_BLOCK_SIZE == 1 << 15 < usize::MAX")]
        let offset_into_block = usize::try_from(offset_into_block).unwrap();
        // Note that this does not underflow, since, again,
        // `0 <= offset_into_block < WRITE_LOG_BLOCK_SIZE`.
        // Then, `remaining_space` is in `1..=WRITE_LOG_BLOCK_SIZE`.
        let remaining_space = WRITE_LOG_BLOCK_SIZE - offset_into_block;

        let type_checksums = WriteLogRecordType::ALL_TYPES.map(|record_type| {
            crc32c::crc32c(&[u8::from(record_type)])
        });
        Self {
            log_file,
            type_checksums,
            remaining_space,
        }
    }

    /// Calls [`WritableFile::sync_data`] on the log file.
    ///
    /// THe `WriteLogWriter` syncs its log file only when this function is called.
    pub fn sync_log_data(&mut self) -> Result<(), IoError> {
        self.log_file.sync_data()
    }

    /// A failure to add a record should be treated as fatal for writes, though not necessarily
    /// reads. See the type-level documentation of [`LogWriteError`] for more.
    pub fn add_record(&mut self, record: &[u8]) -> Result<(), LogWriteError> {
        // This wrapper function's sole task is to ensure that the buffer is flushed,
        // so that `inner_add_record` can have early returns without fear.
        // Note that the below _could_ be a one-liner with `.or`, but I don't like using the eager
        // evaluation of `.or` for correctness rather than performance.
        let result = self.inner_add_record(record);
        let flush_result = self.log_file.flush();
        result.or(flush_result).map_err(LogWriteError)
    }

    fn inner_add_record(&mut self, mut record: &[u8]) -> Result<(), IoError> {
        // Indicates whether we're about to emit the first physical record for the given
        // logical `record`.
        let mut first_physical = true;
        let max_trailer = [0_u8; HEADER_SIZE - 1];

        // We permit empty records to be written as a zero-length Full physical record.
        // LevelDB does not end up calling `add_record` with empty `record`s anyway, and the
        // reader is capable of handling them, so it doesn't particularly matter whether we
        // emit an empty Full physical record or just emit no record at all.
        while !record.is_empty() || first_physical {
            if let Some(trailer) = max_trailer.get(..self.remaining_space) {
                // This implies that `self.remaining_space <= max_trailer.len()`, which occurs
                // precisely when `self.remaining_space < HEADER_SIZE`. In that situation,
                // we must write between 0 and 6 zero bytes for the trailer and then move to
                // the next block.
                self.log_file.write_all(trailer)?;
                self.remaining_space = WRITE_LOG_BLOCK_SIZE;
            }

            // We know here that `self.remaining_space >= HEADER_SIZE`.
            let logical_fragment_len = record.len().min(self.remaining_space - HEADER_SIZE);

            // Indicates whether we're about to emit the final physical record for the given
            // logical `record`.
            let last_physical = logical_fragment_len == record.len();

            let record_type = match (first_physical, last_physical) {
                (true,  true)  => WriteLogRecordType::Full,
                (true,  false) => WriteLogRecordType::First,
                (false, false) => WriteLogRecordType::Middle,
                (false, true)  => WriteLogRecordType::Last,
            };

            // Note that `logical_fragment_len <= record.len()` by `.min` above
            let (logical_fragment, remaining) = record.split_at(logical_fragment_len);

            let checksum = crc32c::crc32c_append(self.crc_for_type(record_type), logical_fragment);
            let masked_checksum = mask_checksum(checksum);
            // Note that `logical_fragment_len <= self.remaining_space - HEADER_SIZE`
            // ` < self.remaining_space <= WRITE_LOG_BLOCK_SIZE == 1 << 15 < u16::MAX`
            #[expect(
                clippy::unwrap_used,
                reason = "`WRITE_LOG_BLOCK_SIZE < u16::MAX`, so fragment len fits in two bytes",
            )]
            let fragment_len_u16 = u16::try_from(logical_fragment_len).unwrap();

            self.log_file.write_all(&masked_checksum.to_le_bytes())?;
            self.log_file.write_all(&fragment_len_u16.to_le_bytes())?;
            self.log_file.write_all(&[u8::from(record_type)])?;
            self.log_file.write_all(logical_fragment)?;

            record = remaining;
            first_physical = false;
            // Note that `logical_fragment_len <= self.remaining_space - HEADER_SIZE`
            // so `logical_fragment_len + HEADER_SIZE <= self.remaining_space`; no underflow.
            self.remaining_space -= HEADER_SIZE + logical_fragment_len;
        }

        Ok(())
    }

    #[inline]
    #[must_use]
    fn crc_for_type(&self, record_type: WriteLogRecordType) -> u32 {
        *self.type_checksums.infallible_index(record_type)
    }
}

impl<File> Debug for WriteLogWriter<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("WriteLogWriter")
            .field("log_file",        &"<File>")
            .field("type_checksums",  &self.type_checksums)
            .field("remaining_space", &self.remaining_space)
            .finish()
    }
}

/// A write log (either a `MANIFEST-_` file or a `_.log` write-ahead log file) could not be
/// written to, due to some non-interrupt IO error.
///
/// This should be considered fatal for writing to the database, necessitating that the database
/// be closed and reopened before the database could allow any more writes. Reads could be allowed.
///
/// If we had written partial record data to the write log before an error occurred,
/// we cannot go back and delete that data using the [`WritableFile`] trait, nor is it likely that
/// we'd be able to retry and write the rest of the record data. We cannot append any additional
/// data from new, unrelated records to the write log, as those entries could be corrupted by the
/// previous error. The best option seems to be going through the usual recovery process.
///
/// Besides, even though this may require more effort from the library's user to retry a write,
/// presumably they too would need to bubble up an error of this sort to the end user, who could
/// try to figure out what crazy edge case(s) happened (out of disk space? someone modified the
/// file's permissions or put a seal on it while the database was running? filesystem corruption?)
/// and try to fix the issue.
#[derive(Error, Debug)]
#[repr(transparent)]
#[error("fatal error in `WriteLogWriter::add_record`: {0}")]
pub(crate) struct LogWriteError(pub IoError);
