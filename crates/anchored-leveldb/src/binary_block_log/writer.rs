use std::io::Error as IoError;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use anchored_vfs::WritableFile;

use crate::utils::mask_checksum;
use crate::pub_typed_bytes::{IndexRecordTypes as _, PhysicalRecordType};
use super::{HEADER_SIZE, slices::Slices, WRITE_LOG_BLOCK_SIZE, WRITE_LOG_BLOCK_SIZE_U16};


/// A writer for the binary log format used by LevelDB to store serialized [`WriteBatch`]es, in the
/// case of write-ahead logs corresponding to memtables, or serialized [`VersionEdit`]s in the case
/// of MANIFEST files.
///
/// [`WriteBatch`]: crate::write_batch::WriteBatch
/// [`VersionEdit`]: crate::version::version_edit::VersionEdit
pub(crate) struct WriteLogWriter<File> {
    file:            File,
    type_checksums:  [u32; PhysicalRecordType::ALL_TYPES.len()],
    /// The space remaining in the current block of [`WRITE_LOG_BLOCK_SIZE`] bytes.
    ///
    /// This should be in the range `0..=WRITE_LOG_BLOCK_SIZE`, where `0` should be incremented
    /// to `WRITE_LOG_BLOCK_SIZE`.
    remaining_space: usize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File: WritableFile> WriteLogWriter<File> {
    #[must_use]
    pub fn new_empty(file: File) -> Self {
        let type_checksums = PhysicalRecordType::ALL_TYPES.map(|record_type| {
            crc32c::crc32c(&[u8::from(record_type)])
        });
        Self {
            file,
            type_checksums,
            remaining_space: WRITE_LOG_BLOCK_SIZE,
        }
    }

    #[must_use]
    pub fn new_with_offset(file: File, offset: u64) -> Self {
        let offset_into_block = offset % u64::from(WRITE_LOG_BLOCK_SIZE_U16);
        // Note that `0 <= offset_into_block < WRITE_LOG_BLOCK_SIZE`
        #[expect(clippy::unwrap_used, reason = "WRITE_LOG_BLOCK_SIZE < u16::MAX <= usize::MAX")]
        let offset_into_block = usize::try_from(offset_into_block).unwrap();
        // Note that this does not underflow, since, again,
        // `0 <= offset_into_block < WRITE_LOG_BLOCK_SIZE`.
        // Then, `remaining_space` is in `1..=WRITE_LOG_BLOCK_SIZE`.
        let remaining_space = WRITE_LOG_BLOCK_SIZE - offset_into_block;

        let type_checksums = PhysicalRecordType::ALL_TYPES.map(|record_type| {
            crc32c::crc32c(&[u8::from(record_type)])
        });
        Self {
            file,
            type_checksums,
            remaining_space,
        }
    }

    /// Calls [`WritableFile::sync_data`] on the binary log file.
    ///
    /// The `WriteLogWriter` syncs its file only when this function is called.
    pub fn sync_log_data(&mut self) -> Result<(), IoError> {
        self.file.sync_data()
    }

    /// A failure to add a record should be treated as fatal for writes, though not necessarily
    /// reads.
    pub fn add_record(&mut self, record: Slices<'_>) -> Result<(), IoError> {
        // This wrapper function's sole task is to ensure that the buffer is flushed,
        // so that `inner_add_record` can have early returns without fear.
        // Note that the below _could_ be a one-liner with `.or`, but I don't like using the eager
        // evaluation of `.or` for correctness rather than performance.
        let result = self.inner_add_record(record);
        let flush_result = self.file.flush();
        result.or(flush_result)
    }

    fn inner_add_record(&mut self, mut record: Slices<'_>) -> Result<(), IoError> {
        // Indicates whether we're about to emit the first physical record for the given
        // logical `record`.
        let mut first_physical = true;
        #[expect(clippy::as_conversions, reason = "`usize::from` not available in const context")]
        let max_trailer = [0_u8; (HEADER_SIZE - 1) as usize];

        // We permit empty records to be written as a zero-length `Full` physical record.
        // LevelDB does not end up using empty `record`s anyway, though the reader is capable of
        // handling them, so it doesn't particularly matter whether we emit an empty `Full`
        // physical record or just emit no record at all.
        while !record.is_empty() || first_physical {
            if let Some(trailer) = max_trailer.get(..self.remaining_space) {
                // This implies that `self.remaining_space <= max_trailer.len()`, which occurs
                // precisely when `self.remaining_space < HEADER_SIZE`. In that situation,
                // we must write between 0 and 6 zero bytes for the trailer and then move to
                // the next block.
                self.file.write_all(trailer)?;
                self.remaining_space = WRITE_LOG_BLOCK_SIZE;
            }

            // We know here that `self.remaining_space >= HEADER_SIZE`.
            let logical_fragment_len = record.len()
                .min(self.remaining_space - usize::from(HEADER_SIZE));

            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "`self.remaining_space <= WRITE_LOG_BLOCK_SIZE < u16::MAX`",
            )]
            let fragment_len_u16 = logical_fragment_len as u16;

            // Indicates whether we're about to emit the final physical record for the given
            // logical `record`.
            let last_physical = logical_fragment_len == record.len();

            let record_type = match (first_physical, last_physical) {
                (true,  true)  => PhysicalRecordType::Full,
                (true,  false) => PhysicalRecordType::First,
                (false, false) => PhysicalRecordType::Middle,
                (false, true)  => PhysicalRecordType::Last,
            };

            // Note that `logical_fragment_len <= record.len()` by `.min` above
            let checksum = record.fold_in_prefix(
                logical_fragment_len,
                self.crc_for_type(record_type),
                crc32c::crc32c_append,
            );
            let masked_checksum = mask_checksum(checksum);

            self.file.write_all(&masked_checksum.to_le_bytes())?;
            self.file.write_all(&fragment_len_u16.to_le_bytes())?;
            self.file.write_all(&[u8::from(record_type)])?;

            record.try_pop_each_in_prefix(logical_fragment_len, |fragment| {
                self.file.write_all(fragment)
            })?;

            first_physical = false;
            // Note that `logical_fragment_len <= self.remaining_space - HEADER_SIZE`
            // so `logical_fragment_len + HEADER_SIZE <= self.remaining_space`; no underflow.
            self.remaining_space -= usize::from(HEADER_SIZE) + logical_fragment_len;
        }

        Ok(())
    }

    #[inline]
    #[must_use]
    fn crc_for_type(&self, record_type: PhysicalRecordType) -> u32 {
        *self.type_checksums.infallible_index(record_type)
    }
}

impl<File> Debug for WriteLogWriter<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("WriteLogWriter")
            .field("file",            &"<File>")
            .field("type_checksums",  &self.type_checksums)
            .field("remaining_space", &self.remaining_space)
            .finish()
    }
}
