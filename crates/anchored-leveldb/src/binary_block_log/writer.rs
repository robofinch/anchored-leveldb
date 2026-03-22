use std::{io::Error as IoError, num::NonZeroU64};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use anchored_vfs::WritableFile;

use crate::utils::mask_checksum;
use crate::pub_typed_bytes::{
    BinaryLogBlockSize, FileOffset, IndexRecordTypes as _, PhysicalRecordType,
};
use super::{BINARY_LOG_HEADER_SIZE, slices::Slices};


/// A writer for the binary log format used by LevelDB to store serialized [`WriteBatch`]es, in the
/// case of write-ahead logs corresponding to memtables, or serialized [`VersionEdit`]s in the case
/// of MANIFEST files.
///
/// [`WriteBatch`]: crate::write_batch::WriteBatch
/// [`VersionEdit`]: crate::version::version_edit::VersionEdit
pub(crate) struct WriteLogWriter<File> {
    file:                File,
    type_checksums:      [u32; PhysicalRecordType::ALL_TYPES.len()],
    block_size:          BinaryLogBlockSize,
    /// The space remaining in the current block of `block_size` bytes.
    ///
    /// This should be in the range `0..=block_size`, where `0` should be incremented back up to
    /// to `block_size`.
    remaining_space:     usize,
    /// The number of blocks which have been written in part or in full, such that the total length
    /// of `file` should be `self.block_size * self.cur_block_index - self.remaining_space`.
    cur_block_index:     NonZeroU64,
    /// The last offset that was synced with [`Self::sync_log_data`].
    ///
    /// When recovering an existing log file, we pessimistically assume that nothing was synced.
    /// Alas, that isn't actually guaranteed to help flush previous writes to persistent storage if
    /// they weren't synced and haven't already been flushed to persistent storage, since `fsync`
    /// is based on file descriptor, not file. However, on the off chance that it slightly helps
    /// some obscure edge case... we might as well try, since the performance impact should be
    /// relatively small.
    offset_of_last_sync: FileOffset,
    /// If the current length of `file` minus `self.offset_of_last_sync` exceeds
    /// `self.bytes_per_sync`, the file is synced.
    bytes_per_sync:      NonZeroU64,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File: WritableFile> WriteLogWriter<File> {
    #[must_use]
    pub fn new_empty(file: File, block_size: BinaryLogBlockSize, bytes_per_sync: NonZeroU64) -> Self {
        let type_checksums = PhysicalRecordType::ALL_TYPES.map(|record_type| {
            crc32c::crc32c(&[u8::from(record_type)])
        });
        Self {
            file,
            type_checksums,
            block_size,
            remaining_space:     block_size.as_usize(),
            cur_block_index:     const { NonZeroU64::new(1).unwrap() },
            offset_of_last_sync: FileOffset(0),
            bytes_per_sync,
        }
    }

    #[must_use]
    pub fn new_with_offset(
        file:           File,
        offset:         FileOffset,
        block_size:     BinaryLogBlockSize,
        bytes_per_sync: NonZeroU64,
    ) -> Self {
        #[expect(clippy::integer_division, reason = "taking the floor is intentional")]
        let prev_block_index  = offset.0 / block_size.as_u64();
        let offset_into_block = offset.0 % block_size.as_u64();

        #[expect(clippy::expect_used, reason = "never fails, regardless of input")]
        let cur_block_index = {
            let cur_block_index = prev_block_index
                .checked_add(1)
                .expect("`(u64 num) / (num bigger than 2) + 1` should not overflow `u64`");

            NonZeroU64::new(cur_block_index).expect("`.checked_add(1)` yields a nonzero number")
        };

        // Note that `0 <= offset_into_block < block_size`
        #[expect(clippy::unwrap_used, reason = "block_size < u16::MAX <= usize::MAX")]
        let offset_into_block = usize::try_from(offset_into_block).unwrap();
        // Note that this does not underflow, since, again,
        // `0 <= offset_into_block < block_size`.
        // Then, `remaining_space` is in `1..=block_size`.
        let remaining_space = block_size.as_usize() - offset_into_block;

        let type_checksums = PhysicalRecordType::ALL_TYPES.map(|record_type| {
            crc32c::crc32c(&[u8::from(record_type)])
        });
        Self {
            file,
            type_checksums,
            block_size,
            remaining_space,
            cur_block_index,
            offset_of_last_sync: FileOffset(0),
            bytes_per_sync,
        }
    }

    /// # Panics
    /// Theoretically, could panic if the file length exceeds 18 exabytes and overflows a `u64`.
    #[must_use]
    pub const fn file_length(&self) -> u64 {
        #[expect(clippy::expect_used, reason = "this *could* panic, but shouldn't in practice")]
        let blocks_len = self.cur_block_index
            .get()
            .checked_mul(self.block_size.as_u64())
            .expect(
                "anchored-leveldb `.log` and `MANIFEST` files must not \
                 exceed `u64::MAX` bytes in length",
            );
        // Does not underflow, since `self.cur_block_index` is nonzero,
        // and `self.remaining_space <= WRITE_LOG_BLOCK_SIZE`.
        #[allow(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "`self.remaining_space <= WRITE_LOG_BLOCK_SIZE < u16::MAX < u64::MAX`",
        )]
        {
            blocks_len - (self.remaining_space as u64)
        }
    }

    /// Calls [`WritableFile::sync_data`] on the binary log file.
    ///
    /// The `WriteLogWriter` syncs its file only when this function is called.
    pub fn sync_log_data(&mut self) -> Result<(), IoError> {
        self.offset_of_last_sync = FileOffset(self.file_length());
        self.file.sync_data()
    }

    /// Returns `true` if the most-recent data might not been synced with [`Self::sync_log_data`].
    #[must_use]
    pub const fn needs_sync(&self) -> bool {
        self.offset_of_last_sync.0 != self.file_length()
    }

    /// A failure to add a record should be treated as fatal for writes, though not necessarily
    /// reads.
    pub fn add_record(&mut self, record: Slices<'_>) -> Result<(), IoError> {
        // This wrapper function's sole task is to ensure that the buffer is flushed,
        // so that `inner_add_record` can have early returns without fear.
        // Note that the below _could_ be a single statement with `.or`, but I don't like using the
        // eager evaluation of `.or` for correctness rather than performance.
        let result = self.inner_add_record(record);

        // Does not underflow, since `self.file_length()` is monotonically increasing, and
        // `self.offset_of_last_sync.0` is never set to a value greater than `self.file_length()`.
        let unsynced_bytes = self.file_length() - self.offset_of_last_sync.0;
        let do_sync = unsynced_bytes >= self.bytes_per_sync.get();

        let flush_result = if do_sync {
            self.sync_log_data()
        } else {
            self.file.flush()
        };
        result.or(flush_result)
    }

    fn inner_add_record(&mut self, mut record: Slices<'_>) -> Result<(), IoError> {
        // Indicates whether we're about to emit the first physical record for the given
        // logical `record`.
        let mut first_physical = true;
        #[expect(clippy::as_conversions, reason = "`usize::from` not available in const context")]
        let max_trailer = [0_u8; (BINARY_LOG_HEADER_SIZE - 1) as usize];

        // We permit empty records to be written as a zero-length `Full` physical record.
        // LevelDB does not end up using empty `record`s anyway, though the reader is capable of
        // handling them, so it doesn't particularly matter whether we emit an empty `Full`
        // physical record or just emit no record at all.
        while !record.is_empty() || first_physical {
            if let Some(trailer) = max_trailer.get(..self.remaining_space) {
                // This implies that `self.remaining_space <= max_trailer.len()`, which occurs
                // precisely when `self.remaining_space < BINARY_LOG_HEADER_SIZE`. In that
                // situation, we must write between 0 and 6 zero bytes for the trailer and then
                // move to the next block.
                self.file.write_all(trailer)?;
                self.remaining_space = self.block_size.as_usize();
            }

            // We know here that `self.remaining_space >= BINARY_LOG_HEADER_SIZE`.
            let logical_fragment_len = record.len()
                .min(self.remaining_space - usize::from(BINARY_LOG_HEADER_SIZE));

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
            // Note that `logical_fragment_len <= self.remaining_space - BINARY_LOG_HEADER_SIZE`
            // so `logical_fragment_len + BINARY_LOG_HEADER_SIZE <= self.remaining_space`;
            // no underflow.
            self.remaining_space -= usize::from(BINARY_LOG_HEADER_SIZE) + logical_fragment_len;
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
            .field("file",                &"<File>")
            .field("type_checksums",      &self.type_checksums)
            .field("block_size",          &self.block_size)
            .field("remaining_space",     &self.remaining_space)
            .field("cur_block_index",     &self.cur_block_index)
            .field("offset_of_last_sync", &self.offset_of_last_sync)
            .field("bytes_per_sync",      &self.bytes_per_sync)
            .finish()
    }
}
