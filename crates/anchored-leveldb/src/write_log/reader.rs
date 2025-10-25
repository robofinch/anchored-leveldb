#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to perform Polonius-style lifetime extension"),
)]

#[cfg(not(feature = "polonius"))]
use std::slice;
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    io::{Error as IoError, ErrorKind, Read},
};

use thiserror::Error;

use crate::format::{unmask_checksum, WriteLogRecordType, WRITE_LOG_BLOCK_SIZE};


/// The header of each physical record is 7 bytes long.
const HEADER_SIZE: usize = size_of::<u32>() + size_of::<u16>() + size_of::<u8>();


/// A reader for the log format used by LevelDB to store serialized [`WriteBatch`]es, in the case
/// of write-ahead logs corresponding to memtables, or serialized [`VersionEdit`]s in the case
/// of MANIFEST files.
///
/// [`WriteBatch`]: crate::write_batch::WriteBatch
/// [`VersionEdit`]: crate::version::version_edit::VersionEdit
pub(crate) struct WriteLogReader<'a, File> {
    /// Should always have length equal to [`WRITE_LOG_BLOCK_SIZE`] unless end-of-file is reached.
    ///
    /// Outside of [`InnerReader::fill_block_until_eof`] and the constructors, the data should be
    /// bytes read from the log file, not just zeroes or junk data.
    block_buffer: Vec<u8>,
    reader:       InnerReader<'a, File>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a, File: Read> WriteLogReader<'a, File> {
    #[must_use]
    pub fn new<E: ErrorHandler + 'a>(log_file: File, error_handler: E) -> Self {
        Self::new_with_boxed_handler(log_file, Box::new(error_handler))
    }

    #[must_use]
    pub fn new_with_boxed_handler(
        log_file:      File,
        error_handler: Box<dyn ErrorHandler + 'a>,
    ) -> Self {
        let mut block_buffer = vec![0; WRITE_LOG_BLOCK_SIZE];
        let reader = InnerReader::new(log_file, error_handler, &mut block_buffer);
        Self { block_buffer, reader }
    }

    #[must_use]
    pub fn read_record(&mut self) -> Option<(&[u8], u64)> {
        self.reader.read_record(&mut self.block_buffer)
    }
}

impl<File> Debug for WriteLogReader<'_, File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("WriteLogReader")
            .field("block_buffer", &format!("[{} bytes]", self.block_buffer.len()))
            .field("reader", &self.reader)
            .finish()
    }
}

/// Inner reader to get around lifetime issues. The fact that the borrows on the `block_buffer`
/// are disjoint from mutable borrows on other parts of the struct relies on knowledge of
/// function bodies, unless it's split out as done here.
struct InnerReader<'a, File> {
    log_file:           File,
    error_handler:      Box<dyn ErrorHandler + 'a>,
    /// Offset in the current block of the next physical record, if any.
    ///
    /// At all times, we should have `self.offset_in_block <= block_buffer.len()`.
    offset_in_block:    usize,
    /// Total offset in the file of the next physical record, if any.
    next_record_offset: u64,
    /// Buffer for fragmented logical records (aside from fragmented logical records whose
    /// initial First entry is empty and whose second entry contains all the data).
    record_buffer:      Vec<u8>,
}

impl<'a, File: Read> InnerReader<'a, File> {
    #[must_use]
    fn new(
        log_file:      File,
        error_handler: Box<dyn ErrorHandler + 'a>,
        block_buffer:  &mut Vec<u8>,
    ) -> Self {
        let mut this = Self {
            log_file,
            error_handler,
            offset_in_block:    0,
            next_record_offset: 0,
            record_buffer:      Vec::new(),
        };
        this.fill_block_until_eof(block_buffer);
        this
    }
}

impl<File: Read> InnerReader<'_, File> {
    /// This function returns the next logical record which does not contain any errors, in
    /// addition to the file offset of the start of that record.
    ///
    /// In the main database code, we actually report that opening the database fails if any
    /// error is detected (so this function could have chosen to bail out after the first error),
    /// but this behavior is useful for debugging and the performance of the "something has gone
    /// horribly wrong" path doesn't matter very much.
    fn read_record<'b>(&'b mut self, block_buffer: &'b mut Vec<u8>) -> Option<(&'b [u8], u64)> {
        // Note that setting `fragmented = false` discards the contents of `self.record_buffer`,
        // since `self.record_buffer` is only used for fragmented records, which must start with a
        // First record, and the branch for First does make sure to clear `self.record_buffer`.
        let mut fragmented = false;
        // File offset of the start of a whole fragmented logical record.
        // If we're near EOF, or if part of the would-be next logical record is corrupt, it
        // might not be the next offset we return.
        let mut fragment_start_offset = self.next_record_offset;

        macro_rules! error_if_in_fragmented {
            () => {
                if fragmented {
                    self.error_handler.error(
                        self.record_buffer.len(),
                        LogReadError::ErrorInFragmentedRecord,
                    );
                }
                // See above; this discards the contents of `self.record_buffer`.
                fragmented = false;
            };
        }

        loop {
            let physical_record_offset = self.next_record_offset;

            match self.read_physical_record(block_buffer) {
                PhysicalRecordResult::PhysicalRecord(record_type, record) => {
                    match WriteLogRecordType::try_from(record_type) {
                        Ok(WriteLogRecordType::Full) => {
                            if fragmented {
                                if self.record_buffer.is_empty() {
                                    // See `FullInFragmentedRecord` for details,
                                    // but we must ignore this for backwards compatibility.
                                } else {
                                    self.error_handler.error(
                                        self.record_buffer.len(),
                                        LogReadError::FullInFragmentedRecord,
                                    );
                                }
                            }
                            // We discard the contents of `self.record_buffer` when we return,
                            // since `fragmented` would be set to `false` when `read_record`
                            // is next called.

                            // We're doing a polonius-style conditional early return of a borrow.

                            // SAFETY: `record.as_ptr()` is non-null, properly aligned, valid for
                            // reads of `record.len()` bytes, points to `record.len()`-many valid
                            // bytes, and doesn't have too long of a length, since it came from a
                            // valid slice. The sole remaining constraint is the lifetime. The
                            // returned reference is valid for as long as `self.block_buffer` is
                            // borrowed, which is `'b`, which is the lifetime to which we are
                            // extending this lifetime.
                            // Further, the code compiles under Polonius, so it's sound.
                            #[cfg(not(feature = "polonius"))]
                            let record: &'b [u8] = unsafe {
                                slice::from_raw_parts(record.as_ptr(), record.len())
                            };

                            return Some((record, physical_record_offset));
                        }
                        Ok(WriteLogRecordType::First) => {
                            if fragmented {
                                if self.record_buffer.is_empty() {
                                    // See `ExtraFirstInFragmentedRecord` for details,
                                    // but we must ignore this for backwards compatibility.
                                } else {
                                    self.error_handler.error(
                                        self.record_buffer.len(),
                                        LogReadError::ExtraFirstInFragmentedRecord,
                                    );
                                }
                            }
                            fragmented = true;
                            self.record_buffer.clear();
                            self.record_buffer.extend(record);
                            fragment_start_offset = physical_record_offset;
                            // Continue iteration, read rest of fragmented record.
                        }
                        Ok(WriteLogRecordType::Middle) => {
                            if fragmented {
                                self.record_buffer.extend(record);
                                // Continue iteration, read rest of fragmented record.
                            } else {
                                self.error_handler.error(
                                    record.len(),
                                    LogReadError::MiddleWithoutFirst,
                                );
                                // Continue iteration, look for next valid record.
                            }
                        }
                        Ok(WriteLogRecordType::Last) => {
                            if fragmented {
                                // We discard the contents of `self.record_buffer` when we return,
                                // since `fragmented` would be set to `false` when `read_record`
                                // is next called.

                                if self.record_buffer.is_empty() {
                                    // We're lucky, no need to append to `self.record_buffer`.

                                    // SAFETY: The code compiles under Polonius, so it's sound.
                                    // See above for the minor details about slice lifetime
                                    // extension.
                                    #[cfg(not(feature = "polonius"))]
                                    let record: &'b [u8] = unsafe {
                                        slice::from_raw_parts(record.as_ptr(), record.len())
                                    };

                                    return Some((record, fragment_start_offset));
                                } else {
                                    self.record_buffer.extend(record);

                                    let frag_rec: &[u8] = &self.record_buffer;

                                    // SAFETY: The code compiles under Polonius, so it's sound.
                                    // See above for the minor details about slice lifetime
                                    // extension.
                                    #[cfg(not(feature = "polonius"))]
                                    let frag_rec: &'b [u8] = unsafe {
                                        slice::from_raw_parts(frag_rec.as_ptr(), frag_rec.len())
                                    };

                                    return Some((frag_rec, fragment_start_offset));
                                }
                            } else {
                                self.error_handler.error(
                                    record.len(),
                                    LogReadError::LastWithoutFirst,
                                );
                                // Continue iteration, look for next valid record.
                            }
                        }
                        Ok(WriteLogRecordType::Zero) | Err(()) => {
                            self.error_handler.error(
                                record.len(),
                                LogReadError::UnknownRecordType(record_type),
                            );
                            error_if_in_fragmented!();
                            // Continue iteration, look for next valid record.
                        }
                    }
                }
                PhysicalRecordResult::EndOfFile => {
                    // Conceivably, even if we're in a fragmented logical record, a writer could
                    // wrongly report that it had successfully written data but not use fsync and
                    // have it fail to save, but that case is indistinguishable from a writer which
                    // crashed but would have used fsync after finishing a write.
                    // Therefore: *use fsync* if you don't want to lose a small amount of recent
                    // data. We do not report dropping any bytes from early EOF as an error
                    // or corruption.
                    // We discard the contents of `self.record_buffer` when we return,
                    // since `fragmented` would be set to `false` when `read_record` is next called.
                    return None;
                }
                PhysicalRecordResult::Error => {
                    error_if_in_fragmented!();
                    // Continue iteration, look for next valid record.
                }
            }
        }
    }

    /// This function performs a small amount of work at the start (advancing to the next block
    /// if necessary) and at the end (advance offsets to the next physical record within the
    /// current block or file, return the current physical record as a slice).
    ///
    /// Most of the function is error checking.
    fn read_physical_record<'b>(
        &mut self,
        block_buffer: &'b mut Vec<u8>,
    ) -> PhysicalRecordResult<'b> {
        /// This would be a method, but it wouldn't be usable here due to
        /// function-level boundaries in the borrow checker.
        macro_rules! increment_next_record_offset {
            ($delta:expr) => {
                // No file will actually be 16 exabytes in length, but might as well be thorough.
                self.next_record_offset = self.next_record_offset.saturating_add(
                    u64::try_from($delta).unwrap_or(u64::MAX),
                );
            };
        }

        // Note that `self.offset_in_block <= WRITE_LOG_BLOCK_SIZE == 1 << 15`, so this addition
        // wouldn't even overflow a u16.
        if self.offset_in_block + HEADER_SIZE > block_buffer.len() {
            // Skip any remaining trailer (or incomplete header) bytes in this block,
            // and read the next block (if there is one).
            self.fill_block_until_eof(block_buffer);

            // If we still haven't read enough data, then clearly we're at EOF and have processed
            // every physical record in the file.
            // Note that anywhere from 0-6 bytes of remaining data, either here or before
            // calling `self.fill_block_until_eof()` just above, is perfectly fine;
            // either no data remains (we precisely read it all), a trailer remains (zero bytes
            // at the end of a block), or it's an incomplete header (perhaps a writer crashed
            // as it was writing to the log file). In no case is that considered an error or
            // instance of corruption.
            // Conceivably, a writer could wrongly report that it had successfully written data
            // but not use fsync and have it fail to save, but that case is indistinguishable
            // from a writer which crashed but would have used fsync after finishing a write.
            // Therefore: *use fsync* if you don't want to lose a small amount of recent data.
            if self.offset_in_block + HEADER_SIZE > block_buffer.len() {
                return PhysicalRecordResult::EndOfFile;
            }
        }

        // We can easily see that if we get here, then
        // `self.offset_in_block + HEADER_SIZE <= block_buffer.len()`.
        let (next_physical_record, masked_checksum, length, record_type) = {{
            #![expect(clippy::indexing_slicing, reason = "we checked the lengths")]
            #![expect(clippy::unwrap_used, reason = "valid slice -> array conversion")]

            let next_physical_record = &block_buffer[self.offset_in_block..];
            // Note that `next_physical_record.len() >= HEADER_SIZE == 7 > 6`
            assert!(next_physical_record.len() > 6, "would have returned EOF otherwise");

            let masked_checksum: [u8; 4] = next_physical_record[0..4].try_into().unwrap();
            let length: [u8; 2] = next_physical_record[4..6].try_into().unwrap();
            let record_type: u8 = next_physical_record[6];

            (
                next_physical_record,
                u32::from_le_bytes(masked_checksum),
                u16::from_le_bytes(length),
                record_type,
            )
        }};

        // Now, if usize were 16 bits, many problems would surely occur elsewhere, but might as
        // well do the stubbornly correct approach.
        let alleged_length = HEADER_SIZE.saturating_add(usize::from(length));
        let max_reasonable_length = WRITE_LOG_BLOCK_SIZE - self.offset_in_block;
        // Note that `len_to_end_of_block == block_buffer.len() - self.offset_in_block`.
        let len_to_end_of_block = next_physical_record.len();

        if alleged_length > max_reasonable_length {
            self.error_handler.error(
                len_to_end_of_block,
                LogReadError::CorruptedRecordLength,
            );

            // Oh well, we don't know exactly where the next entry should actually begin;
            // skip to the next block.
            increment_next_record_offset!(len_to_end_of_block);
            self.offset_in_block = block_buffer.len();
            return PhysicalRecordResult::Error;

        }

        if alleged_length > len_to_end_of_block {
            // If we get here, then `max_reasonable_length >= alleged_length > len_to_end_of_block`.
            // Note that this implies `block_buffer.len() < WRITE_LOG_BLOCK_SIZE`,
            // so we've reached EOF.
            // As above, conceivably a writer could have either reported its write as complete
            // without using fsync, or crashed before it had a chance to fsync. We do not
            // report this as an error or corruption.

            // Note that we could perhaps push `self.offset_in_block` forwards to avoid recomputing
            // the fact that there is no next physical record.... but we don't call
            // `read_physical_record` any additional times in practice, and it's not *incorrect* to
            // do slightly more computational work if it were called again.
            return PhysicalRecordResult::EndOfFile;
        }

        // Note that `HEADER_SIZE + usize::from(length)` does not overflow, else we would have
        // `HEADER_SIZE.saturating_add(usize::from(length)) == usize::MAX`
        // which is certainly strictly greater than `max_reasonable_length` above.
        let length_with_header = alleged_length;

        if length == 0 && record_type == u8::from(WriteLogRecordType::Zero) {
            // This is, unfortunately, another case where it seems the best option is to
            // skip to the next block. See the documentation for `ZeroRecord` for more.
            self.error_handler.error(0, LogReadError::ZeroRecord);
            increment_next_record_offset!(len_to_end_of_block);
            self.offset_in_block = block_buffer.len();
            return PhysicalRecordResult::Error;
        }

        // Note that `6..` excludes the checksum and length from the data being checksummed;
        // precisely the physical record type and data are checksummed.
        #[expect(clippy::indexing_slicing, reason = "see above; len is >= 7 == HEADER_SIZE")]
        let actual_crc = crc32c::crc32c(&next_physical_record[6..length_with_header]);
        let expected_crc = unmask_checksum(masked_checksum);
        if actual_crc != expected_crc {
            // Unfortunately... yet again we must skip to the next block. For all we know,
            // the `length` field was corrupted, too.
            self.error_handler.error(
                len_to_end_of_block,
                LogReadError::ChecksumMismatch,
            );
            increment_next_record_offset!(len_to_end_of_block);
            self.offset_in_block = block_buffer.len();
            return PhysicalRecordResult::Error;
        }

        increment_next_record_offset!(length_with_header);
        self.offset_in_block += length_with_header;

        // Note again that this addition wouldn't even overflow a u16.
        if self.offset_in_block + HEADER_SIZE > block_buffer.len() {
            // The next time we call `Self::read_physical_record`, we know that we will be
            // either doing nothing whatsoever because we reached EOF
            // or we'll set `self.offset_in_block` to position 0 of the next block, which is the
            // same (semantically) as position `block_buffer.len()` of the current block.
            // We perform that update now and NOT later in order to ensure that
            // `self.read_record` can see the file offset of the next physical record, without
            // needing to conditionally apply an offset to handle the trailer at the end of blocks.
            increment_next_record_offset!(block_buffer.len() - self.offset_in_block);
        }

        // Note that `length_with_header <= len_to_end_of_block == next_physical_record.len()`.
        #[expect(clippy::indexing_slicing, reason = "see above, and len is >= HEADER_SIZE")]
        PhysicalRecordResult::PhysicalRecord(
            record_type,
            &next_physical_record[HEADER_SIZE..length_with_header],
        )
    }

    /// Fill `block_buffer` until either its whole length is filled, EOF is reached, or some
    /// non-interrupt IO error occurs.
    ///
    /// Unless EOF was previously reached, the length of `block_buffer` is resized to the
    /// number of bytes read, `self.offset_in_block` is reset to zero, and
    /// `self.next_record_offset` is **NOT** advanced; if EOF was previously reached, then `self`
    /// is not mutated. Note that `block_buffer` has length equal to [`WRITE_LOG_BLOCK_SIZE`]
    /// if and only if EOF has not yet been reached.
    ///
    /// Reads are retried if interrupted, but if a non-interrupt IO error occurs, the function
    /// acts as though zero bytes were successfully read (so `block_buffer` is resized to zero
    /// bytes) and an error is reported to the handler (indicating the number of bytes which had
    /// been successfully read but were dropped due to the error.
    fn fill_block_until_eof(&mut self, block_buffer: &mut Vec<u8>) {
        if block_buffer.len() != WRITE_LOG_BLOCK_SIZE {
            return;
        }
        //    just-returned physical record    self.offset_in_block    next actual physical record
        // 0                        v                v                   v        end of next block
        // | ----------- | ------------------------- |   | ---------------- | ------ |    |
        // |                                           ^ |                                |
        // |             delta for self.next_record_offset (includes any trailer bytes)   |
        //
        // `self.offset_in_block` could be anywhere from `0` to the end of the current block;
        // the above diagram is just one case. Worth keeping in mind that position
        // `block_buffer.len()` in the current block is the same as position 0 in the next
        // block.

        // We do NOT need to increment `self.next_record_offset` right here; we instead
        // do it slightly early, at the end of `read_physical_record`, so that it always
        // points to the next physical record in the file (or to the end of the file), even if
        // there is no next physical record in the current block.

        // We're going to move to a new block (if an IO error occurs, pretend that we reached
        // EOF and the new block is length 0).
        self.offset_in_block = 0;

        let mut buf = block_buffer.as_mut_slice();

        while !buf.is_empty() {
            match self.log_file.read(buf) {
                Ok(0) => break,
                Ok(n) => {
                    // Yes, a bad Read implementation *could* cause a panic here. But we aren't
                    // doing anything `unsafe` here, so a panic due to someone else's buggy
                    // `Read` is fine.
                    #[expect(
                        clippy::indexing_slicing,
                        reason = "Return value of `Read::read` should be <= `buf.len()`",
                    )]
                    {
                        buf = &mut buf[n..];
                    }
                    // Note that we continue to the next iteration
                }
                Err(io_err) => {
                     // Ignore interrupts
                    if matches!(io_err.kind(), ErrorKind::Interrupted) {
                        continue;
                    }

                    let len_already_read = WRITE_LOG_BLOCK_SIZE - buf.len();

                    // Make future efforts to read from the log file indicate that EOF was reached.
                    block_buffer.clear();

                    self.error_handler.error(
                        len_already_read,
                        LogReadError::FileReadError(io_err),
                    );
                    return;
                }
            }
        }

        // Remove the last `buf.len()` elements of the block buffer, which could not be read
        // due to EOF. (There may be zero such elements.)
        let remaining_len = buf.len();
        block_buffer.truncate(block_buffer.len() - remaining_len);
    }
}

impl<File> Debug for InnerReader<'_, File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InnerReader")
            .field("log_file", &"<File>")
            .field("error_handler", &"<Box<dyn ErrorHandler + 'a>>")
            .field("offset_in_block", &self.offset_in_block)
            .field("next_record_offset", &self.next_record_offset)
            .field("record_buffer", &format!("[{} bytes]", self.record_buffer.len()))
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
enum PhysicalRecordResult<'a> {
    PhysicalRecord(u8, &'a [u8]),
    EndOfFile,
    Error,
}

pub(crate) trait ErrorHandler {
    fn error(&mut self, bytes_dropped: usize, cause: LogReadError);
}

impl<F: FnMut(usize, LogReadError)> ErrorHandler for F {
    fn error(&mut self, bytes_dropped: usize, cause: LogReadError) {
        self(bytes_dropped, cause);
    }
}

/// The various errors that can occur while parsing physical or logical records from LevelDB's
/// log format.
///
/// None of these errors seem particularly capable of being recovered automatically. Dropping
/// data in response to encountering any `LogReadError` is reasonable.
#[allow(variant_size_differences, reason = "the error enum shouldn't be hot")]
#[derive(Error, Debug)]
pub(crate) enum LogReadError {
    /// The expected checksum from a physical record's header did not match the actual calculated
    /// checksum of the physical record.
    #[error("physical record checksum mismatch")]
    ChecksumMismatch,
    /// The length of a physical record, as given in its header, was too long to possibly be
    /// correct.
    #[error("physical record header had corrupted length field")]
    CorruptedRecordLength,
    /// The record type of a physical record, as given in its header, was not among the known
    /// values.
    ///
    /// This includes a [`WriteLogRecordType::Zero`] record whose header did not have 0 in its
    /// length field; otherwise, it is based solely on the record type byte.
    #[error("physical record header had unknown record type `{0}`")]
    UnknownRecordType(u8),
    /// A Full physical record occurred in a fragmented record, at least one of whose previous
    /// physical records were nonempty. A fragmented record should not include Full physical
    /// records.
    ///
    /// This does not result in the Full record being discarded; instead, the preceding parts
    /// of the fragmented logical record are dropped.
    ///
    /// Note that Google's `log::Writer` used to have a bug where it could emit an empty First
    /// physical record in the last seven bytes of a block and fail to realize the record was
    /// fragmented, possibly resulting in the next physical record being a Full record.
    #[error("a Full physical record occurred in a fragmented logical record")]
    FullInFragmentedRecord,
    /// A First physical record occurred in a fragmented record, at least one of whose previous
    /// physical records were nonempty. A fragmented record should only have a single First
    /// physical record at its beginning.
    ///
    /// This does not result in the First record being discarded; instead, the preceding parts
    /// of the fragmented logical record are dropped, and a new fragmented logical record is begun.
    ///
    /// Note that Google's `log::Writer` used to have a bug where it could emit an empty First
    /// physical record in the last seven bytes of a block and fail to realize the record was
    /// fragmented, possibly resulting in the next physical record being a First record.
    #[error("an extra First physical record occurred in a fragmented logical record")]
    ExtraFirstInFragmentedRecord,
    /// A Middle physical record occurred outside a fragmented record (or, a fragmented
    /// record failed to be started with a First physical record).
    ///
    /// The offending Middle record is dropped.
    ///
    /// It is possible that a previous would-be First or Middle physical record in the intended
    /// fragmented record was corrupted.
    #[error("a Middle physical record occurred outside a fragmented logical record")]
    MiddleWithoutFirst,
    /// A Last physical record occurred outside a fragmented record (or, a fragmented
    /// record failed to be started with a First physical record).
    ///
    /// The offending Last record is dropped.
    ///
    /// It is possible that a previous would-be First or Middle physical record in the intended
    /// fragmented record was corrupted.
    #[error("a Last physical record occurred outside a fragmented logical record")]
    LastWithoutFirst,
    /// Used to be produced from Google's LevelDB back when its writable file interface was
    /// sometimes implemented with mmap.
    ///
    /// Reported for clarity in debugging. It seems possible that corruption could cause
    /// portions of a log file to erroneously contain solely zeroes and thus register as a
    /// `ZeroRecord` instead of some more obvious corruption.
    ///
    /// Note that the number of bytes lost from such a failure is always reported as 0.
    #[error("a Zero physical record was encountered (not particularly an error)")]
    ZeroRecord,
    /// A call to [`Read::read`] on a file failed to read data for a reason other than an
    /// interrupt or early end-of-file.
    ///
    /// Note that the number of bytes lost from such a failure is likely underestimated;
    /// potentially, filesystem-level or disk-level corruption may have occurred, depending
    /// on what the IO error indicates.
    #[error("failed to read a file: {0}")]
    FileReadError(IoError),
    /// If some IO error or other corruption error occurred while reading a physical record, then
    /// any logical record it was part of is discarded as corrupted or incomplete.
    ///
    /// The `bytes_dropped` argument indicates how many bytes had been successfully read from
    /// previous complete physical records that are being discarded (which are in the same
    /// fragmented logical record as the damaged physical record).
    ///
    /// This error does not cover [`FullInFragmentedRecord`] or [`ExtraFirstInFragmentedRecord`],
    /// which also discard previously-successful physical records, nor does it cover
    /// [`MiddleWithoutFirst`] or [`LastWithoutFirst`] which cause otherwise-successful physical
    /// records to be dropped as they are parsed.
    ///
    /// [`FullInFragmentedRecord`]: LogReadError::FullInFragmentedRecord
    /// [`ExtraFirstInFragmentedRecord`]: LogReadError::ExtraFirstInFragmentedRecord
    /// [`MiddleWithoutFirst`]: LogReadError::MiddleWithoutFirst
    /// [`LastWithoutFirst`]: LogReadError::LastWithoutFirst
    #[error("discarding an entire fragmented logical record due to a physical record error")]
    ErrorInFragmentedRecord,
}
