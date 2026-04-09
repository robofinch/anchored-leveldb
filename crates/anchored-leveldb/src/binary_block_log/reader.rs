#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to perform Polonius-style early returns of borrows"),
)]

#[cfg(not(feature = "polonius"))]
use std::mem::transmute;
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    io::{Error as IoError, ErrorKind as IoErrorKind, Read},
};

use crate::{all_errors::types::BinaryBlockLogCorruptionError, utils::unmask_checksum};
use crate::{
    pub_traits::error_handler::{LogControlFlow, ManifestControlFlow, OpenCorruptionHandler},
    pub_typed_bytes::{BinaryLogBlockSize, FileNumber, FileOffset, FileSize, PhysicalRecordType},
};
use super::BINARY_LOG_HEADER_SIZE;


// ================================================================
//  Type that should be reused between readers
// ================================================================

/// A type storing buffers that should be reused across readers for manifest and log files.
pub(crate) struct BinaryBlockLogReaderBuffers {
    /// Buffer for physical records (and logical records whose data is in a single physical record).
    ///
    /// # Correctness
    /// The length of `block_buffer` must be a valid [`BinaryLogBlockSize`] value.
    block_buffer:  Box<[u8]>,
    /// Buffer for fragmented logical records (aside from fragmented logical records whose
    /// initial `First` entry is empty and whose second entry contains all the data).
    record_buffer: Vec<u8>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl BinaryBlockLogReaderBuffers {
    #[inline]
    #[must_use]
    pub fn new(block_size: BinaryLogBlockSize) -> Self {
        Self {
            block_buffer:  vec![0; block_size.as_usize()].into_boxed_slice(),
            record_buffer: Vec::new(),
        }
    }

    pub fn read_manifest<File: Read>(
        &mut self,
        manifest_file: File,
        file_size:     FileSize,
    ) -> Result<ManifestReader<'_, File>, IoError> {
        InnerReader::new(manifest_file, &mut self.block_buffer).map(|inner| {
            ManifestReader {
                inner,
                // Correctness: the block buffer provided to `LogReader` is the same block buffer
                // used to construct `inner`, and thus has the correct length.
                buffers: self,
                file_size,
            }
        })
    }

    pub fn read_log<File: Read>(
        &mut self,
        log_file:    File,
        file_number: FileNumber,
        file_size:   FileSize,
    ) -> Result<LogReader<'_, File>, IoError> {
        InnerReader::new(log_file, &mut self.block_buffer).map(|inner| {
            LogReader {
                inner,
                // Correctness: the block buffer provided to `LogReader` is the same block buffer
                // used to construct `inner`, and thus has the correct length.
                buffers: self,
                file_number,
                file_size,
            }
        })
    }
}

impl Debug for BinaryBlockLogReaderBuffers {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("BinaryBlockLogReaderBuffers")
            .field("block_buffer",  &format!("<{} bytes>", self.block_buffer.len()))
            .field("record_buffer", &format!("<{} bytes>", self.record_buffer.len()))
            .finish()
    }
}

// ================================================================
//  Abstraction across manifest and log file readers
// ================================================================

/// A logical record, returned from [`ManifestReader::read_record`] or [`LogReader::read_record`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct LogicalRecord<'a> {
    pub data:   &'a [u8],
    pub offset: FileOffset,
}

/// Abstract over manifest and log files. The two cases have only minor differences.
trait InnerHandler {
    type RecordResult<'a>;

    /// `None` should correspond to a `Continue` control flow, while returning `Some` results
    /// in the whole [`InnerReader::read_record`] function returning the given result.
    #[must_use]
    fn report_error<'a>(
        &mut self,
        file_offset: FileOffset,
        cause:       BinaryBlockLogCorruptionError,
        bytes_lost:  usize,
    ) -> Option<Self::RecordResult<'a>>;

    #[must_use]
    fn some_logical_record(record: LogicalRecord<'_>) -> Self::RecordResult<'_>;

    /// The entire file has been processed. (Some of it might have been corrupted records ignored
    /// by the handler, but this report of EOF is not a lie by the handler.)
    #[must_use]
    fn true_end_of_file<'a>() -> Self::RecordResult<'a>;

    #[must_use]
    fn io_error<'a>(io_err: IoError) -> Self::RecordResult<'a>;
}

// ================================================================
//  Manifest file reader
// ================================================================

#[derive(Debug)]
pub(crate) enum ManifestRecordResult<'a> {
    Some(LogicalRecord<'a>),
    EndOfFile,
    HandlerReportedError,
    ReadError(IoError),
}

struct ManifestHandler<'a, InvalidKey>(&'a mut dyn OpenCorruptionHandler<InvalidKey>, FileSize);

impl<InvalidKey> InnerHandler for ManifestHandler<'_, InvalidKey> {
    type RecordResult<'a> = ManifestRecordResult<'a>;

    fn report_error<'a>(
        &mut self,
        file_offset: FileOffset,
        cause:       BinaryBlockLogCorruptionError,
        bytes_lost:  usize,
    ) -> Option<Self::RecordResult<'a>> {
        match self.0.manifest_corruption(file_offset, cause, bytes_lost, self.1) {
            ManifestControlFlow::Continue     => None,
            ManifestControlFlow::BreakSuccess => Some(ManifestRecordResult::EndOfFile),
            ManifestControlFlow::BreakError   => Some(ManifestRecordResult::HandlerReportedError),
        }
    }

    fn some_logical_record(record: LogicalRecord<'_>) -> Self::RecordResult<'_> {
        ManifestRecordResult::Some(record)
    }

    fn true_end_of_file<'a>() -> Self::RecordResult<'a> {
        ManifestRecordResult::EndOfFile
    }

    fn io_error<'a>(io_err: IoError) -> Self::RecordResult<'a> {
        ManifestRecordResult::ReadError(io_err)
    }
}

/// A reader for the binary log format used by LevelDB to store serialized [`WriteBatch`]es, in the
/// case of write-ahead logs corresponding to memtables, or serialized [`VersionEdit`]s in the case
/// of MANIFEST files.
///
/// A `ManifestReader` is distinct from [`LogReader`]s only in how they interact with
/// an [`OpenCorruptionHandler`] and report control flow.
pub(crate) struct ManifestReader<'a, File> {
    inner:     InnerReader<File>,
    /// Separated from `inner` to avoid borrowck errors.
    ///
    /// # Correctness
    /// `self.buffers.block_buffer` must have the same length as the `block_buffer` provided
    /// to `InnerReader::new` to construct `self.inner`.
    buffers:   &'a mut BinaryBlockLogReaderBuffers,
    file_size: FileSize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File: Read> ManifestReader<'_, File> {
    /// Get the next logical record in the manifest file, if any, in addition to the file offset of
    /// the start of that record.
    ///
    /// This method does _not_ directly report any corruption errors; instead, errors are reported
    /// via the given handler.
    ///
    /// # Errors
    /// If reading the manifest file fails, an error is returned. (This is under the rationale
    /// that either the read error is transient, and the user may wish to try again, or the read
    /// error is more permanent and likely due to a serious filesystem-level error.)
    pub fn read_record<InvalidKey>(
        &mut self,
        handler: &mut dyn OpenCorruptionHandler<InvalidKey>,
    ) -> ManifestRecordResult<'_> {
        // Correctness: We provide the same `self.buffers.block_buffer` every time,
        // so it has the correct length.
        self.inner.read_record(self.buffers, ManifestHandler(handler, self.file_size))
    }
}

impl<File> Debug for ManifestReader<'_, File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ManifestReader")
            .field("inner",     &self.inner)
            .field("buffers",   &self.buffers)
            .field("file_size", &self.file_size)
            .finish()
    }
}

// ================================================================
//  Log file reader
// ================================================================

#[derive(Debug)]
pub(crate) enum LogRecordResult<'a> {
    Some(LogicalRecord<'a>),
    EndOfFile,
    ReadError(IoError),
}

struct LogHandler<'a, InvalidKey>(
    &'a mut dyn OpenCorruptionHandler<InvalidKey>,
    FileSize,
    FileNumber,
);

impl<InvalidKey> InnerHandler for LogHandler<'_, InvalidKey> {
    type RecordResult<'a> = LogRecordResult<'a>;

    fn report_error<'a>(
        &mut self,
        file_offset: FileOffset,
        cause:       BinaryBlockLogCorruptionError,
        bytes_lost:  usize,
    ) -> Option<Self::RecordResult<'a>> {
        match self.0.log_corruption(self.2, file_offset, cause, bytes_lost, self.1) {
            LogControlFlow::Continue => None,
            LogControlFlow::Break    => Some(LogRecordResult::EndOfFile),
        }
    }

    fn some_logical_record(record: LogicalRecord<'_>) -> Self::RecordResult<'_> {
        LogRecordResult::Some(record)
    }

    fn true_end_of_file<'a>() -> Self::RecordResult<'a> {
        LogRecordResult::EndOfFile
    }

    fn io_error<'a>(io_err: IoError) -> Self::RecordResult<'a> {
        LogRecordResult::ReadError(io_err)
    }
}

/// A reader for the binary log format used by LevelDB to store serialized [`WriteBatch`]es, in the
/// case of write-ahead logs corresponding to memtables, or serialized [`VersionEdit`]s in the case
/// of MANIFEST files.
///
/// A `LogReader` is distinct from [`ManifestReader`]s only in how they interact with
/// an [`OpenCorruptionHandler`] and report control flow.
pub(crate) struct LogReader<'a, File> {
    inner:       InnerReader<File>,
    /// Separated from `inner` to avoid borrowck errors.
    ///
    /// # Correctness
    /// `self.buffers.block_buffer` must have the same length as the `block_buffer` provided
    /// to `InnerReader::new` to construct `self.inner`.
    buffers:     &'a mut BinaryBlockLogReaderBuffers,
    file_size:   FileSize,
    file_number: FileNumber,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File: Read> LogReader<'_, File> {
    /// Get the next logical record in the log file, if any, in addition to the file offset of
    /// the start of that record.
    ///
    /// This method does _not_ directly report any corruption errors; instead, errors are reported
    /// via the given handler.
    ///
    /// # Errors
    /// If reading the log file fails, an error is returned. (This is under the rationale
    /// that either the read error is transient, and the user may wish to try again, or the read
    /// error is more permanent and likely due to a serious filesystem-level error.)
    pub fn read_record<InvalidKey>(
        &mut self,
        handler: &mut dyn OpenCorruptionHandler<InvalidKey>,
    ) -> LogRecordResult<'_> {
        // Correctness: We provide the same `self.buffers.block_buffer` every time,
        // so it has the correct length.
        self.inner.read_record(self.buffers, LogHandler(handler, self.file_size, self.file_number))
    }
}

impl<File> Debug for LogReader<'_, File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("LogReader")
            .field("inner",       &self.inner)
            .field("buffers",     &self.buffers)
            .field("file_size",   &self.file_size)
            .field("file_number", &self.file_number)
            .finish()
    }
}

// ================================================================
//  The actual reader implementation
// ================================================================

#[derive(Debug)]
enum PhysicalRecordResult<'a> {
    PhysicalRecord {
        /// The type of the physical record.
        ///
        /// Note that the `Zero` record type is handled specially, so if the returned record
        /// type is not one of `Full`, `First`, `Middle`, or `Last`, it should be considered to be
        /// an unknown record type.
        record_type:  u8,
        /// The physical record itself.
        data:         &'a [u8],
        /// The offset within the current block of the returned physical record.
        start_offset: u16,
    },
    EndOfFile,
    BinaryBlockLogCorruptionError {
        /// The offset within the current block of the corrupted physical record.
        start_offset: u16,
        /// The type of corruption that occurred.
        cause: BinaryBlockLogCorruptionError,
        /// An estimation of the number of bytes lost in this physical record, not including
        /// any previous blocks in a fragmented logical record.
        bytes_directly_lost: u16,
    },
    ReadError(IoError),
}

/// A reader for the log format used by LevelDB to store serialized [`WriteBatch`]es, in the case
/// of write-ahead logs corresponding to memtables, or serialized [`VersionEdit`]s in the case
/// of MANIFEST files.
struct InnerReader<File> {
    file:               File,
    /// The first `current_block_len` bytes of `self.buffers.block_buffer` contain the current
    /// block of the binary log file.
    ///
    /// This is `self.buffers.block_buffer.len()` unless EOF has been reached.
    current_block_len:  u16,
    /// The length of data in the current block which we have already processed. The next
    /// (not-yet-processed) physical record, if any, begins at this at this offset.
    offset_in_block:    u16,
    /// The number of blocks which have already been completely read, such that the file offset
    /// of the next (not-yet-processed) physical record, if any, is given by
    /// `FileOffset(self.block_index * self.buffers.block_buffer.len() + self.offset_in_block)`
    /// (ignoring the specifics of integer types).
    ///
    /// This value is only used for reporting errors, so we choose to saturate the file offset
    /// to `u64::MAX` (if, somehow, an 18 exabyte file is encountered someday) rather than
    /// panicking or wrapping on overflow.
    block_index:        u64,
}

impl<File: Read> InnerReader<File> {
    /// Initialize the block buffer to the first block in the file.
    ///
    /// # Correctness
    /// The length of `block_buffer` must be a valid [`BinaryLogBlockSize`] value. The same length
    /// of buffer must be used whenever reading the log.
    fn new(
        file:         File,
        block_buffer: &mut [u8],
    ) -> Result<Self, IoError> {
        // Note that we don't need to reset the `record_buffer`, since it's `clear`ed when
        // `First` records are read.
        let mut this = Self {
            file,
            current_block_len: 0,
            offset_in_block:   0,
            block_index:       0,
        };

        let block_buffer_len = block_buffer.len();
        let mut unfilled = block_buffer;

        while !unfilled.is_empty() {
            match this.file.read(unfilled) {
                Ok(0) => break,
                Ok(n) => {
                    // Yes, a bad Read implementation *could* cause a panic here. But we aren't
                    // doing anything `unsafe` here, so a panic due to someone else's buggy
                    // `Read` is fine.
                    #[expect(
                        clippy::indexing_slicing,
                        reason = "Return value of `Read::read` should be <= `unfilled.len()`",
                    )]
                    {
                        unfilled = &mut unfilled[n..];
                    }
                    // Note that we continue to the next iteration
                }
                Err(io_err) => {
                    // Ignore interrupts
                    if matches!(io_err.kind(), IoErrorKind::Interrupted) {
                        continue;
                    }

                    return Err(io_err);
                }
            }
        }

        // Ignore the last `unfilled.len()` bytes of the block buffer, which could not be
        // read due to EOF. (There might be zero such elements.)
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "`unfilled.len() <= block_buffer.len() < u16::MAX`",
        )]
        {
            this.current_block_len = (block_buffer_len - unfilled.len()) as u16;
        };

        Ok(this)
    }

    /// # Correctness
    /// `block_size` must be the length of the `buffers.block_buffer` provided to `Self::new` to
    /// construct this `self` value.
    #[must_use]
    fn file_offset(
        &self,
        block_size:              u64,
        offset_in_current_block: u16,
    ) -> FileOffset {
        FileOffset(
            self.block_index
                .saturating_mul(block_size)
                .saturating_add(u64::from(offset_in_current_block))
        )
    }

    /// Get the next logical record in the binary log file, if any, in addition to the file offset
    /// of the start of that record.
    ///
    /// This method does _not_ directly report any corruption errors; instead, errors are reported
    /// via the given handler.
    ///
    /// # Correctness
    /// `buffers.block_buffer` must have the same length used in `Self::new` to construct
    /// this `self` value.
    ///
    /// # Errors
    /// If reading the binary log file fails, an error is returned. (This is under the rationale
    /// that either the read error is transient, and the user may wish to try again, or the read
    /// error is more permanent and likely due to a serious filesystem-level error.)
    #[expect(clippy::too_many_lines, reason = "massive match, with a helper macro to mitigate it")]
    fn read_record<'a, H: InnerHandler>(
        &mut self,
        buffers:     &'a mut BinaryBlockLogReaderBuffers,
        mut handler: H,
    ) -> H::RecordResult<'a> {
        // The file offset of the start of a fragmented logical record (if `Some`).
        // Note that setting `fragmented = None` discards the contents of `record_buffer`,
        // since `record_buffer` is only used for fragmented records, which must start with a
        // `First` record, and the branch for `First` does make sure to clear `record_buffer`.
        let mut fragmented = None;

        #[expect(
            clippy::as_conversions,
            reason = "should not truncate, since the caller is told \
                      that it should be a valid `BinaryLogBlockSize`",
        )]
        let block_size: u64 = buffers.block_buffer.len() as u64;

        macro_rules! file_offset {
            ($offset_in_block:expr) => {
                self.file_offset(block_size, $offset_in_block)
            };
        }

        // `bytes_lost_directly` is the bytes which are lost *other* than bytes in
        // previously-successfully-read physical records of a fragmented logical record.
        macro_rules! report_error {
            ($file_offset:expr, $cause:expr, $bytes_lost_directly:expr $(,)?) => {
                {
                    let mut bytes_lost = usize::from($bytes_lost_directly);
                    if fragmented.is_some() {
                        bytes_lost += buffers.record_buffer.len();
                    }

                    if let Some(record_result) = handler.report_error(
                        $file_offset,
                        $cause,
                        bytes_lost,
                    ) {
                        return record_result;
                    }

                    // See above; this discards the contents of `record_buffer`.
                    fragmented = None;
                    continue;
                }
            };
        }

        #[allow(
            clippy::needless_continue,
            reason = "triggered by `report_error` in some but not all usages",
        )]
        loop {
            match self.read_physical_record(&mut buffers.block_buffer) {
                PhysicalRecordResult::PhysicalRecord {
                    record_type,
                    data,
                    start_offset,
                } => match PhysicalRecordType::try_from(record_type) {
                    Ok(PhysicalRecordType::Full) => {
                        if let Some(fragmented_offset) = fragmented {
                            if buffers.record_buffer.is_empty() {
                                // See `FullInFragmentedRecord` for details,
                                // but we must ignore this for backwards compatibility.
                            } else {
                                report_error!(
                                    fragmented_offset,
                                    BinaryBlockLogCorruptionError::FullInFragmentedRecord,
                                    0_usize,
                                );
                            }
                        }
                        // We discard the contents of `self.record_buffer` when we return,
                        // since `fragmented` would be set to `false` when `read_record`
                        // is next called.

                        // SAFETY: We're doing a Polonius-style conditional early return of
                        // a borrow. The code compiles under the Polonius borrow checker, so
                        // this is sound.
                        #[cfg(not(feature = "polonius"))]
                        let data: &[u8] = unsafe {
                            transmute::<&'_ [u8], &'_ [u8]>(data)
                        };

                        break H::some_logical_record(LogicalRecord {
                            data,
                            offset: file_offset!(start_offset),
                        });
                    }
                    Ok(PhysicalRecordType::First) => {
                        if let Some(fragmented_offset) = fragmented {
                            if buffers.record_buffer.is_empty() {
                                // See `ExtraFirstInFragmentedRecord` for details,
                                // but we must ignore this for backwards compatibility.
                            } else {
                                report_error!(
                                    fragmented_offset,
                                    BinaryBlockLogCorruptionError::ExtraFirstInFragmentedRecord,
                                    0_usize,
                                );
                            }
                        }

                        fragmented = Some(file_offset!(start_offset));
                        buffers.record_buffer.clear();
                        // `data` has a length of at most 32 KiB, so this should not fail.
                        buffers.record_buffer.extend(data);
                        // Continue iteration, read rest of fragmented record.
                    }
                    Ok(PhysicalRecordType::Middle) => {
                        if fragmented.is_some() {
                            // TODO: handle alloc error.
                            buffers.record_buffer.extend(data);
                        } else {
                            report_error!(
                                file_offset!(start_offset),
                                BinaryBlockLogCorruptionError::MiddleWithoutFirst,
                                data.len(),
                            );
                        }
                    }
                    Ok(PhysicalRecordType::Last) => {
                        if let Some(fragmented_offset) = fragmented {
                            // We discard the contents of `self.record_buffer` when we return,
                            // since `fragmented` would be set to `false` when `read_record`
                            // is next called.

                            if buffers.record_buffer.is_empty() {
                                // We're lucky, no need to append to `self.record_buffer`.

                                // SAFETY: We're doing a Polonius-style conditional early return of
                                // a borrow. The code compiles under the Polonius borrow checker, so
                                // this is sound.
                                #[cfg(not(feature = "polonius"))]
                                let data: &[u8] = unsafe {
                                    transmute::<&'_ [u8], &'_ [u8]>(data)
                                };

                                return H::some_logical_record(LogicalRecord {
                                    data,
                                    offset: fragmented_offset,
                                });
                            } else {
                                // TODO: handle alloc error.
                                buffers.record_buffer.extend(data);

                                let frag_rec: &[u8] = buffers.record_buffer.as_slice();

                                return H::some_logical_record(LogicalRecord {
                                    data:   frag_rec,
                                    offset: fragmented_offset,
                                });
                            }
                        } else {
                            report_error!(
                                file_offset!(start_offset),
                                BinaryBlockLogCorruptionError::LastWithoutFirst,
                                data.len(),
                            );
                        }
                    }
                    // Note that the `Zero` record type is handled specially;
                    // see `PhysicalRecordResult::PhysicalRecord`.
                    Ok(PhysicalRecordType::Zero) | Err(()) => {
                        report_error!(
                            file_offset!(start_offset),
                            BinaryBlockLogCorruptionError::UnknownRecordType(record_type),
                            data.len(),
                        );
                    }
                }
                PhysicalRecordResult::EndOfFile => {
                    if let Some(fragmented_offset) = fragmented {
                        // Note that `report_error` might internally clear `fragmented` and
                        // then `continue`.
                        // Whatever, the next call to `self.inner.read_physical_record()`
                        // will return `PhysicalRecordResult::EndOfFile` again, and we'll return
                        // in the below branch.
                        report_error!(
                            fragmented_offset,
                            BinaryBlockLogCorruptionError::TruncatedLogicalRecord,
                            0_usize,
                        );
                    } else {
                        return H::true_end_of_file();
                    };
                }
                PhysicalRecordResult::BinaryBlockLogCorruptionError {
                    start_offset,
                    cause,
                    bytes_directly_lost,
                } => {
                    report_error!(file_offset!(start_offset), cause, bytes_directly_lost);
                }
                PhysicalRecordResult::ReadError(io_err) => {
                    return H::io_error(io_err);
                }
            }
        }
    }

    /// This function performs a small amount of work at the start (advancing to the next block
    /// if necessary) and at the end (advance offsets to the next physical record within the
    /// current block or file, return the current physical record as a slice).
    ///
    /// Most of the function is error checking.
    ///
    /// # Correctness
    /// `block_buffer` must have the same length used in `Self::new` to construct this `self` value.
    fn read_physical_record<'a>(
        &mut self,
        block_buffer: &'a mut [u8],
    ) -> PhysicalRecordResult<'a> {
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "should not truncate, since any `BinaryLogBlockSize` is at most `u16::MAX`",
        )]
        let block_buffer_len_u16 = block_buffer.len() as u16;

        // Note that `self.offset_in_block <= block_buffer.len() == 1 << 15`, so this addition
        // can't overflow a u16.
        if self.offset_in_block + BINARY_LOG_HEADER_SIZE > self.current_block_len {
            // Skip any remaining trailer (or incomplete header) bytes in this block,
            // and read the next block (if there is one).
            if let Err(read_err) = self.fill_block_until_eof(block_buffer) {
                return PhysicalRecordResult::ReadError(read_err);
            }

            // If we still haven't read enough data, then clearly we're at EOF and have processed
            // every physical record in the file.
            // If there's not enough space left in this last block for a header, then we've either
            // - processed everything in the block, so the last physical record is complete.
            // - processed everything except for trailer bytes (which would be in the last 0-6
            //   bytes of a `block_buffer.len()`-sized block, though some of the trailer bytes
            //   might not have been written to the file), so the last physical record is complete.
            // - processed everything except for the beginnings of another physical record,
            //   which is thus a truncated physical record.
            if self.offset_in_block == self.current_block_len
                || usize::from(self.offset_in_block + BINARY_LOG_HEADER_SIZE) > block_buffer.len()
            {
                return PhysicalRecordResult::EndOfFile;
            } else if self.offset_in_block + BINARY_LOG_HEADER_SIZE > self.current_block_len {
                // Handling the returned error amounts to processing everything left in this block.
                let start_offset = self.offset_in_block;
                self.offset_in_block = self.current_block_len;

                return PhysicalRecordResult::BinaryBlockLogCorruptionError {
                    start_offset,
                    cause:               BinaryBlockLogCorruptionError::TruncatedHeader,
                    bytes_directly_lost: 0,
                };
            } else {
                // We might have read up to EOF, but there's still enough data that we can
                // try to read more physical records.
            }
        }

        // We can easily see that if we get here, then
        // `self.offset_in_block + BINARY_LOG_HEADER_SIZE <= block_buffer.len()`.
        let (unprocessed, masked_checksum, length, record_type) = {{
            #![expect(clippy::indexing_slicing, reason = "we checked the lengths")]
            #![expect(clippy::unwrap_used, reason = "valid slice -> array conversion")]

            let unprocessed = &block_buffer[
                usize::from(self.offset_in_block)..usize::from(self.current_block_len)
            ];
            // Note that `unprocessed.len() >= BINARY_LOG_HEADER_SIZE == 7 > 6`.
            assert!(unprocessed.len() > 6, "would have returned EOF otherwise");

            let masked_checksum: [u8; 4] = unprocessed[0..4].try_into().unwrap();
            let length: [u8; 2] = unprocessed[4..6].try_into().unwrap();
            let record_type: u8 = unprocessed[6];

            (
                unprocessed,
                u32::from_le_bytes(masked_checksum),
                u16::from_le_bytes(length),
                record_type,
            )
        }};

        // Note that if `alleged_length` saturates to `u16::MAX`, then since
        // `max_reasonable_length < block_buffer.len() < u16::MAX`, the first error
        // branch is hit, and the reported error does not depend on the value of `alleged_length`,
        // so it is unimportant that the exactly correct value is not computed.
        let alleged_length = BINARY_LOG_HEADER_SIZE.saturating_add(length);
        let max_reasonable_length = block_buffer_len_u16 - self.offset_in_block;
        // Note that `len_to_end_of_block == self.current_block_len - self.offset_in_block`,
        // NOT `block_buffer.len() - self.offset_in_block`.
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "`unprocessed.len() <= block_buffer.len() < u16::MAX`",
        )]
        let len_to_end_of_block = unprocessed.len() as u16;

        if alleged_length > max_reasonable_length {
            // Oh well, we don't know exactly where the next entry should actually begin;
            // skip to the next block.
            // Handling the returned error amounts to processing everything left in this block.
            let start_offset = self.offset_in_block;
            self.offset_in_block = self.current_block_len;

            return PhysicalRecordResult::BinaryBlockLogCorruptionError {
                start_offset,
                cause:               BinaryBlockLogCorruptionError::CorruptedRecordLength,
                bytes_directly_lost: len_to_end_of_block,
            };
        }

        if alleged_length > len_to_end_of_block {
            // If we get here, then `max_reasonable_length >= alleged_length > len_to_end_of_block`,
            // and thus `block_buffer_len_u16 - self.offset_in_block
            //           > self.current_block_len - self.offset_in_block`,
            // so `block_buffer_len_u16 > self.current_block_len`, implying that we've reached EOF.
            // Handling the returned error amounts to processing everything left in this block.
            let start_offset = self.offset_in_block;
            self.offset_in_block = self.current_block_len;

            return PhysicalRecordResult::BinaryBlockLogCorruptionError {
                start_offset,
                cause:               BinaryBlockLogCorruptionError::TruncatedPhysicalRecord,
                bytes_directly_lost: len_to_end_of_block,
            };
        }

        // Note that `BINARY_LOG_HEADER_SIZE + length` does not overflow, else we would have
        // `BINARY_LOG_HEADER_SIZE.saturating_add(length) == u16::MAX`
        // which is certainly strictly greater than `max_reasonable_length` above.
        let length_with_header = alleged_length;

        if length == 0 && record_type == u8::from(PhysicalRecordType::Zero) {
            // This is, unfortunately, another case where it seems the best option is to
            // skip to the next block. See the documentation for `ZeroRecord` for more.
            // Handling the returned error amounts to processing everything left in this block.
            let start_offset = self.offset_in_block;
            self.offset_in_block = self.current_block_len;

            return PhysicalRecordResult::BinaryBlockLogCorruptionError {
                start_offset,
                cause:               BinaryBlockLogCorruptionError::ZeroRecord,
                bytes_directly_lost: 0,
            };
        }

        // Note that `6..` excludes the checksum and length from the data being checksummed;
        // precisely the physical record type and data are checksummed.
        #[expect(clippy::indexing_slicing, reason = "`len >= 7 == BINARY_LOG_HEADER_SIZE`")]
        let actual_crc = crc32c::crc32c(&unprocessed[6..usize::from(length_with_header)]);
        let expected_crc = unmask_checksum(masked_checksum);
        if actual_crc != expected_crc {
            // Unfortunately... yet again we must skip to the next block. For all we know,
            // the `length` field was corrupted, too.
            // Handling the returned error amounts to processing everything left in this block.
            let start_offset = self.offset_in_block;
            self.offset_in_block = self.current_block_len;
            return PhysicalRecordResult::BinaryBlockLogCorruptionError {
                start_offset,
                cause:               BinaryBlockLogCorruptionError::ChecksumMismatch,
                bytes_directly_lost: len_to_end_of_block,
            };
        }

        let start_offset = self.offset_in_block;
        self.offset_in_block += length_with_header;

        // Note that `length_with_header <= len_to_end_of_block == next_physical_record.len()`.
        #[expect(clippy::indexing_slicing, reason = "see above; `len >= BINARY_LOG_HEADER_SIZE`")]
        PhysicalRecordResult::PhysicalRecord {
            record_type,
            data: &unprocessed[
                usize::from(BINARY_LOG_HEADER_SIZE)..usize::from(length_with_header)
            ],
            start_offset,
        }
    }

    /// Fill `block_buffer` until either its whole length is filled, EOF is reached, or some
    /// non-interrupt IO error occurs.
    ///
    /// Unless EOF was previously reached, `self.current_block_len` is resized to the number of
    /// bytes read, `self.block_index` is incremented, and `self.offset_in_block` is reset to zero.
    /// If EOF was previously reached, then `self` is not mutated. Note that
    /// `self.current_block_len != block_buffer.len()` if and only if EOF has occurred.
    ///
    /// If a non-interrupt IO error occurs, `self.current_block_len` is set to however much
    /// data was successfully read (though, in practice, we never read that value again in that
    /// circumstance.)
    ///
    /// # Correctness
    /// `block_buffer` must have the same length used in `Self::new` to construct this `self` value.
    fn fill_block_until_eof(
        &mut self,
        block_buffer: &mut [u8],
    ) -> Result<(), IoError> {
        if usize::from(self.current_block_len) != block_buffer.len() {
            return Ok(());
        }

        self.offset_in_block = 0;
        self.block_index = self.block_index.saturating_add(1);

        let block_buffer_len = block_buffer.len();
        let mut unfilled = block_buffer;

        while !unfilled.is_empty() {
            match self.file.read(unfilled) {
                Ok(0) => break,
                Ok(n) => {
                    // Yes, a bad Read implementation *could* cause a panic here. But we aren't
                    // doing anything `unsafe` here, so a panic due to someone else's buggy
                    // `Read` is fine.
                    #[expect(
                        clippy::indexing_slicing,
                        reason = "Return value of `Read::read` should be <= `unfilled.len()`",
                    )]
                    {
                        unfilled = &mut unfilled[n..];
                    }
                    // Note that we continue to the next iteration
                }
                Err(io_err) => {
                    // Ignore interrupts
                    if matches!(io_err.kind(), IoErrorKind::Interrupted) {
                        continue;
                    }

                    #[expect(
                        clippy::as_conversions,
                        clippy::cast_possible_truncation,
                        reason = "`unfilled.len() <= block_buffer.len() < u16::MAX`",
                    )]
                    {
                        self.current_block_len = (block_buffer_len - unfilled.len()) as u16;
                    };

                    return Err(io_err);
                }
            }
        }

        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "`unfilled.len() <= block_buffer.len() < u16::MAX`",
        )]
        {
            // Ignore the last `buf.len()` elements of the block buffer, which could not be read
            // due to EOF. (There may be zero such elements.)
            self.current_block_len = (block_buffer_len - unfilled.len()) as u16;
        };

        Ok(())
    }
}

impl<File> Debug for InnerReader<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InnerReader")
            .field("file",              &"<File>")
            .field("current_block_len", &self.current_block_len)
            .field("offset_in_block",   &self.offset_in_block)
            .field("block_index",       &self.block_index)
            .finish()
    }
}
