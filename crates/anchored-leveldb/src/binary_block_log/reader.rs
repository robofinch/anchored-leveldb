#![expect(
    unsafe_code,
    reason = "needed to perform Polonius-style lifetime extension, \
              and construct a zeroed boxed array buffer directly on the heap",
)]

#[cfg(not(feature = "polonius"))]
use std::mem::transmute;
use std::mem::MaybeUninit;
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    io::{ErrorKind, Read},
};

use crate::utils::unmask_checksum;
use crate::{
    all_errors::types::{BinaryBlockLogCorruptionError, BinaryBlockLogReadError},
    pub_traits::error_handler::{LogControlFlow, ManifestControlFlow, OpenCorruptionHandler},
    pub_typed_bytes::{FileNumber, FileOffset, FileSize, PhysicalRecordType},
};
use super::{HEADER_SIZE, WRITE_LOG_BLOCK_SIZE, WRITE_LOG_BLOCK_SIZE_U16};


// ================================================================
//  Type that should be reused between readers
// ================================================================

/// A type storing buffers that should be reused across readers for manifest and log files.
pub(crate) struct BinaryBlockLogReaderBuffers {
    /// Buffer for physical records (and logical records whose data is in a single physical record).
    block_buffer: Box<[u8; WRITE_LOG_BLOCK_SIZE]>,
    /// Buffer for fragmented logical records (aside from fragmented logical records whose
    /// initial `First` entry is empty and whose second entry contains all the data).
    record_buffer: Vec<u8>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl BinaryBlockLogReaderBuffers {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        // TODO: Use `Box::new_zeroed`, after I make one release of anchored-leveldb with MSRV 1.85.
        //
        /// Constructs a new `Box` with uninitialized contents, with the memory being filled
        /// with `0` bytes.
        ///
        /// See [`Box::new_zeroed`].
        #[expect(clippy::unnecessary_box_returns, reason = "false positive")]
        #[inline]
        #[must_use]
        fn new_zeroed_polyfill<T>() -> Box<MaybeUninit<T>> {
            use std::ptr::NonNull;
            use std::alloc::{alloc_zeroed, handle_alloc_error, Layout};

            let ptr: NonNull<MaybeUninit<T>> = if size_of::<MaybeUninit<T>>() == 0 {
                NonNull::dangling()
            } else {
                let layout = Layout::new::<MaybeUninit<T>>();
                // SAFETY: `layout.size() == size_of::<MaybeUninit<T>>() != 0`. The sole safety
                // requirement is that the layout have a nonzero size.
                let ptr = unsafe { alloc_zeroed(layout).cast() };

                let Some(ptr) = NonNull::new(ptr) else {
                    handle_alloc_error(layout);
                };

                ptr
            };

            // SAFETY: The pointee was allocated with the global allocator with the layout
            // of `MaybeUninit<T>`, trivially points to a valid value of type `MaybeUninit<T>`,
            // and isn't accessed in some other way that could cause a double-free or something.
            unsafe { Box::from_raw(ptr.as_ptr()) }
        }

        // SAFETY: The all-zeroes byte pattern is a properly initialized value of type
        // `[u8; N]` for any `N`. Also, see the implementation of `Box::default()`, and
        // https://github.com/rust-lang/rust/issues/136043; technically, we *could*
        // use `Box::default()` here to avoid `unsafe`, but this ensures that we don't use a bunch
        // of stack space *and* might avoid a `memset` by giving a hint to the allocator.
        let block_buffer = unsafe {
            new_zeroed_polyfill().assume_init()
        };
        // let block_buffer = unsafe {
        //     Box::<[u8; WRITE_LOG_BLOCK_SIZE]>::new_zeroed().assume_init()
        // };

        Self {
            block_buffer,
            record_buffer: Vec::new(),
        }
    }

    pub fn read_manifest<File: Read>(
        &mut self,
        manifest_file: File,
        file_size:     FileSize,
    ) -> Result<ManifestReader<'_, File>, BinaryBlockLogReadError> {
        InnerReader::new(manifest_file, &mut self.block_buffer).map(|inner| {
            ManifestReader {
                inner,
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
    ) -> Result<LogReader<'_, File>, BinaryBlockLogReadError> {
        InnerReader::new(log_file, &mut self.block_buffer).map(|inner| {
            LogReader {
                inner,
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
    fn read_error<'a>(read_err: BinaryBlockLogReadError) -> Self::RecordResult<'a>;
}

// ================================================================
//  Manifest file reader
// ================================================================

#[derive(Debug)]
pub(crate) enum ManifestRecordResult<'a> {
    Some(LogicalRecord<'a>),
    EndOfFile,
    HandlerReportedError,
    ReadError(BinaryBlockLogReadError),
}

struct ManifestHandler<'a>(&'a mut dyn OpenCorruptionHandler, FileSize);

impl InnerHandler for ManifestHandler<'_> {
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

    fn read_error<'a>(read_err: BinaryBlockLogReadError) -> Self::RecordResult<'a> {
        ManifestRecordResult::ReadError(read_err)
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
    pub fn read_record(
        &mut self,
        handler: &mut dyn OpenCorruptionHandler,
    ) -> ManifestRecordResult<'_> {
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
    EndOfFile {
        /// Distinguish a normal EOF or [`LogControlFlow::ContinueOtherLogs`] from
        /// [`LogControlFlow::BreakSuccess`].
        continue_reading_logs: bool,
    },
    HandlerReportedError,
    ReadError(BinaryBlockLogReadError),
}

struct LogHandler<'a>(&'a mut dyn OpenCorruptionHandler, FileSize, FileNumber);

impl InnerHandler for LogHandler<'_> {
    type RecordResult<'a> = LogRecordResult<'a>;

    fn report_error<'a>(
        &mut self,
        file_offset: FileOffset,
        cause:       BinaryBlockLogCorruptionError,
        bytes_lost:  usize,
    ) -> Option<Self::RecordResult<'a>> {
        match self.0.log_corruption(self.2, file_offset, cause, bytes_lost, self.1) {
            LogControlFlow::Continue          => None,
            LogControlFlow::ContinueOtherLogs => Some(LogRecordResult::EndOfFile {
                continue_reading_logs: true,
            }),
            LogControlFlow::BreakSuccess      => Some(LogRecordResult::EndOfFile {
                continue_reading_logs: false,
            }),
            LogControlFlow::BreakError        => Some(LogRecordResult::HandlerReportedError),
        }
    }

    fn some_logical_record(record: LogicalRecord<'_>) -> Self::RecordResult<'_> {
        LogRecordResult::Some(record)
    }

    fn true_end_of_file<'a>() -> Self::RecordResult<'a> {
        LogRecordResult::EndOfFile {
            continue_reading_logs: true,
        }
    }

    fn read_error<'a>(read_err: BinaryBlockLogReadError) -> Self::RecordResult<'a> {
        LogRecordResult::ReadError(read_err)
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
    pub fn read_record(
        &mut self,
        handler: &mut dyn OpenCorruptionHandler,
    ) -> LogRecordResult<'_> {
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
    ReadError(BinaryBlockLogReadError),
}

/// A reader for the log format used by LevelDB to store serialized [`WriteBatch`]es, in the case
/// of write-ahead logs corresponding to memtables, or serialized [`VersionEdit`]s in the case
/// of MANIFEST files.
struct InnerReader<File> {
    file:               File,
    /// The first `current_block_len` bytes of `self.buffers.block_buffer` contain the current
    /// block of the binary log file.
    ///
    /// This is [`WRITE_LOG_BLOCK_SIZE`] unless EOF has been reached.
    current_block_len:  u16,
    /// The length of data in the current block which we have already processed. The next
    /// (not-yet-processed) physical record, if any, begins at this at this offset.
    offset_in_block:    u16,
    /// The number of blocks which have already been completely read, such that the file offset
    /// of the next (not-yet-processed) physical record, if any, is given by
    /// `FileOffset(self.block_index * WRITE_LOG_BLOCK_SIZE + self.offset_in_block)` (ignoring
    /// integer types).
    ///
    /// This value is only used for reporting errors, so we choose to saturate the file offset
    /// to `u64::MAX` (if, somehow, an 18 exabyte file is encountered someday) rather than
    /// panicking or wrapping on overflow.
    block_index:        u64,
}

impl<File: Read> InnerReader<File> {
    /// Initialize the block buffer to the first block in the file.
    fn new(
        file:         File,
        block_buffer: &mut [u8; WRITE_LOG_BLOCK_SIZE],
    ) -> Result<Self, BinaryBlockLogReadError> {
        // Note that we don't need to reset the `record_buffer`, since it's `clear`ed when
        // `First` records are read.
        let mut this = Self {
            file,
            current_block_len: 0,
            offset_in_block:   0,
            block_index:       0,
        };

        let mut buf = block_buffer.as_mut_slice();

        while !buf.is_empty() {
            match this.file.read(buf) {
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

                    #[expect(
                        clippy::as_conversions,
                        reason = "`buf.len() <= WRITE_LOG_BLOCK_SIZE < u16::MAX < u64::MAX`",
                    )]
                    {
                        let len_already_read = (WRITE_LOG_BLOCK_SIZE - buf.len()) as u64;

                        return Err(BinaryBlockLogReadError {
                            error:  io_err,
                            offset: FileOffset(len_already_read),
                        });
                    }
                }
            }
        }

        // Ignore the last `buf.len()` bytes of the block buffer, which could not be
        // read due to EOF. (There might be zero such elements.)
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "`buf.len() <= WRITE_LOG_BLOCK_SIZE < u16::MAX`",
        )]
        {
            this.current_block_len = (WRITE_LOG_BLOCK_SIZE - buf.len()) as u16;
        };

        Ok(this)
    }

    #[must_use]
    fn file_offset(&self, offset_in_current_block: u16) -> FileOffset {
        FileOffset(
            self.block_index
                .saturating_mul(u64::from(WRITE_LOG_BLOCK_SIZE_U16))
                .saturating_add(u64::from(offset_in_current_block))
        )
    }

    /// Get the next logical record in the binary log file, if any, in addition to the file offset
    /// of the start of that record.
    ///
    /// This method does _not_ directly report any corruption errors; instead, errors are reported
    /// via the given handler.
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
                            offset: self.file_offset(start_offset),
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

                        fragmented = Some(self.file_offset(start_offset));
                        buffers.record_buffer.clear();
                        buffers.record_buffer.extend(data);
                        // Continue iteration, read rest of fragmented record.
                    }
                    Ok(PhysicalRecordType::Middle) => {
                        if fragmented.is_some() {
                           buffers.record_buffer.extend(data);
                        } else {
                            report_error!(
                                self.file_offset(start_offset),
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
                                buffers.record_buffer.extend(data);

                                let frag_rec: &[u8] = buffers.record_buffer.as_slice();

                                return H::some_logical_record(LogicalRecord {
                                    data:   frag_rec,
                                    offset: fragmented_offset,
                                });
                            }
                        } else {
                            report_error!(
                                self.file_offset(start_offset),
                                BinaryBlockLogCorruptionError::LastWithoutFirst,
                                data.len(),
                            );
                        }
                    }
                    // Note that the `Zero` record type is handled specially;
                    // see `PhysicalRecordResult::PhysicalRecord`.
                    Ok(PhysicalRecordType::Zero) | Err(()) => {
                        report_error!(
                            self.file_offset(start_offset),
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
                            BinaryBlockLogCorruptionError::IncompleteLogicalRecord,
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
                    report_error!(self.file_offset(start_offset), cause, bytes_directly_lost);
                }
                PhysicalRecordResult::ReadError(read_err) => {
                    return H::read_error(read_err);
                }
            }
        }
    }

    /// This function performs a small amount of work at the start (advancing to the next block
    /// if necessary) and at the end (advance offsets to the next physical record within the
    /// current block or file, return the current physical record as a slice).
    ///
    /// Most of the function is error checking.
    fn read_physical_record<'a>(
        &mut self,
        block_buffer: &'a mut [u8; WRITE_LOG_BLOCK_SIZE],
    ) -> PhysicalRecordResult<'a> {
        // Note that `self.offset_in_block <= WRITE_LOG_BLOCK_SIZE == 1 << 15`, so this addition
        // can't overflow a u16.
        if self.offset_in_block + HEADER_SIZE > self.current_block_len {
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
            //   bytes of a `WRITE_LOG_BLOCK_SIZE`-sized block, though some of the trailer bytes
            //   might not have been written to the file), so the last physical record is complete.
            // - processed everything except for the beginnings of another physical record,
            //   which is thus a truncated physical record.
            if self.offset_in_block == self.current_block_len
                || usize::from(self.offset_in_block + HEADER_SIZE) > WRITE_LOG_BLOCK_SIZE
            {
                return PhysicalRecordResult::EndOfFile;
            } else if self.offset_in_block + HEADER_SIZE > self.current_block_len {
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
        // `self.offset_in_block + HEADER_SIZE <= block_buffer.len()`.
        let (unprocessed, masked_checksum, length, record_type) = {{
            #![expect(clippy::indexing_slicing, reason = "we checked the lengths")]
            #![expect(clippy::unwrap_used, reason = "valid slice -> array conversion")]

            let unprocessed
                = &block_buffer[usize::from(self.offset_in_block)..];
            // Note that `unprocessed.len() >= HEADER_SIZE == 7 > 6`.
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
        // `max_reasonable_length < WRITE_LOG_BLOCK_SIZE < u16::MAX`, the first error
        // branch is hit, and the reported error does not depend on the value of `alleged_length`,
        // so it is unimportant that the exactly correct value is not computed.
        let alleged_length = HEADER_SIZE.saturating_add(length);
        let max_reasonable_length = WRITE_LOG_BLOCK_SIZE_U16 - self.offset_in_block;
        // Note that `len_to_end_of_block == block_buffer.len() - self.offset_in_block`.
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "`unprocessed.len() <= WRITE_LOG_BLOCK_SIZE < u16::MAX`",
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
            // If we get here, then `max_reasonable_length >= alleged_length > len_to_end_of_block`.
            // Note that this implies `block_buffer.len() < WRITE_LOG_BLOCK_SIZE`,
            // so we've reached EOF.
            // Handling the returned error amounts to processing everything left in this block.
            let start_offset = self.offset_in_block;
            self.offset_in_block = self.current_block_len;

            return PhysicalRecordResult::BinaryBlockLogCorruptionError {
                start_offset,
                cause:               BinaryBlockLogCorruptionError::TruncatedPhysicalRecord,
                bytes_directly_lost: len_to_end_of_block,
            };
        }

        // Note that `HEADER_SIZE + length` does not overflow, else we would have
        // `HEADER_SIZE.saturating_add(length) == u16::MAX`
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
        #[expect(clippy::indexing_slicing, reason = "see above; len is >= 7 == HEADER_SIZE")]
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
        #[expect(clippy::indexing_slicing, reason = "see above, and len is >= HEADER_SIZE")]
        PhysicalRecordResult::PhysicalRecord {
            record_type,
            data: &unprocessed[usize::from(HEADER_SIZE)..usize::from(length_with_header)],
            start_offset,
        }
    }

    /// Fill `block_buffer` until either its whole length is filled, EOF is reached, or some
    /// non-interrupt IO error occurs.
    ///
    /// Unless EOF was previously reached, `self.current_block_len` is resized to the number of
    /// bytes read, `self.block_index` is incremented, and `self.offset_in_block` is reset to zero.
    /// If EOF was previously reached, then `self` is not mutated. Note that
    /// `self.current_block_len != WRITE_LOG_BLOCK_SIZE` if and only if EOF has occurred.
    ///
    /// If a non-interrupt IO error occurs, `self.current_block_len` is set to however much
    /// data was successfully read (though, in practice, we never read that value again in that
    /// circumstance.)
    fn fill_block_until_eof(
        &mut self,
        block_buffer: &mut [u8; WRITE_LOG_BLOCK_SIZE],
    ) -> Result<(), BinaryBlockLogReadError> {
        if usize::from(self.current_block_len) != WRITE_LOG_BLOCK_SIZE {
            return Ok(());
        }

        self.offset_in_block = 0;
        self.block_index = self.block_index.saturating_add(1);

        let mut buf = block_buffer.as_mut_slice();

        while !buf.is_empty() {
            match self.file.read(buf) {
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

                    #[expect(
                        clippy::as_conversions,
                        clippy::cast_possible_truncation,
                        reason = "`buf.len() <= WRITE_LOG_BLOCK_SIZE < u16::MAX`",
                    )]
                    {
                        self.current_block_len = (WRITE_LOG_BLOCK_SIZE - buf.len()) as u16;
                    };

                    return Err(BinaryBlockLogReadError {
                        error:  io_err,
                        offset: self.file_offset(self.current_block_len),
                    });
                }
            }
        }

        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "`buf.len() <= WRITE_LOG_BLOCK_SIZE < u16::MAX`",
        )]
        {
            // Ignore the last `buf.len()` elements of the block buffer, which could not be read
            // due to EOF. (There may be zero such elements.)
            self.current_block_len = (WRITE_LOG_BLOCK_SIZE - buf.len()) as u16;
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
