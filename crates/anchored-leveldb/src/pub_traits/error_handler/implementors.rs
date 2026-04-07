use crate::{
    all_errors::types::{
        BinaryBlockLogCorruptionError, FinishError, HandlerError,
        VersionEditDecodeError, WriteBatchDecodeError,
    },
    pub_typed_bytes::{FileNumber, FileOffset, FileSize, LogicalRecordOffset},
};
use super::handler_trait::{
    FinishedAllLogs,
    FinishedLog,
    FinishedLogControlFlow,
    FinishedManifest,
    LogControlFlow,
    ManifestControlFlow,
    OpenCorruptionHandler,
};



#[expect(clippy::struct_excessive_bools, reason = "TODO: Make LogReuse enum")]
#[derive(Debug, Clone, Copy)]
pub struct DefaultOpenHandlerOptions {
    pub verify_recovered_version:    bool,
    pub allow_final_truncated_entry: bool,
    pub try_reuse_manifest:          bool,
    pub try_reuse_write_ahead_log:   bool,
}

impl DefaultOpenHandlerOptions {
    #[inline]
    #[must_use]
    pub const fn try_reuse_files() -> Self {
        Self {
            verify_recovered_version:    false,
            allow_final_truncated_entry: true,
            try_reuse_manifest:          true,
            try_reuse_write_ahead_log:   true,
        }
    }

    #[inline]
    #[must_use]
    pub const fn never_reuse_files() -> Self {
        Self {
            verify_recovered_version:    false,
            allow_final_truncated_entry: true,
            try_reuse_manifest:          false,
            try_reuse_write_ahead_log:   false,
        }
    }
}

impl Default for DefaultOpenHandlerOptions {
    #[inline]
    fn default() -> Self {
        Self::never_reuse_files()
    }
}

#[expect(clippy::struct_excessive_bools, reason = "TODO: Make LogReuse enum")]
#[derive(Debug)]
pub struct DefaultOpenHandler<InvalidKey> {
    verify_recovered_version:    bool,
    allow_final_truncated_entry: bool,
    manifest_reuse_permitted:    bool,
    log_reuse_permitted:         bool,
    status:                      Result<(), HandlerError<InvalidKey>>,
}

impl<InvalidKey> DefaultOpenHandler<InvalidKey> {
    #[inline]
    #[must_use]
    pub const fn new(opts: DefaultOpenHandlerOptions) -> Self {
        Self {
            verify_recovered_version:    opts.verify_recovered_version,
            allow_final_truncated_entry: opts.allow_final_truncated_entry,
            manifest_reuse_permitted:    opts.try_reuse_manifest,
            log_reuse_permitted:         opts.try_reuse_write_ahead_log,
            status:                      Ok(()),
        }
    }
}

impl<InvalidKey> OpenCorruptionHandler<InvalidKey> for DefaultOpenHandler<InvalidKey> {
    fn manifest_corruption(
        &mut self,
        offset:     FileOffset,
        cause:       BinaryBlockLogCorruptionError,
        _bytes_lost: usize,
        _file_size:  FileSize,
    ) -> ManifestControlFlow {
        self.manifest_reuse_permitted = false;

        if self.allow_final_truncated_entry && matches!(cause,
            BinaryBlockLogCorruptionError::TruncatedHeader
            | BinaryBlockLogCorruptionError::TruncatedPhysicalRecord
            | BinaryBlockLogCorruptionError::TruncatedLogicalRecord
        ) {
            ManifestControlFlow::BreakSuccess
        } else {
            if self.status.is_ok() {
                self.status = Err(HandlerError::ManifestFile(offset, cause));
            }
            ManifestControlFlow::BreakError
        }
    }

    fn version_edit_corruption(
        &mut self,
        offset: LogicalRecordOffset,
        cause:  VersionEditDecodeError<InvalidKey>,
    ) -> ManifestControlFlow {
        self.manifest_reuse_permitted = false;

        if self.status.is_ok() {
            self.status = Err(HandlerError::VersionEdit(offset, cause));
        }
        ManifestControlFlow::BreakError
    }

    fn finished_manifest(&mut self) -> Result<FinishedManifest, FinishError> {
        if self.status.is_ok() {
            Ok(FinishedManifest {
                verify_recovered_version: self.verify_recovered_version,
                manifest_reuse_permitted: self.manifest_reuse_permitted,
            })
        } else {
            Err(FinishError)
        }
    }

    fn log_corruption(
        &mut self,
        file_num:    FileNumber,
        offset:      FileOffset,
        cause:       BinaryBlockLogCorruptionError,
        _bytes_lost: usize,
        _file_size:  FileSize,
    ) -> LogControlFlow {
        self.log_reuse_permitted = false;

        let ignore_error = self.allow_final_truncated_entry && matches!(cause,
            BinaryBlockLogCorruptionError::TruncatedHeader
            | BinaryBlockLogCorruptionError::TruncatedPhysicalRecord
            | BinaryBlockLogCorruptionError::TruncatedLogicalRecord
        );

        if !ignore_error && self.status.is_ok() {
            self.status = Err(HandlerError::LogFile(file_num, offset, cause));
        }

        LogControlFlow::Break
    }

    fn write_batch_corruption(
        &mut self,
        file_num: FileNumber,
        offset:   LogicalRecordOffset,
        cause:    WriteBatchDecodeError,
    ) -> LogControlFlow {
        self.log_reuse_permitted = false;

        if self.status.is_ok() {
            self.status = Err(HandlerError::WriteBatch(file_num, offset, cause));
        }
        LogControlFlow::Break
    }

    fn finished_log(&mut self) -> (FinishedLog, FinishedLogControlFlow) {
        let finished_log = FinishedLog {
            log_reuse_permitted: self.log_reuse_permitted,
        };
        let control_flow = if self.status.is_ok() {
            FinishedLogControlFlow::Continue
        } else {
            FinishedLogControlFlow::BreakError
        };
        // Maybe the next log can be reused.
        self.log_reuse_permitted = true;
        (finished_log, control_flow)
    }

    fn finished_all_logs(&mut self) -> Result<FinishedAllLogs, FinishError> {
        if self.status.is_ok() {
            Ok(FinishedAllLogs {
                verify_new_version: self.verify_recovered_version,
            })
        } else {
            Err(FinishError)
        }
    }

    fn get_error(self: Box<Self>) -> Option<HandlerError<InvalidKey>> {
        self.status.err()
    }
}
