use crate::{
    all_errors::types::{
        BinaryBlockLogCorruptionError, FinishError, HandlerError,
        VersionEditDecodeError, WriteBatchDecodeError,
    },
    pub_typed_bytes::{FileNumber, FileOffset, FileSize, LogicalRecordOffset},
};


pub trait OpenCorruptionHandler {
    // the offset is the start of the dropped record. *Usually*, that means the start
    // of a corrupted physical
    fn manifest_corruption(
        &mut self,
        offset:     FileOffset,
        cause:      BinaryBlockLogCorruptionError,
        bytes_lost: usize,
        file_size:  FileSize,
    ) -> ManifestControlFlow;

    fn version_edit_corruption(
        &mut self,
        offset: LogicalRecordOffset,
        cause:  VersionEditDecodeError,
    ) -> ManifestControlFlow;

    fn finished_manifest(&mut self) -> Result<FinishedManifest, FinishError>;

    fn log_corruption(
        &mut self,
        file_num:   FileNumber,
        offset:     FileOffset,
        cause:      BinaryBlockLogCorruptionError,
        bytes_lost: usize,
        file_size:  FileSize,
    ) -> LogControlFlow;

    fn write_batch_corruption(
        &mut self,
        file_num: FileNumber,
        offset:   LogicalRecordOffset,
        cause:    WriteBatchDecodeError,
    ) -> LogControlFlow;

    fn finished_log(&mut self) -> (FinishedLog, FinishedLogControlFlow);

    fn finished_all_logs(&mut self) -> Result<FinishedAllLogs, FinishError>;

    #[must_use]
    fn get_error(self) -> Option<HandlerError>;
}

#[derive(Debug, Clone, Copy)]
#[must_use]
pub enum ManifestControlFlow {
    /// Ignore corrupted logical or physical records and continue reading the manifest file.
    Continue,
    /// Stop reading the manifest file, without aborting the entire process of opening the
    /// database.
    BreakSuccess,
    /// Stop the process of opening the database and report an error.
    BreakError,
}

#[derive(Debug, Clone, Copy)]
#[must_use]
pub enum LogControlFlow {
    /// Ignore corrupted logical or physical records and continue reading the log file.
    Continue,
    /// Stop reading the current log file, but continue reading any following log files.
    ContinueOtherLogs,
    /// Stop reading log files, without aborting the entire process of opening the
    /// database.
    BreakSuccess,
    /// Stop the process of opening the database and report an error.
    BreakError,
}

#[derive(Debug, Clone, Copy)]
#[must_use]
pub enum FinishedLogControlFlow {
    /// Continue reading any following log files.
    Continue,
    /// Stop reading log files, without aborting the entire process of opening the
    /// database.
    BreakSuccess,
    /// Stop the process of opening the database and report an error.
    BreakError,
}

#[derive(Debug, Clone, Copy)]
pub struct FinishedManifest {
    pub verify_recovered_version: bool,
    pub manifest_reuse_permitted: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct FinishedLog {
    pub log_reuse_permitted: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct FinishedAllLogs {
    pub verify_new_version: bool,
}
