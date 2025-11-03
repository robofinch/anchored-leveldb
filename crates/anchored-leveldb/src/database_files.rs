use std::{
    io::{Error as IoError, Write as _},
    path::{Path, PathBuf},
};

use thiserror::Error;

use anchored_vfs::traits::{WritableFile as _, WritableFilesystem};

use crate::format::FileNumber;


#[derive(Debug, Clone, Copy)]
pub(crate) enum LevelDBFileName {
    Log {
        file_number: FileNumber,
    },
    Lockfile,
    Table {
        file_number: FileNumber,
    },
    TableLegacyExtension {
        file_number: FileNumber,
    },
    Manifest {
        file_number: FileNumber,
    },
    Current,
    Temp {
        file_number: FileNumber,
    },
    InfoLog,
    OldInfoLog,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl LevelDBFileName {
    #[must_use]
    pub fn parse(file_name: &str) -> Option<Self> {
        // Currently, all valid file names for LevelDB files are valid 7-bit ASCII and thus
        // valid UTF-8.

        // Note that all the valid file names are nonempty
        let &first_byte = file_name.as_bytes().first()?;
        // `from_str_radix` permits a leading sign, including `+`. We need to reject this case.
        if first_byte == b'+' {
            return None;
        }

        if let Some(file_number) = file_name.strip_suffix(".ldb") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Table { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".log") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Log { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".sst") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::TableLegacyExtension { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".dbtmp") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Temp { file_number })

        } else if let Some(file_number) = file_name.strip_prefix("MANIFEST-") {
            // Any file number, even 0, would make it nonempty.
            let &first_num_byte = file_number.as_bytes().first()?;
            // `from_str_radix` permits a leading sign, including `+`. We need to reject this case.
            if first_num_byte == b'+' {
                return None;
            }

            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Manifest { file_number })

        } else {
            Some(match file_name {
                "LOCK"    => Self::Lockfile,
                "CURRENT" => Self::Current,
                "LOG"     => Self::InfoLog,
                "LOG.old" => Self::OldInfoLog,
                _         => return None,
            })
        }
    }

    #[must_use]
    pub fn file_name(self) -> String {
        match self {
            Self::Log { file_number }                  => format!("{:06}.log",      file_number.0),
            Self::Lockfile                             => "LOCK".to_owned(),
            Self::Table { file_number }                => format!("{:06}.ldb",      file_number.0),
            Self::TableLegacyExtension { file_number } => format!("{:06}.sst",      file_number.0),
            Self::Manifest { file_number }             => format!("MANIFEST-{:06}", file_number.0),
            Self::Current                              => "CURRENT".to_owned(),
            Self::Temp { file_number }                 => format!("{:06}.dbtmp",    file_number.0),
            Self::InfoLog                              => "LOG".to_owned(),
            Self::OldInfoLog                           => "LOG.old".to_owned(),
        }
    }

    pub fn file_path(self, directory: &Path) -> PathBuf {
        // Technically this performs slightly more allocation than strictly necessary
        // in the lockfile, current, infolog, and oldinfolog cases.
        directory.join(self.file_name())
    }
}

/// `manifest_name` is not validated, but it should agree with `db_directory` and
/// `manifest_file_number`.
pub(crate) fn set_current<FS: WritableFilesystem>(
    filesystem:           &mut FS,
    db_directory:         &Path,
    manifest_file_number: FileNumber,
    manifest_name:        &str,
) -> Result<(), SetCurrentError<FS::Error>> {
    /// Used for a `try` scope.
    fn perform_writes<FS: WritableFilesystem>(
        filesystem:    &mut FS,
        db_directory:  &Path,
        manifest_name: &str,
        temp_path:     &Path,
        mut temp_file: FS::WriteFile,
    ) -> Result<(), SetCurrentError<FS::Error>> {
        temp_file.write_all(manifest_name.as_bytes()).map_err(SetCurrentError::Write)?;
        temp_file.write_all(b"\n").map_err(SetCurrentError::Write)?;

        temp_file.sync_data().map_err(SetCurrentError::FileFsync)?;

        filesystem
            .rename(temp_path, &LevelDBFileName::Current.file_path(db_directory))
            .map_err(SetCurrentError::Rename)?;

        Ok(())
    }

    let temp_path = LevelDBFileName::Temp { file_number: manifest_file_number }
        .file_path(db_directory);

    let temp_file = filesystem
        .open_writable(&temp_path, false)
        .map_err(SetCurrentError::Open)?;

    // Try to clean up the temporary file on error. Of course, if an fsync error occurred and
    // closing it fails (which is not tracked by the returned error), retrying would definitely
    // be bad. Though I'm not sure if it'd be fine to retry if closing did succeed...
    // my rule of thumb is "fsync error -> fatal".
    if let Err(error) = perform_writes(
        filesystem,
        db_directory,
        manifest_name,
        &temp_path,
        temp_file,
    ) {
        // Ignore any additional error; the original error is the only important thing.
        // At worst, the leftover file will be garbage-collected later.
        let _err = filesystem.delete(&temp_path);
        return Err(error);
    }

    // TODO: fsync the `db_directory`.

    Ok(())
}

#[derive(Error, Debug)]
pub(crate) enum SetCurrentError<FilesystemError> {
    /// An error from opening a temporary file.
    #[error("filesystem error when opening a temp file, preventing update of CURRENT file: {0}")]
    Open(FilesystemError),
    /// An error from writing to a file.
    #[error("write error, preventing update of CURRENT file: {0}")]
    Write(IoError),
    /// An error from renaming a temporary file to CURRENT.
    #[error("filesystem error when renaming a temp file to the CURRENT file: {0}")]
    Rename(FilesystemError),
    /// A likely-fatal error while attempting to sync the data of a file.
    #[error("likely-fatal fsyncdata error while setting CURRENT file: {0}")]
    FileFsync(IoError),
    /// A likely-fatal error while attempting to sync the data of a directory.
    #[error("likely-fatal fsyncdata error while setting CURRENT file: {0}")]
    DirectoryFsync(FilesystemError),
}


#[cfg(test)]
mod tests {
    use super::*;


    /// Tests that the filenames do not have directory components.
    #[test]
    fn file_name_has_no_slash() {
        for file_number in 0..10 {
            let file_number = FileNumber(file_number);
            for file_name in [
                LevelDBFileName::Log { file_number },
                LevelDBFileName::Table { file_number },
                LevelDBFileName::TableLegacyExtension { file_number },
                LevelDBFileName::Manifest { file_number },
                LevelDBFileName::Temp { file_number },
            ].map(LevelDBFileName::file_name) {
                let file_name = PathBuf::from(file_name);
                assert_eq!(file_name.file_name(), Some(file_name.as_os_str()));
            }
        }

        for file_name in [
            LevelDBFileName::Lockfile,
            LevelDBFileName::Current,
            LevelDBFileName::InfoLog,
            LevelDBFileName::OldInfoLog,
        ].map(LevelDBFileName::file_name) {
            let file_name = PathBuf::from(file_name);
            assert_eq!(file_name.file_name(), Some(file_name.as_os_str()));
        }
    }
}
