use std::path::Path;

use anchored_vfs::{FSError as _, FSLockError as _, IntoChildFileIterator as _, LevelDBFilesystem};

use crate::database_files::LevelDBFileName;
use crate::all_errors::types::{DestroyError, DestroyErrorKind, RemoveError};


/// Irreversibly delete the contents an entire LevelDB database.
///
/// If the database does not exist, `Ok(())` is returned.
///
/// Warning: it is a bad idea to attempt to simultaneously create and delete a LevelDB database
/// in a certain directory. It seems plausible that, in some edge case, the process (or thread)
/// creating the database could acquire the lockfile just before this function removes the lockfile,
/// which may allow later database invocations to think that the lockfile had not been acquired,
/// potentially enabling database corruption.
pub fn irreversibly_destroy_entire_db<FS: LevelDBFilesystem>(
    filesystem:   &mut FS,
    db_directory: &Path,
) -> Result<(), DestroyError<FS::Error>> {
    let lockfile_path = LevelDBFileName::Lockfile.file_path(db_directory);
    let lockfile = match filesystem.open_and_lock(&lockfile_path) {
        Ok(lockfile) => lockfile,
        Err(lock_error) => {
            if lock_error.is_not_found() {
                return Ok(());
            } else if lock_error.is_already_locked() {
                return Err(DestroyError {
                    db_directory: db_directory.to_owned(),
                    kind:         DestroyErrorKind::DatabaseLocked,
                });
            } else {
                return Err(DestroyError {
                    db_directory: db_directory.to_owned(),
                    kind:         DestroyErrorKind::LockError(lock_error.into()),
                });
            }
        }
    };

    let db_files = match filesystem.child_files(db_directory) {
        Ok(db_files) => db_files,
        Err(fs_err) => {
            if fs_err.is_not_found() {
                return Ok(());
            } else {
                return Err(DestroyError {
                    db_directory: db_directory.to_owned(),
                    kind:         DestroyErrorKind::OpenDatabaseDirectory(fs_err),
                });
            }
        }
    };

    let mut remove_errors = Vec::new();

    for file in db_files.child_files() {
        let file_name = match file {
            Ok((file_name, _file_size)) => file_name,
            Err(fs_err) => {
                remove_errors.push((fs_err.into(), RemoveError::ReadDatabaseDirectory));
                continue;
            }
        };

        // All of LevelDB's files have ASCII names, so files with non-UTF-8 names can be ignored.
        let Some(utf8_file_name) = file_name.as_os_str().to_str() else { continue };
        // LevelDB's files would parse successfully.
        let Some(parsed_file_name) = LevelDBFileName::parse(utf8_file_name) else { continue };

        if matches!(parsed_file_name, LevelDBFileName::Lockfile) {
            continue;
        }

        match filesystem.remove_file(&db_directory.join(&file_name)) {
            Ok(()) => {}
            Err(fs_err) => remove_errors.push((fs_err, RemoveError::RemoveFileError(file_name))),
        }
    }

    drop(lockfile);
    {
        // Ignore error since state is already gone.
        let _ignore1: Result<(), _> = filesystem.remove_file(&lockfile_path);
        // Ignore error in case the directory contains other files.
        let _ignore2: Result<(), _> = filesystem.remove_dir(db_directory);
    };

    if remove_errors.is_empty() {
        Ok(())
    } else {
        Err(DestroyError {
            db_directory: db_directory.to_owned(),
            kind:         DestroyErrorKind::RemoveFileErrors(remove_errors),
        })
    }
}
