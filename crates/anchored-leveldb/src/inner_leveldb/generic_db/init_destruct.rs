use std::path::Path;

use generic_container::FragileTryContainer as _;

use anchored_vfs::traits::{ReadableFilesystem as _, WritableFile, WritableFilesystem as _};

use crate::{snapshot::SnapshotList, version::VersionEdit, write_log::WriteLogWriter};
use crate::{
    containers::{FragileRwCell as _, RefcountedFamily},
    database_files::{LevelDBFileName, set_current},
    format::{FileNumber, SequenceNumber},
    table_traits::{InternalComparator, LevelDBComparator as _},
    leveldb_generics::{LdbContainer, LdbFsCell, LdbRwCell, LevelDBGenerics},
};
use super::super::{fs_guard::FSGuard, write_impl::DBWriteImpl};
use super::super::{
    builder::{BuildGenericDB, InitOptions, InnerGenericDBBuilder},
    db_data::{DBShared, DBSharedMutable, ReadWriteStatus},
};
use super::InnerGenericDB;


#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    pub fn open(
        init_options: InitOptions<LDBG, WriteImpl>,
        filesystem:   LdbFsCell<LDBG>,
    ) -> Result<Self, ()> {
        // Acquire the lockfile as soon as possible. Note that `filesystem: FSGuard` handles
        // releasing the lockfile, so we can use `?` throughout this function without issue.
        let filesystem = Self::acquire_lockfile(&init_options.db_directory, filesystem)?;
        let mut fs_lock = filesystem.filesystem.write();

        // Try to open `CURRENT`. Handle the `create_if_missing` and `error_if_exists`
        // settings.
        let current_path = LevelDBFileName::Current.file_path(&init_options.db_directory);
        let database_exists = fs_lock.exists(&current_path).map_err(|_| ())?;

        if database_exists && init_options.db_options.error_if_exists {
            return Err(());
        } else if !database_exists && !init_options.db_options.create_if_missing {
            return Err(());
        } else if !database_exists && init_options.db_options.create_if_missing {
            Self::write_empty_database(
                &init_options.db_directory,
                &mut fs_lock,
                &init_options.table_options.comparator,
            )?;
            // Continue below to recover the newly-written empty database.
        } else {
            // Continue below to recover the existing database.
        }

        drop(fs_lock);

        InnerGenericDBBuilder::recover_existing(
            init_options,
            filesystem,
        )
    }

    // close - halt compaction and prevent all future reads and writes from succeeding.
    // Existing iterators may start to return `None`, but are _not_ necessarily invalidated.
    // In order to ensure that the ground is not ripped out from under the iterators' feet,
    // the database lockfile is not unlocked until all outstanding iterators are dropped.
    // In other words, you must ensure that existing iterators are dropped in a timely manner.
    // If there are not outstanding iterators, this method will wait for compaction to stop,
    // then close the database and release its lockfile.
    // Ok(CloseStatus)
    // Err(_)
    // CloseStatus: EntirelyClosed, OpenDueToIterators(DB) (or OutstandingIterators)

    // Similar to close, but does not kill the current compaction, and instead waits for it
    // to finish. *other* reads and writes are blocked right away, though.
    // close_after_compaction
    // NOTE: try to have `close` and `close_after_compaction` affect iterators whenever they're
    // about to read from the filesystem, with the option to NOT affect compaction-related
    // processes.

    // irreversibly_delete_db
    // later: repair_db
    // later: clone_db
}

impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    #[must_use]
    pub(in super::super) fn build(build_version: BuildGenericDB<LDBG, WriteImpl>) -> Self {
        // Ensure that no fields are forgotten
        let BuildGenericDB {
            db_directory,
            filesystem,
            table_cache,
            table_options,
            db_options,
            corruption_handler,
            version_set,
            current_memtable,
            current_log,
            info_logger,
            write_impl,
        } = build_version;

        let (write_data, mutable_write_data) = write_impl.split();

        let shared = DBShared {
            db_directory,
            filesystem,
            table_cache,
            table_options,
            db_options,
            corruption_handler,
            write_data,
        };

        let shared_mutable = DBSharedMutable {
            version_set,
            snapshot_list: SnapshotList::<LDBG::Refcounted, LDBG::RwCell>::new(),
            current_memtable,
            current_log,
            memtable_under_compaction: None,
            iter_read_sample_seed:     0,
            info_logger,
            readwrite_status:          ReadWriteStatus::Open,
            mutable_write_data,
        };

        Self(LdbContainer::<LDBG, _>::new_container((
            shared,
            LdbRwCell::<LDBG, _>::new_rw_cell(shared_mutable),
        )))
    }

    /// Attempt to acquire the `LOCK` file in the database directory, and wrap the lockfile
    /// and filesystem into an `FSGuard` that releases the lockfile on drop.
    fn acquire_lockfile(
        db_directory: &Path,
        filesystem: LdbFsCell<LDBG>,
    ) -> Result<FSGuard<LDBG>, ()> {
        let mut fs_lock = filesystem.write();

        // Ignore any error when creating the database directory. If this fails, we'll get
        // an error below anyway. And it could plausibly error for some reason OTHER than
        // us not having permissions to access the existing `db_directory` directory.
        #[expect(
            let_underscore_drop,
            clippy::let_underscore_must_use,
            reason = "if the error is important, we'll get an error when acquiring the lockfile",
        )]
        let _: Result<(), _> = fs_lock.create_dir_all(db_directory);

        let lockfile_path = LevelDBFileName::Lockfile.file_path(db_directory);
        // Wrap the lockfile into an `FSGuard` as soon as possible, so that we release the lock
        // when the `FSGuard` is dropped. That way we can mindlessly use `?` elsewhere without
        // worrying about cleanup.
        let lockfile = fs_lock.create_and_lock(&lockfile_path, false).map_err(|_| ())?;
        drop(fs_lock);
        Ok(FSGuard {
            filesystem,
            lockfile: Some(lockfile),
        })
    }

    /// Create a new LevelDB database, assuming that no database currently exists in
    /// `db_directory`. (If there was one, it may be overwritten.)
    ///
    /// To create an empty `InnerGenericDB` struct, the rest of the normal recovery process can then
    /// be performed on the just-written database.
    fn write_empty_database(
        db_directory:       &Path,
        filesystem:         &mut LDBG::FS,
        comparator:         &InternalComparator<LDBG::Cmp>,
    ) -> Result<(), ()> {
        fn try_scope<Refcounted: RefcountedFamily, File: WritableFile>(
            new_db:        &VersionEdit<Refcounted>,
            manifest_file: File,
        ) -> Result<(), ()> {
            let mut manifest_writer = WriteLogWriter::new_empty(manifest_file);
            let mut new_db_record = Vec::new();
            new_db.encode(&mut new_db_record);
            manifest_writer.add_record(&new_db_record).map_err(|_| ())?;
            manifest_writer.sync_log_data().map_err(|_| ())?;
            Ok(())
        }

        let mut new_db: VersionEdit<LDBG::Refcounted> = VersionEdit::new_empty();
        new_db.comparator_name  = Some(comparator.0.name().to_owned());
        // No file is actually created with file number `0`. And during the recovery process,
        // there will be no previous `.log` file to reuse, so a new `.log` file will be created;
        // that new file will have a file number of at least `2` (from `new_db.next_file_number`).
        new_db.log_number       = Some(FileNumber(0));
        let manifest_number     = FileNumber(1);
        new_db.next_file_number = Some(FileNumber(2));
        new_db.last_sequence    = Some(SequenceNumber::ZERO);

        let manifest = LevelDBFileName::Manifest { file_number: manifest_number };
        let manifest_path = manifest.file_path(db_directory);
        let manifest_file = filesystem
            .open_writable(&manifest_path, false)
            .map_err(|_| ())?;

        try_scope(&new_db, manifest_file).inspect_err(|_| {
            // Try to clean up the now-pointless manifest file. No worries if that fails,
            // the next time that file is opened, it'll be with `open_writable` not
            // `open_appendable`, so no corruption can occur.
            // Also, any leftover file will eventually be garbage-collected.
            let _err = filesystem.delete(&manifest_path);
        })?;

        set_current(
            filesystem,
            db_directory,
            manifest_number,
            &manifest.file_name(),
        ).map_err(|_| ())?;

        Ok(())
    }
}
