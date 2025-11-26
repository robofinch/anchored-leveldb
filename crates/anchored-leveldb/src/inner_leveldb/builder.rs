use std::path::PathBuf;

use tracing::Level as LogLevel;

use anchored_vfs::traits::{ReadableFilesystem as _, WritableFilesystem};

use crate::containers::FragileRwCell as _;
use crate::corruption_handler::InternalCorruptionHandler;
use crate::info_logger::InfoLogger;
use crate::memtable::Memtable;
use crate::version::VersionSet;
use crate::leveldb_generics::{
    LdbFsCell, LdbLockfile, LdbTableOptions, LdbWriteFile, LevelDBGenerics,
};
use crate::write_log::WriteLogWriter;
use super::fs_guard::FSGuard;
use super::write_impl::DBWriteImpl;
use super::db_data::{InnerDBOptions, WriteStatus};


/// The data necessary to create a [`InnerGenericDB`].
///
/// [`InnerGenericDB`]: super::generic_db::InnerGenericDB
pub(super) struct BuildGenericDB<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> {
    pub db_directory:              PathBuf,
    pub filesystem:                FSGuard<LDBG>,
    pub table_cache:               LDBG::TableCache,
    pub table_options:             LdbTableOptions<LDBG>,
    pub db_options:                InnerDBOptions,
    pub corruption_handler:        InternalCorruptionHandler<LDBG::Refcounted, LDBG::RwCell>,
    pub version_set:               VersionSet<LDBG::Refcounted, LdbWriteFile<LDBG>>,
    pub current_memtable:          Memtable<LDBG::Cmp, LDBG::Skiplist>,
    pub current_log:               WriteLogWriter<LdbWriteFile<LDBG>>,
    pub info_logger:               InfoLogger<LdbWriteFile<LDBG>>,
    pub write_status:              WriteStatus,
    pub write_impl:                WriteImpl,
}
