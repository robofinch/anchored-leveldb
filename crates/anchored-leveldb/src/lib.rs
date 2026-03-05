// ================================================================
//  Traits and utilities in the public interface
// ================================================================
#![allow(unused_crate_dependencies, reason = "temporary; TODO: use all deps")]

/// `LevelDBComparator`, `FilterPolicy`, `CompressionCodecs`, `BufferPool`, `PooledBuffer`,
/// `Logger`, `ErrorHandler`, a few helper traits for those main traits,
/// and implementations of most of them.
///
/// (Except `BufferPool` and `PooledBuffer`, which are not yet implemented.)
mod pub_traits;

/// Options structs galore.
mod options;

/// Every possible error emitted by this crate.
mod all_errors;

/// Implementations for various compression codecs, as well as a `CompressorList` struct to
/// bundle up to 12 codecs together into a `CompressionCodecs` implementation.
///
/// (Exceeding the limit of 12 codecs simply means that a manual implementation of
/// `CompressionCodecs` would be necessary.)
mod compression;

/// Welcome to numeric hell.
///
/// Almost every public type in this crate that one might represent as a byte slice or unsigned
/// integer is given a more refined type here.
///
/// (The types are not just trivial wrappers, of course, since they come with useful methods.)
mod pub_typed_bytes;

// ================================================================
//  Lower-level details of this LevelDB implementation
//  (These focus on individual components of the database.)
// ================================================================

/// Welcome to numeric hell.
///
/// Almost every internal type in this crate that one might represent as a byte slice or unsigned
/// integer is given a more refined type here.
///
/// (The types are not just trivial wrappers, of course, since they come with useful methods.)
mod typed_bytes;

/// Welcome to generic hell.
mod leveldb_generics;
/// Utilities to get the common prefix of two byte slices, a varint implementation, and a few
/// other odds and ends.
mod utils;

/// `BlockCache` and `TableCache`.
mod table_caches;
/// `InternalComparator` and `InternalFilterPolicy`.
mod table_format;
/// Sorted-string table implementation.
///
/// Technically, the table format does not need to be tied to LevelDB; however, this implementation
/// is slightly coupled to the rest of `anchored-leveldb` for the sake of convenience.
///
/// In particular, `InternalComparator`, `InternalPolicy`, hardcoded usage of the metadata
/// block to store a filter policy's name (with a certain prefix prepended), and usage of this
/// crate's traits and error types result in some arguably-unnecessary coupling.
mod table;
/// Slightly higher-level interface for the [`table`] module, with greater filesystem utilities.
mod table_file;

/// Wrappers around types in [`anchored_skiplist`], and a definition of the memtable format.
mod memtable;
/// `WriteBatch`, `BorrowedWriteBatch`, `WriteBatchIter`, `WriteEntry`,
/// `ChainedWriteBatches`, `ChainedWriteBatchIter`.
///
/// Note that `WriteBatchIter` and `WriteEntry` are for the benefit of users. They aren't used
/// within this crate (excluding tests).
mod write_batch;
/// A writer queue used to merge concurrent write operations into one. In other words, under
/// heavy contention, writers get pushed onto a queue, processing them more efficiently than
/// letting them freely contend with a mutex.
mod contention_queue;

/// Structure for tracking the `Snapshot`s held by the user.
mod snapshot_list;

/// The binary log format used for write-ahead logs (i.e., `X.log` files) and database manifests
/// (i.e., `MANIFEST-X` files, also known as database descriptors).
///
/// Not to be confused with the [`logger`] module.
mod binary_block_log;
/// Logs human-readable informational messages.
mod logger;

/// Hold a lockfile alongside its source filesystem, releasing the lockfile on drop.
mod fs_guard;

// TODO: provide ways to customize threading. Though, at present time, there's no actual use
// case for anything but "enable multithreading with `std::{sync, thread}`" and
// "disable multithreading" aside from WASM+atomics, which Rust does not (yet) well-support

// ================================================================
//  Higher-level details of this LevelDB implementation
//  (These are what organize everything into a database.)
// ================================================================

mod database_files;
mod file_tracking;

mod version;
mod compaction;

mod read_sampling;

mod inner_leveldb;
mod internal_iters;

mod scan_db;

// ================================================================
//  Public interface of database structs
// ================================================================

mod snapshot;

mod generic_leveldb;

// ================================================================
//  Public exports
// ================================================================

pub mod db_settings {
    pub use crate::{
        pub_traits::{
            cmp_and_policy::{
                AllEqual, BloomPolicy, BloomPolicyOverflow, BytewiseComparator, BytewiseEquality,
                CoarserThan, EquivalenceRelation, FilterPolicy, LevelDBComparator, NoFilterPolicy,
            },
            compression::{CompressionCodecError, CompressionCodecs, CompressorId},
            // more compression stuff
        },
    };
}

pub mod db_options {
    pub use crate::{
        binary_block_log::{HEADER_SIZE, WRITE_LOG_BLOCK_SIZE},
        pub_traits::{
            pool::{BufferAllocError, BufferPool, PooledBuffer},
            error_handler::{
                FinishedAllLogs, FinishedLog, FinishedLogControlFlow, FinishedManifest,
                LogControlFlow, ManifestControlFlow, OpenCorruptionHandler,
            },
        },
        // logger
        // error handler
    };
}

/// Types and traits used to interface with an `anchored-leveldb` LevelDB implementation
/// (aside from settings and options).
pub mod db_interface {
    pub use crate::pub_typed_bytes::PrefixedBytes;
    pub use crate::write_batch::{
        BorrowedWriteBatch, ChainedWriteBatches, WriteBatch, WriteBatchIter, WriteEntry,
    };

    // pub_typed_bytes, snapshot, various `LevelDB` structs.
}

pub mod errors {
    pub use crate::all_errors::types::{
        BinaryBlockLogCorruptionError, BlockHandleCorruption, BlockType, CompressedBlockError,
        CorruptedBlockError, CorruptedLogError, CorruptedManifestError, CorruptedTableError,
        CorruptedVersionError, CorruptionError, DestroyError, DestroyErrorKind, FilesystemError,
        FinishError, HandlerError, InitEmptyDatabaseError, OpenError, OpenFsError, OptionsError,
        PrefixedBytesParseError, PushBatchError, ReadError, ReadFsError, RecoveryError,
        RecoveryErrorKind, RemoveError, RwError, RwErrorKind, SetCurrentError, SettingsError,
        VersionEditDecodeError, WriteBatchDecodeError, WriteBatchDeleteError, WriteBatchPutError,
        WriteBatchValidationError, WriteError, WriteFsError,
    };

    // These types are not exposed except via error types.
    pub use crate::pub_typed_bytes::{
        BlockHandle, EntryType, FileNumber, FileOffset, FileSize, Level, LogicalRecordOffset,
        MinU32Usize, NonZeroLevel, PhysicalRecordType, SequenceNumber, TableBlockOffset,
    };
}

// Export common traits, types, and default options.
pub use self::{
    db_settings::{BloomPolicy, BytewiseComparator, FilterPolicy, LevelDBComparator},
    errors::{RecoveryError, RwError},
};
