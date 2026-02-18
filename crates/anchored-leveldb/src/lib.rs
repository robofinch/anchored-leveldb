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
/// Settings that the user isn't allowed to change.
mod config_constants;

/// Every possible error emitted by this crate.
mod errors;

/// Implementations for various compression codecs, as well as a `CompressorList` struct to
/// bundle up to 12 codecs together into a `CompressionCodecs` implementation.
///
/// (Exceeding the limit of 12 codecs simply means that a manual implementation of
/// `CompressionCodecs` would be necessary.)
mod compression;

/// `WriteBatch`, `UnvalidatedWriteBatch`, `WriteBatchIter`, `WriteEntry`, and `EntryType`.
mod write_batch;
/// `LengthPrefixedBytes`, the sole public (honorary) member of [`crate::typed_bytes`].
mod length_prefixed_bytes;

// ================================================================
//  Lower-level details of this LevelDB implementation
//  (These focus on individual components of the database.)
// ================================================================

/// Welcome to numeric hell.
///
/// Almost everything in this crate that one might represent as a byte slice or unsigned integer
/// is given a more refined type here.
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

/// Structure for tracking the `Snapshot`s held by the user.
mod snapshot_list;

/// The binary log format used for write-ahead logs (i.e., `X.log` files) and database manifests
/// (i.e., `MANIFEST-X` files, also known as database descriptors).
///
/// Not to be confused with the [`logger`] module.
mod write_log;
/// Logs human-readable informational messages.
mod logger;

/// Hold a lockfile alongside its source filesystem, releasing the lockfile on drop.
mod fs_guard;

// TODO: provide ways to customize threading.
//
// Need some way to abstract over `Mutex` impl. Note that `anchored-pool` would also care.
// I think that's sufficient to justify a new `anchored-mutex` crate.
//
// Also need to abstract over how threads are spawned (if at all). In particular:
// - threads required to be truly concurrent, such as for decompression and whatnot
// - thread required to run some function once and then exit, which could be emulated
//   without actual multithreading
//
// Also need to abstract over how threads are told to sleep, and how time is measured (the latter
// is sort of optional, not necessary for MVP).
//
// FOR NOW: use `std::thread`

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
