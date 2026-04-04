use std::{path::PathBuf, time::Duration};
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    num::{NonZeroU8, NonZeroU16, NonZeroU32, NonZeroUsize},
};

use tracing::level_filters::LevelFilter;

use crate::snapshot::Snapshot;
use crate::{
    pub_traits::{
        compression::CompressorId,
        cmp_and_policy::{BloomPolicy, LevelDBComparator},
        error_handler::OpenCorruptionHandler,
        logger::Logger,
    },
    pub_typed_bytes::{
        BinaryLogBlockSize, FileSize, Level, NUM_MIDDLE_LEVELS_USIZE, NUM_NONZERO_LEVELS_USIZE,
    },
};

#[cfg(any(feature = "google-leveldb-compression", feature = "mojang-leveldb-compression"))]
use crate::pub_traits::cmp_and_policy::BytewiseComparator;
#[cfg(feature = "google-leveldb-compression")]
use crate::compression::GoogleLevelDBCodecs;
#[cfg(feature = "mojang-leveldb-compression")]
use crate::compression::MojangLevelDBCodecs;


// TODO: Finish docs
// TODO: in particular, it needs to be documented *somewhere* that attacker-controlled LevelDB
// databases can cause OOM errors (if the database files are sufficiently large, e.g. gigabytes)


#[derive(Debug)]
pub struct OpenOptions<FS, Cmp: LevelDBComparator, Policy, Codecs, Pool> {
    pub filesystem:         FS,
    pub database_directory: PathBuf,
    pub create_if_missing:  bool,
    pub error_if_exists:    bool,
    pub clamp_options:      ClampOptions,
    pub format:             FormatSettings<Cmp, Codecs>,
    pub compression:        CompressionOptions,
    pub filter:             FilterOptions<Policy>,
    pub consistency:        ConsistencyOptions<Cmp::InvalidKeyError>,
    pub logger:             LoggerOptions,
    pub manifest:           ManifestOptions,
    pub memtable:           MemtableOptions,
    pub sstable:            SSTableOptions,
    pub compaction:         CompactionOptions,
    pub size_compaction:    SizeCompactionOptions,
    pub seek_compaction:    SeekCompactionOptions,
    pub write_throttling:   WriteThrottlingOptions,
    pub buffer_pool:        BufferPoolOptions<Pool>,
    pub cache:              CacheOptions,
}

#[derive(Debug)]
pub struct ReadOptions {
    /// Whether newly-read data blocks of SSTables should have their checksums verified.
    ///
    /// This **does not** affect what is read from the block cache. In particular, the checksum
    /// of a block already in the cache might not have been verified, but that cached block may
    /// be used even if this setting is `Some(true)`.
    ///
    /// `None` defers to the setting chosen in [`ConsistencyOptions`].
    ///
    /// Defaults to `None`.
    pub verify_data_checksums:  Option<bool>,
    /// Whether the index and metaindex blocks of newly-read SSTables should have their checksums
    /// verified.
    ///
    /// This **does not** affect what is read from the table cache. In particular, the index and
    /// metaindex checksums of an SSTable already in the table cache might not have been verified,
    /// but that cached table may be used even if this setting is `true`.
    ///
    /// `None` defers to the setting chosen in [`ConsistencyOptions`].
    ///
    /// Defaults to `None`.
    pub verify_index_checksums: Option<bool>,
    pub block_cache_usage:      CacheUsage,
    pub table_cache_usage:      CacheUsage,
    pub record_seeks:           bool,
    pub snapshot:               Option<Snapshot>,
    // TODO: error handler (with per-db default)
}

#[expect(missing_copy_implementations, reason = "will likely need to be `!Copy` in the future")]
#[derive(Debug)]
pub struct WriteOptions {
    // TODO: Some `ReadOptions` might need to be included here.
    pub sync: bool,
    // TODO: error handler (with per-db default)
}

/// Persistent database settings that readers and writers of a LevelDB database **must** agree on
/// for correctness rather than solely performance.
#[derive(Debug, Clone, Copy)]
pub struct FormatSettings<Cmp, Codecs> {
    comparator:            Cmp,
    compression_codecs:    Codecs,
    binary_log_block_size: BinaryLogBlockSize,
}

#[cfg(feature = "google-leveldb-compression")]
impl FormatSettings<BytewiseComparator, GoogleLevelDBCodecs> {
    /// Use the comparator and compression settings used by Google, the creators of LevelDB.
    #[inline]
    #[must_use]
    pub const fn google_leveldb_format() -> Self {
        let zstd_compression_level = GoogleLevelDBCodecs::DEFAULT_COMPRESSION_LEVEL;
        Self {
            comparator:            BytewiseComparator,
            compression_codecs:    GoogleLevelDBCodecs { zstd_compression_level },
            binary_log_block_size: BinaryLogBlockSize::DEFAULT,
        }
    }
}

#[cfg(feature = "google-leveldb-compression")]
impl<Cmp> FormatSettings<Cmp, GoogleLevelDBCodecs> {
    #[inline]
    #[must_use]
    pub const fn with_zstd_compression_level(mut self, compression_level: i32) -> Self {
        self.compression_codecs.zstd_compression_level = compression_level;
        self
    }
}

#[cfg(feature = "mojang-leveldb-compression")]
impl FormatSettings<BytewiseComparator, MojangLevelDBCodecs> {
    /// Use the comparator and compression settings used by Mojang's fork of LevelDB.
    #[inline]
    #[must_use]
    pub const fn mojang_leveldb_format() -> Self {
        Self {
            comparator:            BytewiseComparator,
            compression_codecs:    MojangLevelDBCodecs,
            binary_log_block_size: BinaryLogBlockSize::DEFAULT,
        }
    }
}

impl<Cmp, Codecs> FormatSettings<Cmp, Codecs> {
    /// Choose the persistent format settings of the LevelDB database.
    ///
    /// If you attempt to read the database with an incorrect choice of comparator, an error
    /// will be returned that indicates the name of the correct comparator.
    ///
    /// No metadata about the choice of compression codecs is stored, so making an incorrect
    /// choice of compression codecs may corrupt the database, or it make the database appear
    /// to be corrupted even if it could be read with the correct choice of compression codecs.
    #[inline]
    #[must_use]
    pub const fn from_cmp_and_unchecked_compression_codecs(
        comparator: Cmp,
        codecs:     Codecs,
    ) -> Self {
        Self {
            comparator,
            compression_codecs:    codecs,
            binary_log_block_size: BinaryLogBlockSize::DEFAULT,
        }
    }

    #[inline]
    #[must_use]
    pub const fn comparator(&self) -> &Cmp {
        &self.comparator
    }

    #[inline]
    #[must_use]
    pub const fn comparator_mut(&mut self) -> &mut Cmp {
        &mut self.comparator
    }

    #[inline]
    #[must_use]
    pub const fn compression_codecs(&self) -> &Codecs {
        &self.compression_codecs
    }

    #[inline]
    #[must_use]
    pub const fn compression_codecs_mut(&mut self) -> &mut Codecs {
        &mut self.compression_codecs
    }

    #[inline]
    #[must_use]
    pub const fn binary_log_block_size(&self) -> BinaryLogBlockSize {
        self.binary_log_block_size
    }

    /// The size of blocks in the binary log format used by `MANIFEST-_` manifest files and `_.log`
    /// write-ahead log files.
    ///
    /// Note that *every* reader and writer of a given LevelDB database need to use **the exact same
    /// value** for this block size, and all LevelDB databases produced by Google's LevelDB library
    /// use a hardcoded value equal to this setting's default. Mojang's LevelDB fork makes this
    /// value configurable, though appears to exclusively use the same setting.
    ///
    /// If an incorrect value is used, the database may be corrupted.
    ///
    /// Defaults to `1 << 15`. Not clamped.
    #[inline]
    pub const fn set_binary_log_block_size_unchecked(&mut self, block_size: BinaryLogBlockSize) {
        self.binary_log_block_size = block_size;
    }

    #[must_use]
    pub(crate) fn into_pieces(self) -> (Cmp, Codecs, BinaryLogBlockSize) {
        (self.comparator, self.compression_codecs, self.binary_log_block_size)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CompressionOptions {
    /// The type of compression to use when generating SSTable files from memtables.
    ///
    /// No default. This option can be dynamically changed while the database is running.
    pub memtable_compressor:            Option<CompressorId>,
    /// The type of compression to use for SSTable files produced by compactions in a nonzero level.
    ///
    /// The `i`-th compressor corresponds to level `i+1`.
    ///
    /// No default. This option can be dynamically changed while the database is running.
    pub table_compressors:              [Option<CompressorId>; NUM_NONZERO_LEVELS_USIZE.get()],
    /// Aim to compress the source data by at least `(compression_goal / 256) * 100%`, falling back
    /// to no compression if the goal cannot be met (in order to avoid wasting compute on
    /// decompression).
    ///
    /// Defaults to `32` (for a minimum of 12.5% compression, or no compression). Not clamped.
    /// This option can be dynamically changed while the database is running.
    pub memtable_compression_goal:      u8,
    /// On each respective nonzero level, aim to compress the source data by at least
    /// `(compression_goal / 256) * 100%`, falling back to no compression if the goal cannot be met
    /// (in order to avoid wasting compute on decompression).
    ///
    /// Defaults to `32` (for a minimum of 12.5% compression, or no compression). Not clamped.
    /// This option can be dynamically changed while the database is running.
    pub table_compression_goals:        [u8; NUM_NONZERO_LEVELS_USIZE.get()],
}

impl CompressionOptions {
    /// `(32 / 256) * 100% = 12.5%` compression.
    const DEFAULT_COMPRESSION_GOAL: u8 = 32;

    /// Use the same compressor for all generated SSTable files.
    ///
    /// Note that the lack of `Option` around the [`CompressorId`] intentionally add friction
    /// to choosing no compression, which is generally not the best choice.
    ///
    /// # Downstream Errors
    /// If this [`CompressorId`] is not supported by the chosen set of compression codecs, then
    /// an [`OptionsError`] may be returned.
    ///
    /// [`OptionsError`]: crate::all_errors::types::OptionsError
    #[inline]
    #[must_use]
    pub const fn from_compressor(compressor: CompressorId) -> Self {
        Self {
            memtable_compressor:       Some(compressor),
            table_compressors:         [Some(compressor); _],
            memtable_compression_goal: Self::DEFAULT_COMPRESSION_GOAL,
            table_compression_goals:   [Self::DEFAULT_COMPRESSION_GOAL; _],
        }
    }

    /// Use this compressor when generating SSTable files from memtables.
    ///
    /// # Downstream Errors
    /// If this [`CompressorId`] is not supported by the chosen set of compression codecs, then
    /// an [`OptionsError`] may be returned.
    ///
    /// [`OptionsError`]: crate::all_errors::types::OptionsError
    #[inline]
    #[must_use]
    pub const fn with_memtable_compressor(mut self, compressor: Option<CompressorId>) -> Self {
        self.memtable_compressor = compressor;
        self
    }

    /// If a certain level of a block of data in an SSTable cannot be compressed by
    /// `(compression_goal / 256) * 100%`, the block falls back to using no compression (in order
    /// to avoid wasting compute on decompression).
    ///
    /// This compression goal applies to SSTables generated from memtables.
    #[inline]
    #[must_use]
    pub const fn with_memtable_compression_goal(mut self, compression_goal: u8) -> Self {
        self.memtable_compression_goal = compression_goal;
        self
    }

    /// Use the `i`-th compressor when producing an SSTable file in level `i+1` during compactions.
    ///
    /// # Downstream Errors
    /// If any of these [`CompressorId`]s are not supported by the chosen set of compression codecs,
    /// then an [`OptionsError`] may be returned.
    ///
    /// [`OptionsError`]: crate::all_errors::types::OptionsError
    #[inline]
    #[must_use]
    pub const fn with_table_compressors(mut self, compressors: [Option<CompressorId>; 6]) -> Self {
        self.table_compressors = compressors;
        self
    }

    /// If a certain level of a block of data in an SSTable cannot be compressed by
    /// `(compression_goal / 256) * 100%`, the block falls back to using no compression (in order
    /// to avoid wasting compute on decompression).
    ///
    /// The `i`-th compression goal applies to SSTables generated in level `i+1` during compactions.
    #[inline]
    #[must_use]
    pub const fn with_table_compression_goals(mut self, compression_goals: [u8; 6]) -> Self {
        self.table_compression_goals = compression_goals;
        self
    }
}

// TODO: add link to `get` below.
//
/// Configuration for filters, which improve the performance of random-access reads (such
/// as calls to `get`).
///
/// Using a filter is not mandatory.
#[derive(Debug, Clone, Copy)]
pub struct FilterOptions<Policy> {
    // TODO: add link to `get` below.
    //
    /// The [`FilterPolicy`] to use for the database. Filters improve the performance of
    /// random-access reads, such as calls to `get`.
    ///
    /// When reading a database, any filters for other filter policies are ignored.
    ///
    /// LevelDB provides a default Bloom filter implementation.
    pub filter_policy:          Option<Policy>,
    /// One filter is generated per `1 << filter_chunk_size_log2` bytes of key data.
    ///
    /// Defaults to `11`. No clamping is performed.
    pub filter_chunk_size_log2: u8,
}

impl<Policy> FilterOptions<Policy> {
    /// By default, one filter is generated per 2048 bytes of key data.
    pub const DEFAULT_FILTER_CHUNK_SIZE_LOG2: u8 = 11;

    /// Use the given optional `filter_policy` and default values for other fields.
    #[inline]
    #[must_use]
    pub const fn from_filter_policy(filter_policy: Option<Policy>) -> Self {
        Self {
            filter_policy,
            filter_chunk_size_log2: Self::DEFAULT_FILTER_CHUNK_SIZE_LOG2,
        }
    }

    #[inline]
    #[must_use]
    pub fn with_filter_policy(mut self, filter_policy: Option<Policy>) -> Self {
        self.filter_policy = filter_policy;
        self
    }

    #[inline]
    #[must_use]
    pub const fn with_filter_chunk_size_log2(mut self, filter_chunk_size_log2: u8) -> Self {
        self.filter_chunk_size_log2 = filter_chunk_size_log2;
        self
    }
}

impl FilterOptions<BloomPolicy> {
    #[inline]
    #[must_use]
    pub const fn default_bloom_policy() -> Self {
        Self::from_filter_policy(Some(BloomPolicy::new(BloomPolicy::DEFAULT_BITS_PER_KEY)))
    }

    #[inline]
    #[must_use]
    pub const fn bloom_policy_with_bits(bits_per_key: u8) -> Self {
        Self::from_filter_policy(Some(BloomPolicy::new(bits_per_key)))
    }
}

impl<Policy: Default> Default for FilterOptions<Policy> {
    #[inline]
    fn default() -> Self {
        Self {
            filter_policy:          Some(Policy::default()),
            filter_chunk_size_log2: Self::DEFAULT_FILTER_CHUNK_SIZE_LOG2,
        }
    }
}

pub struct ConsistencyOptions<InvalidKey> {
    pub open_corruption_handler: Box<dyn OpenCorruptionHandler<InvalidKey>>,
    // TODO: corruption handler
    pub verify_data_checksums:   bool,
    pub verify_index_checksums:  bool,
    pub unwrap_poison:           bool,
    pub web_scale:               WebScale,
}

impl<InvalidKey> Debug for ConsistencyOptions<InvalidKey> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ConsistencyOptions")
            .field("open_corruption_handler", &"Box<dyn OpenCorruptionHandler<_>>")
            .field("verify_data_checksums",   &self.verify_data_checksums)
            .field("verify_index_checksums",  &self.verify_index_checksums)
            .field("unwrap_poison",           &self.unwrap_poison)
            .field("web_scale",               &self.web_scale)
            .finish()
    }
}

pub struct LoggerOptions {
    pub log_file_filter: LevelFilter,
    pub logger_filter:   LevelFilter,
    // TODO: dedicated `tracing` option that avoids `dyn`?
    pub custom_logger:   Option<Box<dyn Logger>>,
    //
    // TODO: support keeping more than one old log file.
    // /// If `old_logs_kept` is `1`, then the old log is stored as `LOG.old`.
    // ///
    // /// Otherwise, an `OLD-LOGS` directly is created, storing log files of the form
    // /// `LOG-XXXXXX.old`.
    // pub old_logs_kept: NonZeroU32,
}

impl Debug for LoggerOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let custom_logger = if self.custom_logger.is_some() {
            "Some(<Box<dyn Logger>>)"
        } else {
            "None"
        };

        f.debug_struct("LoggerOptions")
            .field("log_file_filter", &self.log_file_filter)
            .field("logger_filter",   &self.logger_filter)
            .field("custom_logger",   &custom_logger)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ManifestOptions {
    /// If the most-recent `MANIFEST-XXXXXX` file has at most the indicated size, it will be reused
    /// when the database is opened.
    ///
    /// As a special case, manifest files are never reused if this option is zero.
    pub max_reused_manifest_size: FileSize,
}

#[derive(Debug, Clone, Copy)]
pub struct MemtableOptions {
    /// An approximate limit on the size of in-memory "memtable" write buffers that store values
    /// before they are written to SSTables.
    ///
    /// Once a memtable reaches approximately this size, it is flushed to an SSTable. The database
    /// keeps one active memtable at all times, and may keep a second older memtable while the older
    /// memtable is being flushed to an SSTable. Additionally, old reference-counted memtables may
    /// be kept alive by long-living database iterators.
    ///
    /// Defaults to 4 MiB in order to match Google's default. However, larger values are very
    /// beneficial. Not clamped by default, but if [`BackwardsCompatibilityClamping`] is enabled,
    /// the limit is clamped to between 64 KiB and 1 GiB (`64 << 10` and `1 << 30`).
    ///
    /// [`BackwardsCompatibilityClamping`]: ClampOptions::BackwardsCompatibilityClamping
    pub max_memtable_size:         usize,
    pub initial_memtable_capacity: usize,
    pub max_write_log_file_size:   FileSize,
    /// If the most-recent `XXXXXX.log` file has at most the indicated size, it will be reused
    /// when the database is opened.
    ///
    /// As a special case, log files are never reused if this option is zero.
    pub max_reused_write_log_size: FileSize,
    pub memtable_pool_size:        NonZeroU8,
}

#[derive(Debug, Clone, Copy)]
pub struct SSTableOptions {
    /// Approximate limits for the size of SSTable files generated by compactions in nonzero levels.
    ///
    /// Once the limit is reached, no additional user data will be added to the SSTable. The `i`-th
    /// limit applies to files in level `i+1`.
    ///
    /// Note that the limits do not apply to SSTable files produced from memtables.
    ///
    /// Defaults to 2 MiB on every level. Not clamped by default, but if
    /// [`BackwardsCompatibilityClamping`] is enabled, each max table size is clamped to between
    /// 1 MiB and 1 GiB (`1 << 20` and `1 << 30`).
    ///
    /// If this value is changed, then [`max_compaction_inputs`] [`max_grandparent_overlap`],
    /// and [`max_level_sizes`] should likely be adjusted as well.
    ///
    /// [`BackwardsCompatibilityClamping`]: ClampOptions::BackwardsCompatibilityClamping
    /// [`max_compaction_inputs`]: CompactionOptions::max_compaction_inputs
    /// [`max_grandparent_overlap`]: CompactionOptions::max_grandparent_overlap
    /// [`max_level_sizes`]: SizeCompactionOptions::max_level_sizes
    pub max_sstable_sizes:      [FileSize; NUM_NONZERO_LEVELS_USIZE.get()],
    /// Approximate size of uncompressed user data per block of an SSTable.
    ///
    /// Once this limit is exceeded, a block will be compressed and added to the SSTable.
    ///
    /// Defaults to 4 KiB in order to match Google's default. However, larger values are very
    /// beneficial. Not clamped by default, but if [`BackwardsCompatibilityClamping`] is enabled,
    /// the block size is clamped to between 1 KiB and 4 MiB (`1 << 10` and `4 << 20`). This option
    /// can be dynamically changed while the database is running. (The dynamic values are not
    /// clamped.)
    ///
    /// [`BackwardsCompatibilityClamping`]: ClampOptions::BackwardsCompatibilityClamping
    pub sstable_block_size:     usize,
    /// Number of keys between restart points for delta encoding of keys.
    ///
    /// Except at restart points, keys in an SSTable block store the difference from the previous
    /// key, for the sake of improving space efficiency. Restart points are added to allow
    /// iterators to quickly seek through an SSTable block.
    ///
    /// Defaults to 16. Not clamped. This option can be dynamically changed while the database is
    /// running. (The dynamic values are not clamped.)
    pub block_restart_interval: NonZeroU32,
    // TODO: Could add a `sst_bytes_per_sync`, like what RocksDB has. However, it'd be best to
    // get benchmarks to motivate such a setting and its default value.
}

/// Options for configuring compactions (other than options specific to size or seek compactions),
/// which improve read performance and reduce the memory consumed by the database.
#[derive(Debug, Clone, Copy)]
pub struct CompactionOptions {
    /// Whether compactions should be performed in a background thread (rather than on foreground
    /// user threads as necessary).
    ///
    /// Defaults to `true`.
    pub compact_in_background:        bool,
    /// The highest level that an SSTable generated from a memtable could be placed in. Memtables
    /// can skip past some levels, if possible, in order to reduce the number of compactions needed
    /// in the database.
    ///
    /// Defaults to `2`. Not clamped.
    pub max_level_for_memtable_flush: Level,
    /// An approximate limit on the number of bytes taken as input (from files in levels `n-1` and
    /// `n`) to a compaction into level `n`.
    ///
    /// The set of input files to a compaction may be expanded if the expanded compaction would not
    /// exceed this limit. The `i`-th limit corresponds to level `i+1`.
    ///
    /// Defaults to 10 MiB.
    pub max_compaction_inputs:        [u64; NUM_NONZERO_LEVELS_USIZE.get()],
    /// An approximate limit on the number of level-`n+1` files that a compaction from level `n-1`
    /// into level `n` overlaps with.
    ///
    /// The set of input files to a compaction may be expanded if the expanded compaction would not
    /// exceed this limit. The `i`-th limit corresponds to level `i+1`.
    ///
    /// Defaults to 10 MiB.
    pub max_grandparent_overlap:      [u64; NUM_MIDDLE_LEVELS_USIZE.get()],
}

#[derive(Debug, Clone, Copy)]
pub struct SizeCompactionOptions {
    pub autocompact_level_zero:     bool,
    pub autocompact_nonzero_levels: bool,
    pub max_level0_files:           NonZeroU16,
    pub max_level_sizes:            [u64; NUM_MIDDLE_LEVELS_USIZE.get()],
}

// TODO: add link to `get` below.
//
/// Options for configuring automatic seek compactions, which attempt to compact frequently-used
/// files.
///
/// # Seeks
/// If automatic compactions are enabled, one seek is recorded per unnecessary random access to a
/// file (such as when a call to `get` needs to read more than one file). Iterators also
/// pseudorandomly record file seeks approximately once every `iter_sample_period` bytes of user
/// data they read.
///
/// # Tuning
/// If enabled, the three settings for seek compactions should balance the cost of compactions
/// against the cost of additional file seeks.
///
/// For example, if the file size options are chosen such that level `n+1` contains
/// roughly ten times as much data as level `n`, if we assume:
/// - One file seek costs 10 milliseconds,
/// - Writing or reading 1 MiB costs 10 milliseconds (100 MiB/s), and
/// - A compaction of 1 MiB does approximately 25 MiB of IO:
///   - 1 MiB from level `n`
///   - 10-12 MiB read from level `n+1` (file boundaries may be misaligned)
///   - 10-12 MiB written to level `n+1`
///
/// then 25 file seeks cost approximately the same as a 1 MiB compaction, meaning that
/// 1 seek is equivalent to the cost of compacting 40 KiB of data.
///
/// The default settings, inherited from Google's `leveldb`, allow a greater amount of seeks
/// (1 seek per 16 KiB of data).
#[derive(Debug, Clone, Copy)]
pub struct SeekCompactionOptions {
    /// Whether automatic seek compactions, which attempt to compact frequently-used files,
    /// should be performed.
    ///
    /// Enabling seek compactions requires a slight amount of additional tracking.
    ///
    /// Defaults to `true`.
    pub seek_autocompactions: bool,
    /// Used to calculate how many times an unnecessary read to a file must occur before an
    /// automatic seek compaction may be triggered on that file (if enabled).
    ///
    /// Larger files permit a greater number of seeks before a compaction (as compaction is more
    /// expensive for larger files). The size-based limit is clamped to
    /// `min_allowed_seeks..=u32::MAX/2`, with the `u32::MAX/2` maximum taking priority over
    /// the provided `min_allowed_seeks` minimum option.
    ///
    /// Defaults to 100. No clamping is performed, though all values greater than or equal to
    /// `u32::MAX/2` behave the same as `u32::MAX/2`.
    pub min_allowed_seeks: u32,
    /// Used to calculate how many times an unnecessary read to a file must occur before an
    /// automatic seek compaction may be triggered on that file.
    ///
    /// Larger files permit a greater number of seeks before a compaction (as compaction is more
    /// expensive for larger files); one additional seek is permitted per `file_bytes_per_seek`
    /// bytes. The size-based limit is clamped to `min_allowed_seeks..=u32::MAX/2`.
    ///
    /// Defaults to 16 KiB. No clamping is performed.
    pub file_bytes_per_seek: NonZeroU32,
    /// In order to encourage the compaction of frequently-accessed files, iteration through the
    /// database will record a file seek approximately once every `iter_sample_period` bytes read
    /// (if automatic seek compactions are enabled).
    ///
    /// Defaults to 1 MiB. Always clamped to at most `u32::MAX/2`, for the sake of correctness
    /// in the implementation.
    pub iter_sample_period: u32,
}

impl SeekCompactionOptions {
    const DEFAULT_MIN_ALLOWED_SEEKS: u32 = 100;
    #[allow(clippy::unwrap_used, reason = "validated at compile time")]
    const DEFAULT_FILE_BYTES_PER_SEEK: NonZeroU32 = NonZeroU32::new(16 << 10_u8).unwrap();
    const DEFAULT_ITER_SEEK_PERIOD: u32 = 1 << 20;

    #[inline]
    #[must_use]
    pub const fn enabled() -> Self {
        Self {
            seek_autocompactions: true,
            min_allowed_seeks:    Self::DEFAULT_MIN_ALLOWED_SEEKS,
            file_bytes_per_seek:  Self::DEFAULT_FILE_BYTES_PER_SEEK,
            iter_sample_period:   Self::DEFAULT_ITER_SEEK_PERIOD,
        }
    }

    #[inline]
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            seek_autocompactions: false,
            min_allowed_seeks:    Self::DEFAULT_MIN_ALLOWED_SEEKS,
            file_bytes_per_seek:  Self::DEFAULT_FILE_BYTES_PER_SEEK,
            iter_sample_period:   Self::DEFAULT_ITER_SEEK_PERIOD,
        }
    }
}

impl Default for SeekCompactionOptions {
    #[inline]
    fn default() -> Self {
        Self::enabled()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WriteThrottlingOptions {
    pub level0_write_throttle_trigger: NonZeroU16,
    pub throttle_sleep_duration:       Duration,
    pub level0_write_halt_trigger:     NonZeroU16,
    // TODO: write_thread_adaptive_yield - RocksDB has this option to try to improve the
    // performance of what I'd call the "contention queue".
}

#[derive(Debug, Clone, Copy)]
pub struct BufferPoolOptions<Pool> {
    pub buffer_pool: Pool,
}

#[derive(Debug, Clone, Copy)]
pub struct CacheOptions {
    /// The capacity (in bytes) of the cache for uncompressed SSTable blocks.
    ///
    /// Defaults to 8 MiB in order to match the default of Google's LevelDB.
    pub block_cache_size:     u64,
    /// An estimate for the average size of uncompressed SSTable blocks, used to improve the
    /// performance of the block cache.
    pub average_block_size:   NonZeroUsize,
    // TODO: add link to `get` below.
    /// The maxmimum number of SSTables to cache in the table cache.
    ///
    /// Note that each cached SSTable keeps the corresponding SSTable file open. If you wish to
    /// limit the total number of files opened by `anchored-leveldb`, keep the following in mind:
    /// - At any time, no more than `10` non-SSTable files are kept open by the database.
    ///   (This limit does not constitute an entirely stable guarantee, but currently seems unlikely
    ///   to be exceeded.)
    /// - Each database iterator may open `6 + num_level0_files` SSTable files, where
    ///   `num_level0_files` is the number of level-0 files at the time the iterator is created.
    ///   That number is most influenced by [`SizeCompactionOptions`] and
    ///   [`WriteThrottlingOptions`].
    /// - Most methods of reading the database (including `get`) internally create a database
    ///   iterator.
    /// - There could (and, generally, *should*) be a large overlap between the SSTable files
    ///   kept open by database iterators and the table cache. That is, a single open SSTable file
    ///   may be in the table cache and be used by multiple database iterators.
    /// - Reads to the database can choose to bypass the table cache, which could cause one
    ///   SSTable file to be opened multiple times by different database iterators, forcing low
    ///   overlap.
    ///
    /// Defaults to 1000 in order to approximately match Google's default. Not clamped by default,
    /// but if [`BackwardsCompatibilityClamping`] is enabled, the capacity is clamped to between
    /// 54 and 49990.
    ///
    /// [`BackwardsCompatibilityClamping`]: ClampOptions::BackwardsCompatibilityClamping
    pub table_cache_capacity: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum ClampOptions {
    /// Do not clamp numeric options to reasonable values.
    NoClamping,
    /// Clamp options in the same way as Google's leveldb library.
    BackwardsCompatibilityClamping,
}

#[derive(Debug, Clone, Copy)]
pub enum WebScale {
    /// Unleash the power of a [web scale](https://www.youtube.com/watch?v=b2F-DItXtZs)
    /// database to get some kickass benchmark numbers.
    ///
    /// `/dev/null` support not included.
    WebScale,
    /// Ensure that written SSTables are properly persisted.
    NotWebScale,
}

#[derive(Debug, Clone, Copy)]
pub enum CacheUsage {
    ReadAndFill,
    Read,
    Ignore,
}
