use std::array;
use std::{path::PathBuf, time::Duration};
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    num::{NonZeroU8, NonZeroU16, NonZeroU32, NonZeroUsize},
};

// TODO: create own enum.
use tracing::level_filters::LevelFilter;

use crate::snapshot::Snapshot;
use crate::{
    pub_traits::{
        compression::CompressorId,
        cmp_and_policy::{BloomPolicy, LevelDBComparator},
        error_handler::{DefaultOpenHandler, DefaultOpenHandlerOptions, OpenCorruptionHandler},
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


// TODO: Make it convenient to change all the file size settings at once.
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
    /// Defaults to `ReadAndFill`.
    pub block_cache_usage:      CacheUsage,
    /// Defaults to `ReadAndFill`.
    pub table_cache_usage:      CacheUsage,
    /// This setting is ignored if automatic seek compactions are disabled.
    ///
    /// Defaults to `true`.
    pub record_seeks:           bool,
    /// Defaults to `None`.
    pub snapshot:               Option<Snapshot>,
    // TODO: error handler (with per-db default)
}

impl Default for ReadOptions {
    #[inline]
    fn default() -> Self {
        Self {
            verify_data_checksums:  None,
            verify_index_checksums: None,
            block_cache_usage:      CacheUsage::ReadAndFill,
            table_cache_usage:      CacheUsage::ReadAndFill,
            record_seeks:           true,
            snapshot:               None,
        }
    }
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
        const LEN: usize = NUM_NONZERO_LEVELS_USIZE.get();
        Self {
            memtable_compressor:       Some(compressor),
            table_compressors:         [Some(compressor); LEN],
            memtable_compression_goal: Self::DEFAULT_COMPRESSION_GOAL,
            table_compression_goals:   [Self::DEFAULT_COMPRESSION_GOAL; LEN],
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
    pub open_corruption_handler: Box<dyn OpenCorruptionHandler<InvalidKey> + Send + Sync>,
    // TODO: corruption handler
    pub verify_data_checksums:   bool,
    pub verify_index_checksums:  bool,
    /// While `DB` and `DBState` take substantial measures to avoid causing hangs or leaking
    /// resources when they panic (and, necessarily, do not enable panics to cause unsoundness), it
    /// is **not** guaranteed that all their logical invariants are preserved when an unwind occurs.
    ///
    /// Therefore, using `unwrap_poison = false` is **highly** discouraged. This option is mostly
    /// provided for completeness, and since (depending on settings and usage) it may be
    /// possible to *guarantee* that the database never automatically writes anything to persistent
    /// storage, in which case read-only access to the database may be permissible even after
    /// an unwind. Do so at your own risk.
    pub unwrap_poison:           bool,
    pub web_scale:               WebScale,
}

impl<InvalidKey: Send + Sync + 'static> ConsistencyOptions<InvalidKey> {
    #[must_use]
    pub fn paranoid_with_default_handler(try_reuse_files: bool) -> Self {
        Self {
            open_corruption_handler: Box::new(DefaultOpenHandler::new(DefaultOpenHandlerOptions {
                verify_recovered_version:    true,
                allow_final_truncated_entry: false,
                try_reuse_manifest:          try_reuse_files,
                try_reuse_write_ahead_log:   try_reuse_files,
            })),
            verify_data_checksums:   true,
            verify_index_checksums:  true,
            unwrap_poison:           true,
            web_scale:               WebScale::NotWebScale,
        }
    }

    #[must_use]
    pub fn verify_with_default_handler(try_reuse_files: bool) -> Self {
        Self {
            open_corruption_handler: Box::new(DefaultOpenHandler::new(DefaultOpenHandlerOptions {
                verify_recovered_version:    true,
                allow_final_truncated_entry: true,
                try_reuse_manifest:          try_reuse_files,
                try_reuse_write_ahead_log:   try_reuse_files,
            })),
            verify_data_checksums:   true,
            verify_index_checksums:  true,
            unwrap_poison:           true,
            web_scale:               WebScale::NotWebScale,
        }
    }

    #[must_use]
    pub fn permissive_with_default_handler(try_reuse_files: bool) -> Self {
        Self {
            open_corruption_handler: Box::new(DefaultOpenHandler::new(DefaultOpenHandlerOptions {
                verify_recovered_version:    false,
                allow_final_truncated_entry: true,
                try_reuse_manifest:          try_reuse_files,
                try_reuse_write_ahead_log:   try_reuse_files,
            })),
            verify_data_checksums:   false,
            verify_index_checksums:  false,
            unwrap_poison:           true,
            web_scale:               WebScale::NotWebScale,
        }
    }

    /// Unleash the power of a [web scale](https://www.youtube.com/watch?v=b2F-DItXtZs)
    /// database to get some kickass benchmark numbers.
    ///
    /// (Do not seriously use this setting.)
    #[must_use]
    pub fn web_scale() -> Self {
        Self {
            open_corruption_handler: Box::new(DefaultOpenHandler::new(DefaultOpenHandlerOptions {
                verify_recovered_version:    false,
                allow_final_truncated_entry: true,
                try_reuse_manifest:          true,
                try_reuse_write_ahead_log:   true,
            })),
            verify_data_checksums:   false,
            verify_index_checksums:  false,
            unwrap_poison:           true,
            web_scale:               WebScale::WebScale,
        }
    }

    /// Almost as web scale as `/dev/null`.
    ///
    /// (Do not seriously use this setting.)
    #[must_use]
    pub fn extra_web_scale() -> Self {
        let mut this = Self::web_scale();
        this.unwrap_poison = false;
        this
    }
}

impl<InvalidKey: Send + Sync + 'static> Default for ConsistencyOptions<InvalidKey> {
    /// Roughly equivalent to the default used by Google's LevelDB, except filter blocks'
    /// checksums are always checked. (Corrupted filter blocks are discarded, rather than resulting
    /// in hard errors.)
    #[inline]
    fn default() -> Self {
        Self::permissive_with_default_handler(false)
    }
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
    pub custom_logger:   Option<Box<dyn Logger + Send + Sync>>,
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
    ///
    /// Defaults to 2 MiB.
    pub max_reused_manifest_size: FileSize,
}

impl Default for ManifestOptions {
    #[inline]
    fn default() -> Self {
        Self {
            max_reused_manifest_size: FileSize(1 << 20_u8),
        }
    }
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
    /// If this value is changed, then [`initial_memtable_capacity`], [`max_write_log_file_size`],
    /// and [`max_reused_write_log_size`] hould likely be adjusted as well.
    ///
    /// [`initial_memtable_capacity`]: MemtableOptions::initial_memtable_capacity
    /// [`max_write_log_file_size`]: MemtableOptions::max_write_log_file_size
    /// [`max_reused_write_log_size`]: MemtableOptions::max_reused_write_log_size
    ///
    /// [`BackwardsCompatibilityClamping`]: ClampOptions::BackwardsCompatibilityClamping
    pub max_memtable_size:         usize,
    /// The in-memory write buffer is initially given this capacity.
    ///
    /// Defaults to 4.25 MiB.
    pub initial_memtable_capacity: usize,
    /// Prevent the write-ahead log from undergoing unbounded growth when a large number of empty
    /// write batches are written to it.
    ///
    /// Defaults to 8 MiB.
    pub max_write_log_file_size:   FileSize,
    /// If the most-recent `XXXXXX.log` file has at most the indicated size, it will be reused
    /// when the database is opened.
    ///
    /// This is primarily used to prevent the write-ahead log from undergoing unbounded growth when
    /// a large number of empty write batches are written to it.
    ///
    /// As a special case, log files are never reused if this option is zero, though that behavior
    /// can also be controlled via the `open_corruption_handler`.
    ///
    /// Defaults to 8 MiB.
    pub max_reused_write_log_size: FileSize,
    /// Efficiently reuse memtable buffers.
    ///
    /// The database keeps one active memtable at all times, and may keep a second older memtable
    /// while that older memtable is being flushed to an SSTable. However, when a memtable finishes
    /// being flushed, it might not be able to be immediately reused; database iterators hold
    /// reference counts on memtables, which can prevent old memtables from being reset. Instead,
    /// whenever a memtable has no active reference counts, it is returned to a pool for later
    /// reuse.
    ///
    /// An unbounded number of unused buffers could be produced by dropping a large number of
    /// database iterators which were keeping old memtables alive; at most `memtable_pool_size`
    /// unused memtable buffers are kept around.
    ///
    /// Defaults to 4.
    pub memtable_pool_size:        NonZeroU8,
}

impl Default for MemtableOptions {
    #[inline]
    fn default() -> Self {
        #[allow(clippy::unwrap_used, reason = "validated at compile time")]
        let four = const { NonZeroU8::new(4).unwrap() };
        Self {
            max_memtable_size:         4 << 20_u8,
            initial_memtable_capacity: (4 << 20_u8) + (4 << 16_u8),
            max_write_log_file_size:   FileSize(8 << 20_u8),
            max_reused_write_log_size: FileSize(8 << 20_u8),
            memtable_pool_size:        four,
        }
    }
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
    /// If this value is changed, then [`max_reused_manifest_size`], [`max_compaction_inputs`],
    /// [`max_grandparent_overlap`], and [`max_level_sizes`] should likely be adjusted as well.
    ///
    /// [`max_reused_manifest_size`]: ManifestOptions::max_reused_manifest_size
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
    /// If this value is changed, [`average_block_size`] should be as well. (To begin with,
    /// [`average_block_size`] is a rough estimate.)
    ///
    /// [`average_block_size`]: CacheOptions::average_block_size
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

impl Default for SSTableOptions {
    #[inline]
    fn default() -> Self {
        const LEN: usize = NUM_NONZERO_LEVELS_USIZE.get();
        #[allow(clippy::unwrap_used, reason = "validated at compile time")]
        let sixteen = const { NonZeroU32::new(16).unwrap() };

        Self {
            max_sstable_sizes:      [FileSize(1 << 20_u8); LEN],
            sstable_block_size:     4 << 10_u8,
            block_restart_interval: sixteen,
        }
    }
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
    /// Defaults to 25 MiB.
    pub max_compaction_inputs:        [u64; NUM_NONZERO_LEVELS_USIZE.get()],
    /// An approximate limit (in bytes) on the amount of level-`n+1` data that a single output file
    /// on level `n` will overlap with (during a compaction from level `n-1` into level `n`).
    ///
    /// The set of input files to a compaction may be expanded if the expanded compaction would not
    /// exceed this limit. The `i`-th limit corresponds to level `i+1`.
    ///
    /// Defaults to 10 MiB.
    pub max_grandparent_overlap:      [u64; NUM_MIDDLE_LEVELS_USIZE.get()],
}

impl Default for CompactionOptions {
    #[inline]
    fn default() -> Self {
        const LEN1: usize = NUM_NONZERO_LEVELS_USIZE.get();
        const LEN2: usize = NUM_MIDDLE_LEVELS_USIZE.get();

        #[allow(clippy::unwrap_used, reason = "validated at compile time")]
        let level_2 = const { Level::new(2).unwrap() };

        Self {
            compact_in_background:        true,
            max_level_for_memtable_flush: level_2,
            max_compaction_inputs:        [25 << 20_u8; LEN1],
            max_grandparent_overlap:      [10 << 20_u8; LEN2],
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SizeCompactionOptions {
    /// Defaults to `true`.
    pub autocompact_level_zero:     bool,
    /// Defaults to `true`.
    pub autocompact_nonzero_levels: bool,
    /// Defaults to `4`.
    pub max_level0_files:           NonZeroU16,
    /// Defaults to 10 MiB for level 1, increasing by a factor of 10 for each higher level.
    pub max_level_sizes:            [u64; NUM_MIDDLE_LEVELS_USIZE.get()],
}

impl SizeCompactionOptions {
    #[inline]
    #[must_use]
    fn default_level_limits() -> (NonZeroU16, [u64; NUM_MIDDLE_LEVELS_USIZE.get()]) {
        #[allow(clippy::unwrap_used, reason = "validated at compile time")]
        let four = const { NonZeroU16::new(4).unwrap() };

        let mut max_level_size = 1 << 20_u8;
        let max_level_sizes = array::from_fn(|_idx| {
            max_level_size *= 10;
            max_level_size
        });

        (four, max_level_sizes)
    }

    #[inline]
    #[must_use]
    pub fn enabled() -> Self {
        let (max_level0_files, max_level_sizes) = Self::default_level_limits();
        Self {
            autocompact_level_zero:     true,
            autocompact_nonzero_levels: true,
            max_level0_files,
            max_level_sizes
        }
    }

    #[inline]
    #[must_use]
    pub fn disabled() -> Self {
        let (max_level0_files, max_level_sizes) = Self::default_level_limits();
        Self {
            autocompact_level_zero:     false,
            autocompact_nonzero_levels: false,
            max_level0_files,
            max_level_sizes
        }
    }
}

impl Default for SizeCompactionOptions {
    #[inline]
    fn default() -> Self {
        #[allow(clippy::unwrap_used, reason = "validated at compile time")]
        let four = const { NonZeroU16::new(4).unwrap() };

        let mut max_level_size = 1 << 20_u8;
        let max_level_sizes = array::from_fn(|_idx| {
            max_level_size *= 10;
            max_level_size
        });

        Self {
            autocompact_level_zero:     true,
            autocompact_nonzero_levels: true,
            max_level0_files:           four,
            max_level_sizes,
        }
    }
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
    /// Defaults to 8.
    pub level0_write_throttle_trigger: NonZeroU16,
    /// Defaults to 1 second.
    pub throttle_sleep_duration:       Duration,
    /// Defaults to 12.
    pub level0_write_halt_trigger:     NonZeroU16,
    // TODO: write_thread_adaptive_yield - RocksDB has this option to try to improve the
    // performance of what I'd call the "contention queue".
}

impl Default for WriteThrottlingOptions {
    #[inline]
    fn default() -> Self {
        #[allow(clippy::unwrap_used, reason = "validated at compile time")]
        let eight = const { NonZeroU16::new(8).unwrap() };
        #[allow(clippy::unwrap_used, reason = "validated at compile time")]
        let twelve = const { NonZeroU16::new(12).unwrap() };

        Self {
            level0_write_throttle_trigger: eight,
            throttle_sleep_duration:       Duration::from_secs(1),
            level0_write_halt_trigger:     twelve,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BufferPoolOptions<Pool> {
    /// Defaults to `Pool`'s default.
    pub buffer_pool:                Pool,
    /// Database iterators internally use two `Vec<u8>` buffers. For each of them, if the buffer
    /// exceeds `iter_buffer_capacity_limit` in capacity, it will not be reused.
    ///
    /// Defaults to 1 MiB.
    pub iter_buffer_capacity_limit: usize,
}

impl<Pool: Default> Default for BufferPoolOptions<Pool> {
    #[inline]
    fn default() -> Self {
        Self {
            buffer_pool:                Pool::default(),
            iter_buffer_capacity_limit: 1 << 20_u8,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CacheOptions {
    /// The capacity (in bytes) of the cache for uncompressed SSTable blocks.
    ///
    /// Defaults to 8 MiB in order to match the default of Google's LevelDB. Larger values are
    /// very beneficial.
    pub block_cache_size:     u64,
    /// An estimate for the average size of uncompressed SSTable blocks, used to improve the
    /// performance of the block cache.
    ///
    /// Defaults to 8 KiB.
    pub average_block_size:   NonZeroUsize,
    // TODO: add link to `get` below.
    /// The maximum number of SSTables to cache in the table cache.
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

impl Default for CacheOptions {
    #[inline]
    fn default() -> Self {
        #[allow(clippy::unwrap_used, reason = "validated at compile time")]
        let eight_kb = const { NonZeroUsize::new(8 << 10_u8).unwrap() };
        Self {
            block_cache_size:     8 << 20_u8,
            average_block_size:   eight_kb,
            table_cache_capacity: 1000,
        }
    }
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
