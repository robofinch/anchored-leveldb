/// The maximum number of levels in the LevelDB database.
pub(crate) const NUM_LEVELS: u8 = 7;
/// The maximum number of levels in the LevelDB database, as a usize.
#[expect(clippy::as_conversions, reason = "`From` conversions do not yet work in const")]
pub(crate) const NUM_LEVELS_USIZE: usize = NUM_LEVELS as usize;
/// The maximum level which a level-0 file may be compacted to
pub(crate) const MAX_LEVEL_FOR_COMPACTION: u8 = 2;

/// Once there are [`L0_COMPACTION_TRIGGER`]-many level-0 files, size compactions may target
/// level 0.
pub(crate) const L0_COMPACTION_TRIGGER: u8 = 4;
/// Once there are [`L0_SOFT_FILE_LIMIT`]-many level-0 files, writes are slowed down
/// in order to let compactions catch up.
pub(crate) const L0_SOFT_FILE_LIMIT: u8 = 8;
/// Once there are [`L0_HARD_FILE_LIMIT`]-many level-0 files, writes are entirely stopped
/// in order to let compactions catch up.
pub(crate) const L0_HARD_FILE_LIMIT: u8 = 12;

// Note that the maximum size per file is configurable, but the maximum size per level is not.

/// Once level-1 files have a total file size exceeding [`MAX_BYTES_FOR_L1`], size compactions
/// may target level 1.
#[expect(clippy::as_conversions, reason = "`From` conversions do not yet work in const")]
pub(crate) const MAX_BYTES_FOR_L1: f64 = (1_u32 << 20_u8) as f64 * MAX_BYTES_MULTIPLIER;
/// Once level-(`n+1`) files have a total file size exceeding [`MAX_BYTES_MULTIPLIER`] times
/// the max bytes limit of level `n`, size compactions may target level `n+1`.
pub(crate) const MAX_BYTES_MULTIPLIER: f64 = 10.0;

/// For a given `file_size_limit` setting, a file being built in a compaction from level `n` to
/// level `n+1` will stop being built if the file's overlapping grandparents in level `n+2`
/// reach a total size of <code>[GRANDPARENT_OVERLAP_SIZE_FACTOR] * file_size_limit</code> bytes.
pub(crate) const GRANDPARENT_OVERLAP_SIZE_FACTOR: u64 = 10;
/// For a given `file_size_limit` setting, a compaction from level `n` to level `n+1` will not be
/// expanded if the total file size of input files for the compaction, across both levels,
/// would exceed <code>[EXPANDED_COMPACTION_SIZE_FACTOR] * file_size_limit</code> bytes
/// after expansion.
pub(crate) const EXPANDED_COMPACTION_SIZE_FACTOR: u64 = 25;

/// The maximum value for the `file_size_limit` setting. This number was chosen to ensure that
/// <code>[GRANDPARENT_OVERLAP_SIZE_FACTOR] * file_size_limit</code> and
/// <code>[EXPANDED_COMPACTION_SIZE_FACTOR] * file_size_limit</code> do not overflow.
pub(crate) const MAXIMUM_FILE_SIZE_LIMIT: u64 = 1 << 59;

/// The block size for the log format used by `MANIFEST-_` files and write-ahead logs
/// (`_.log` files).
pub(crate) const WRITE_LOG_BLOCK_SIZE: usize = 1 << 15;
