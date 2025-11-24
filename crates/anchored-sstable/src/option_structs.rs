use std::num::NonZeroUsize;
use std::fmt::{Debug, Formatter, Result};

use clone_behavior::{Fast, MirroredClone, Speed};
use generic_container::FragileContainer;

use crate::{compressors::CompressorList, pool::BufferPool};
use crate::caches::{BlockCacheKey, KVCache};

// TODO: provide builders and/or defaults


/// # Policy-Comparator Compatibility
///
/// The [`TableFilterPolicy`] and [`TableComparator`] of a [`Table`] must be compatible; in
/// particular, the [`TableFilterPolicy`] value (if `Some`) must ensure that generated filters
/// match not only the exact keys for which the filter was generated, but also any key which
/// compares equal to a key the filter was generated for. This matters if the equivalence relation
/// of the [`TableComparator`] is looser than strict equality; that is, if bytewise-distinct keys
/// may compare as equal.
///
/// [`Table`]: crate::table::Table
/// [`TableFilterPolicy`]: crate::filters::TableFilterPolicy
/// [`TableComparator`]: crate::comparator::TableComparator
#[derive(Clone)]
pub struct ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool> {
    pub compressor_list:  CompList,
    pub filter_policy:    Option<Policy>,
    pub comparator:       TableCmp,
    pub verify_checksums: bool,
    pub block_cache:      Cache,
    pub buffer_pool:      Pool,
}

impl<CompList, Policy, TableCmp, Cache, Pool>
    ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool>
where
    CompList: MirroredClone<Fast>,
    Policy:   MirroredClone<Fast>,
    TableCmp: MirroredClone<Fast>,
    Cache:    MirroredClone<Fast>,
    Pool:     MirroredClone<Fast>,
{
    #[inline]
    #[must_use]
    pub fn fast_clone(&self) -> Self {
        Self {
            compressor_list:  self.compressor_list.fast_mirrored_clone(),
            filter_policy:    self.filter_policy.as_ref().map(Policy::fast_mirrored_clone),
            comparator:       self.comparator.fast_mirrored_clone(),
            verify_checksums: self.verify_checksums,
            block_cache:      self.block_cache.fast_mirrored_clone(),
            buffer_pool:      self.buffer_pool.fast_mirrored_clone(),
        }
    }
}

impl<CompList, Policy, TableCmp, Cache, Pool> Debug
for ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool>
where
    CompList:           FragileContainer<CompressorList>,
    Policy:             Debug,
    TableCmp:           Debug,
    Cache:              KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:               Debug + BufferPool,
    Pool::PooledBuffer: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("ReadTableOptions")
            .field("compressor_list",  &*self.compressor_list.get_ref())
            .field("filter_policy",    &self.filter_policy)
            .field("comparator",       &self.comparator)
            .field("verify_checksums", &self.verify_checksums)
            .field("block_cache",      KVCache::debug(&self.block_cache))
            .field("buffer_pool",      &self.buffer_pool)
            .finish()
    }
}


/// # Policy-Comparator Compatibility
///
/// The [`TableFilterPolicy`] and [`TableComparator`] of a [`Table`] must be compatible; in
/// particular, the [`TableFilterPolicy`] value (if `Some`) must ensure that generated filters
/// match not only the exact keys for which the filter was generated, but also any key which
/// compares equal to a key the filter was generated for. This matters if the equivalence relation
/// of the [`TableComparator`] is looser than strict equality; that is, if bytewise-distinct keys
/// may compare as equal.
///
/// [`Table`]: crate::table::Table
/// [`TableFilterPolicy`]: crate::filters::TableFilterPolicy
/// [`TableComparator`]: crate::comparator::TableComparator
#[derive(Clone)]
pub struct WriteTableOptions<CompList, Policy, TableCmp> {
    pub compressor_list:        CompList,
    pub selected_compressor:    u8,
    pub filter_policy:          Option<Policy>,
    pub comparator:             TableCmp,
    /// The [`Block`]s of the table will have exactly one `restart` entry every
    /// `block_restart_interval` entries. These restart entries are used by iterators seeking
    /// through the `Block`, including moving backwards. (Forwards step-by-step iteration does not
    /// require `restart`s, but many operations do require them).
    ///
    /// A good default is `16`. Excessively large values do not cause panics or other errors,
    /// but may cause poor performance when seeking.
    ///
    /// [`Block`]: super::block::Block
    pub block_restart_interval: NonZeroUsize,
    /// Loose upper bound on the maximum size in bytes that a [`Block`] in the table may have.
    ///
    /// A [`Block`]'s size may overshoot this limit by at most one key-value entry.
    ///
    /// See [`TableBuilder::add_entry`] for a limit on what `block_size` could be without
    /// risking panics.
    ///
    /// [`Block`]: super::block::Block
    /// [`TableBuilder::add_entry`]: super::table::TableBuilder::add_entry
    pub block_size:             usize,
    /// Whether to sync a table file to persistent storage once it has finished being built.
    pub sync_table:             bool,
}

impl<CompList, Policy, TableCmp>
    WriteTableOptions<CompList, Policy, TableCmp>
where
    CompList: MirroredClone<Fast>,
    Policy:   MirroredClone<Fast>,
    TableCmp: MirroredClone<Fast>,
{
    #[inline]
    #[must_use]
    pub fn fast_clone(&self) -> Self {
        Self {
            compressor_list:        self.compressor_list.fast_mirrored_clone(),
            selected_compressor:    self.selected_compressor,
            filter_policy:          self.filter_policy.as_ref().map(Policy::fast_mirrored_clone),
            comparator:             self.comparator.fast_mirrored_clone(),
            block_restart_interval: self.block_restart_interval,
            block_size:             self.block_size,
            sync_table:             self.sync_table,
        }
    }
}

impl<CompList, Policy, TableCmp> Debug for WriteTableOptions<CompList, Policy, TableCmp>
where
    CompList: FragileContainer<CompressorList>,
    Policy:   Debug,
    TableCmp: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("WriteTableOptions")
            .field("compressor_list",        &*self.compressor_list.get_ref())
            .field("selected_compressor",    &self.selected_compressor)
            .field("filter_policy",          &self.filter_policy)
            .field("comparator",             &self.comparator)
            .field("block_restart_interval", &self.block_restart_interval)
            .field("block_size",             &self.block_size)
            .field("sync_table",             &self.sync_table)
            .finish()
    }
}

/// # Policy-Comparator Compatibility
///
/// The [`TableFilterPolicy`] and [`TableComparator`] of a [`Table`] must be compatible; in
/// particular, the [`TableFilterPolicy`] value (if `Some`) must ensure that generated filters
/// match not only the exact keys for which the filter was generated, but also any key which
/// compares equal to a key the filter was generated for. This matters if the equivalence relation
/// of the [`TableComparator`] is looser than strict equality; that is, if bytewise-distinct keys
/// may compare as equal.
///
/// [`Table`]: crate::table::Table
/// [`TableFilterPolicy`]: crate::filters::TableFilterPolicy
/// [`TableComparator`]: crate::comparator::TableComparator
#[derive(Clone)]
pub struct TableOptions<CompList, Policy, TableCmp, Cache, Pool> {
    pub compressor_list:        CompList,
    pub selected_compressor:    u8,
    pub filter_policy:          Option<Policy>,
    pub comparator:             TableCmp,
    pub verify_checksums:       bool,
    pub block_cache:            Cache,
    pub buffer_pool:            Pool,
    /// The [`Block`]s of the table will have exactly one `restart` entry every
    /// `block_restart_interval` entries. These restart entries are used by iterators seeking
    /// through the `Block`, including moving backwards. (Forwards step-by-step iteration does not
    /// require `restart`s, but many operations do require them).
    ///
    /// A good default is `16`. Excessively large values do not cause panics or other errors,
    /// but may cause poor performance when seeking.
    ///
    /// [`Block`]: super::block::Block
    pub block_restart_interval: NonZeroUsize,
    /// Loose upper bound on the maximum size in bytes that a [`Block`] in the table may have.
    ///
    /// A [`Block`]'s size may overshoot this limit by at most one key-value entry.
    ///
    /// [`Block`]: super::block::Block
    pub block_size:             usize,
    /// Whether to sync a table file to persistent storage once it has finished being built.
    ///
    /// A good default is `true`.
    pub sync_table:             bool,
}

impl<CompList, Policy, TableCmp, Cache, Pool>
    TableOptions<CompList, Policy, TableCmp, Cache, Pool>
{
    #[expect(clippy::type_complexity, reason = "long generic lists, but flat structure")]
    #[inline]
    #[must_use]
    pub fn split<S>(self) -> (
        ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool>,
        WriteTableOptions<CompList, Policy, TableCmp>,
    )
    where
        CompList: MirroredClone<S>,
        Policy:   MirroredClone<S>,
        TableCmp: MirroredClone<S>,
        S:        Speed,
    {
        (
            ReadTableOptions {
                compressor_list:        self.compressor_list.mirrored_clone(),
                filter_policy:          self.filter_policy.as_ref().map(Policy::mirrored_clone),
                comparator:             self.comparator.mirrored_clone(),
                verify_checksums:       self.verify_checksums,
                block_cache:            self.block_cache,
                buffer_pool:            self.buffer_pool,
            },
            WriteTableOptions {
                compressor_list:        self.compressor_list,
                selected_compressor:    self.selected_compressor,
                filter_policy:          self.filter_policy,
                comparator:             self.comparator,
                block_restart_interval: self.block_restart_interval,
                block_size:             self.block_size,
                sync_table:             self.sync_table,
            },
        )
    }

    #[inline]
    #[must_use]
    pub fn fast_clone(&self) -> Self
    where
        CompList: MirroredClone<Fast>,
        Policy:   MirroredClone<Fast>,
        TableCmp: MirroredClone<Fast>,
        Cache:    MirroredClone<Fast>,
        Pool:     MirroredClone<Fast>,
    {
        Self {
            compressor_list:        self.compressor_list.fast_mirrored_clone(),
            selected_compressor:    self.selected_compressor,
            filter_policy:          self.filter_policy.as_ref().map(Policy::fast_mirrored_clone),
            comparator:             self.comparator.fast_mirrored_clone(),
            verify_checksums:       self.verify_checksums,
            block_cache:            self.block_cache.fast_mirrored_clone(),
            buffer_pool:            self.buffer_pool.fast_mirrored_clone(),
            block_restart_interval: self.block_restart_interval,
            block_size:             self.block_size,
            sync_table:             self.sync_table,
        }
    }

    #[inline]
    #[must_use]
    pub fn read_options(&self) -> ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool>
    where
        CompList: MirroredClone<Fast>,
        Policy:   MirroredClone<Fast>,
        TableCmp: MirroredClone<Fast>,
        Cache:    MirroredClone<Fast>,
        Pool:     MirroredClone<Fast>,
    {
        ReadTableOptions {
            compressor_list:  self.compressor_list.fast_mirrored_clone(),
            filter_policy:    self.filter_policy.as_ref().map(Policy::fast_mirrored_clone),
            comparator:       self.comparator.fast_mirrored_clone(),
            verify_checksums: self.verify_checksums,
            block_cache:      self.block_cache.fast_mirrored_clone(),
            buffer_pool:      self.buffer_pool.fast_mirrored_clone(),
        }
    }

    #[inline]
    #[must_use]
    pub fn write_options(&self) -> WriteTableOptions<CompList, Policy, TableCmp>
    where
        CompList: MirroredClone<Fast>,
        Policy:   MirroredClone<Fast>,
        TableCmp: MirroredClone<Fast>,
    {
        WriteTableOptions {
            compressor_list:        self.compressor_list.fast_mirrored_clone(),
            selected_compressor:    self.selected_compressor,
            filter_policy:          self.filter_policy.as_ref().map(Policy::fast_mirrored_clone),
            comparator:             self.comparator.fast_mirrored_clone(),
            block_restart_interval: self.block_restart_interval,
            block_size:             self.block_size,
            sync_table:             self.sync_table,
        }
    }
}

impl<CompList, Policy, TableCmp, Cache, Pool>
    From<TableOptions<CompList, Policy, TableCmp, Cache, Pool>>
for ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool>
{
    #[inline]
    fn from(opts: TableOptions<CompList, Policy, TableCmp, Cache, Pool>) -> Self {
        Self {
            compressor_list:  opts.compressor_list,
            filter_policy:    opts.filter_policy,
            comparator:       opts.comparator,
            verify_checksums: opts.verify_checksums,
            block_cache:      opts.block_cache,
            buffer_pool:      opts.buffer_pool,
        }
    }
}

impl<CompList, Policy, TableCmp, Cache, Pool>
    From<TableOptions<CompList, Policy, TableCmp, Cache, Pool>>
for WriteTableOptions<CompList, Policy, TableCmp>
{
    #[inline]
    fn from(opts: TableOptions<CompList, Policy, TableCmp, Cache, Pool>) -> Self {
        Self {
            compressor_list:        opts.compressor_list,
            selected_compressor:    opts.selected_compressor,
            filter_policy:          opts.filter_policy,
            comparator:             opts.comparator,
            block_restart_interval: opts.block_restart_interval,
            block_size:             opts.block_size,
            sync_table:             opts.sync_table,
        }
    }
}

impl<CompList, Policy, TableCmp, Cache, Pool> Debug
for TableOptions<CompList, Policy, TableCmp, Cache, Pool>
where
    CompList:           FragileContainer<CompressorList>,
    Policy:             Debug,
    TableCmp:           Debug,
    Cache:              KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:               Debug + BufferPool,
    Pool::PooledBuffer: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("TableOptions")
            .field("compressor_list",        &*self.compressor_list.get_ref())
            .field("selected_compressor",    &self.selected_compressor)
            .field("filter_policy",          &self.filter_policy)
            .field("comparator",             &self.comparator)
            .field("verify_checksums",       &self.verify_checksums)
            .field("block_cache",            KVCache::debug(&self.block_cache))
            .field("buffer_pool",            &self.buffer_pool)
            .field("block_restart_interval", &self.block_restart_interval)
            .field("block_size",             &self.block_size)
            .field("sync_table",             &self.sync_table)
            .finish()
    }
}
