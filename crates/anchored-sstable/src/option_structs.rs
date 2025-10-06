use std::num::NonZeroUsize;

use clone_behavior::{MirroredClone, Speed};

// TODO: provide builders and/or defaults


#[derive(Debug, Clone)]
pub struct ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool> {
    pub compressor_list:  CompList,
    pub filter_policy:    Option<Policy>,
    pub comparator:       TableCmp,
    pub verify_checksums: bool,
    pub block_cache:      Cache,
    pub buffer_pool:      Pool,
}

impl<CompList, Policy, TableCmp, Cache, Pool, S> MirroredClone<S>
for ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool>
where
    CompList: MirroredClone<S>,
    Policy:   MirroredClone<S>,
    TableCmp: MirroredClone<S>,
    Cache:    MirroredClone<S>,
    Pool:     MirroredClone<S>,
    S:        Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self {
            compressor_list:  self.compressor_list.mirrored_clone(),
            filter_policy:    self.filter_policy.mirrored_clone(),
            comparator:       self.comparator.mirrored_clone(),
            verify_checksums: self.verify_checksums,
            block_cache:      self.block_cache.mirrored_clone(),
            buffer_pool:      self.buffer_pool.mirrored_clone(),
        }
    }
}

#[derive(Debug, Clone)]
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

impl<CompList, Policy, TableCmp, S> MirroredClone<S>
for WriteTableOptions<CompList, Policy, TableCmp>
where
    CompList: MirroredClone<S>,
    Policy:   MirroredClone<S>,
    TableCmp: MirroredClone<S>,
    S:        Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self {
            compressor_list:        self.compressor_list.mirrored_clone(),
            selected_compressor:    self.selected_compressor,
            filter_policy:          self.filter_policy.mirrored_clone(),
            comparator:             self.comparator.mirrored_clone(),
            block_restart_interval: self.block_restart_interval,
            block_size:             self.block_size,
            sync_table:             self.sync_table,
        }
    }
}

#[derive(Debug, Clone)]
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
                filter_policy:          self.filter_policy.mirrored_clone(),
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
