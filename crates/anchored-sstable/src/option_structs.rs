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
    /// Must be strictly greater than `0`. A good default is `16`.
    ///
    /// [`Block`]: super::block::Block
    pub block_restart_interval: usize,
    /// Loose upper bound on the maximum size in bytes that a [`Block`] in the table may have.
    ///
    /// A [`Block`]'s size may overshoot this limit by at most one key-value entry.
    ///
    /// [`Block`]: super::block::Block
    pub block_size:             usize,
}

#[derive(Debug, Clone)]
pub struct TableOptions<CompList, Policy, TableCmp, Cache, Pool> {
    pub compressor_list:        CompList,
    pub selected_compressor:    u8,
    pub filter_policy:          Option<Policy>,
    pub comparator:             TableCmp,
    /// The [`Block`]s of the table will have exactly one `restart` entry every
    /// `block_restart_interval` entries. These restart entries are used by iterators seeking
    /// through the `Block`, including moving backwards. (Forwards step-by-step iteration does not
    /// require `restart`s, but many operations do require them).
    ///
    /// Must be strictly greater than `0`. A good default is `16`.
    ///
    /// [`Block`]: super::block::Block
    pub block_restart_interval: usize,
    /// Loose upper bound on the maximum size in bytes that a [`Block`] in the table may have.
    ///
    /// A [`Block`]'s size may overshoot this limit by at most one key-value entry.
    ///
    /// [`Block`]: super::block::Block
    pub block_size:             usize,
    pub verify_checksums:       bool,
    pub block_cache:            Cache,
    pub buffer_pool:            Pool,
}

impl<CompList, Policy, TableCmp, Cache, Pool>
    TableOptions<CompList, Policy, TableCmp, Cache, Pool>
{
    #[inline]
    #[must_use]
    pub fn mirrored_read_opts<S>(
        &self,
    ) -> ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool>
    where
        CompList: MirroredClone<S>,
        Policy:   MirroredClone<S>,
        TableCmp: MirroredClone<S>,
        Cache:    MirroredClone<S>,
        Pool:     MirroredClone<S>,
        S:        Speed,
    {
        ReadTableOptions {
            compressor_list:  self.compressor_list.mirrored_clone(),
            filter_policy:    self.filter_policy.mirrored_clone(),
            comparator:       self.comparator.mirrored_clone(),
            verify_checksums: self.verify_checksums,
            block_cache:      self.block_cache.mirrored_clone(),
            buffer_pool:      self.buffer_pool.mirrored_clone(),
        }
    }

    #[inline]
    #[must_use]
    pub fn mirrored_write_opts<S>(&self) -> WriteTableOptions<CompList, Policy, TableCmp>
    where
        CompList: MirroredClone<S>,
        Policy:   MirroredClone<S>,
        TableCmp: MirroredClone<S>,
        S:        Speed,
    {
        WriteTableOptions {
            compressor_list:        self.compressor_list.mirrored_clone(),
            selected_compressor:    self.selected_compressor,
            filter_policy:          self.filter_policy.mirrored_clone(),
            comparator:             self.comparator.mirrored_clone(),
            block_restart_interval: self.block_restart_interval,
            block_size:             self.block_size,
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
    From<&TableOptions<CompList, Policy, TableCmp, Cache, Pool>>
for ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool>
where
    CompList: Clone,
    Policy:   Clone,
    TableCmp: Clone,
    Cache:    Clone,
    Pool:     Clone,
{
    #[inline]
    fn from(opts: &TableOptions<CompList, Policy, TableCmp, Cache, Pool>) -> Self {
        Self {
            compressor_list:  opts.compressor_list.clone(),
            filter_policy:    opts.filter_policy.clone(),
            comparator:       opts.comparator.clone(),
            verify_checksums: opts.verify_checksums,
            block_cache:      opts.block_cache.clone(),
            buffer_pool:      opts.buffer_pool.clone(),
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
        }
    }
}

impl<CompList, Policy, TableCmp, Cache, Pool>
    From<&TableOptions<CompList, Policy, TableCmp, Cache, Pool>>
for WriteTableOptions<CompList, Policy, TableCmp>
where
    CompList: Clone,
    Policy:   Clone,
    TableCmp: Clone,
{
    #[inline]
    fn from(opts: &TableOptions<CompList, Policy, TableCmp, Cache, Pool>) -> Self {
        Self {
            compressor_list:        opts.compressor_list.clone(),
            selected_compressor:    opts.selected_compressor,
            filter_policy:          opts.filter_policy.clone(),
            comparator:             opts.comparator.clone(),
            block_restart_interval: opts.block_restart_interval,
            block_size:             opts.block_size,
        }
    }
}
