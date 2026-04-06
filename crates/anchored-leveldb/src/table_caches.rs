use std::sync::Arc;
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    num::{NonZeroU64, NonZeroUsize},
};

use quick_cache::{Weighter, sync::Cache};

use crate::sstable::TableReader;
use crate::{
    pub_traits::pool::{BufferPool, ByteBuffer},
    pub_typed_bytes::{FileNumber, FileOffset},
};


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BlockCacheKey {
    pub table_number: FileNumber,
    pub block_offset: FileOffset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TableCacheKey {
    pub table_number: FileNumber,
}

#[derive(Debug, Clone, Copy)]
struct BufferWeighter;

impl<PooledBuffer: ByteBuffer> Weighter<BlockCacheKey, Arc<PooledBuffer>> for BufferWeighter {
    fn weight(&self, _key: &BlockCacheKey, val: &Arc<PooledBuffer>) -> u64 {
        u64::try_from(val.capacity()).unwrap_or(u64::MAX)
    }
}

pub(crate) struct BlockCache<Pool: BufferPool>(
    Cache<BlockCacheKey, Arc<Pool::PooledBuffer>, BufferWeighter>,
);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Pool: BufferPool> BlockCache<Pool> {
    #[must_use]
    pub fn new(
        byte_capacity:      u64,
        average_block_size: NonZeroUsize,
    ) -> Self {
        let average_u64 = NonZeroU64::try_from(average_block_size);
        #[expect(clippy::integer_division, reason = "exact value does not matter much")]
        let estimated_blocks_capacity = if let Ok(average) = average_u64 {
            usize::try_from(byte_capacity / average).unwrap_or(usize::MAX)
        } else {
            1
        };

        Self(Cache::with_weighter(
            estimated_blocks_capacity,
            byte_capacity,
            BufferWeighter,
        ))
    }

    #[must_use]
    pub fn get(&self, block_key: BlockCacheKey) -> Option<Arc<Pool::PooledBuffer>> {
        self.0.get(&block_key)
    }

    pub fn get_or_insert_with<F, E>(
        &self,
        block_key: BlockCacheKey,
        with:      F,
    ) -> Result<Arc<Pool::PooledBuffer>, E>
    where
        F: FnOnce() -> Result<Arc<Pool::PooledBuffer>, E>,
    {
        self.0.get_or_insert_with(&block_key, with)
    }

    pub fn evict(&self, table_key: TableCacheKey) {
        // Retain blocks from *different* tables.
        self.0.retain(|block_key, _| block_key.table_number != table_key.table_number);
    }

    pub fn clear(&self) {
        self.0.clear();
    }
}

impl<Pool: BufferPool> Debug for BlockCache<Pool> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("BlockCache").field(&self.0).finish()
    }
}

pub(crate) struct TableCache<File, Policy, Pool: BufferPool>(
    Cache<TableCacheKey, Arc<TableReader<File, Policy, Pool>>>,
);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File, Policy, Pool: BufferPool> TableCache<File, Policy, Pool> {
    #[must_use]
    pub fn new(table_capacity: usize) -> Self {
        Self(Cache::new(table_capacity))
    }

    #[must_use]
    pub fn get(&self, table_key: TableCacheKey) -> Option<Arc<TableReader<File, Policy, Pool>>> {
        self.0.get(&table_key)
    }

    pub fn get_or_insert_with<F, E>(
        &self,
        table_key: TableCacheKey,
        with:      F,
    ) -> Result<Arc<TableReader<File, Policy, Pool>>, E>
    where
        F: FnOnce() -> Result<Arc<TableReader<File, Policy, Pool>>, E>,
    {
        self.0.get_or_insert_with(&table_key, with)
    }

    pub fn evict(&self, table_key: TableCacheKey) {
        self.0.remove(&table_key);
    }

    pub fn clear(&self) {
        self.0.clear();
    }
}

impl<File, Policy, Pool: BufferPool> Debug for TableCache<File, Policy, Pool> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("TableCache").field(&self.0).finish()
    }
}
