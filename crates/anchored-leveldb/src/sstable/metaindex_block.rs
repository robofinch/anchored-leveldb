use std::{cmp::Ordering, convert::Infallible};

use crate::{pub_traits::cmp_and_policy::FilterPolicy, table_format::InternalFilterPolicy};
use crate::{
    all_errors::types::{BlockSeekError, CorruptedBlockError, MetaindexIterError},
    pub_typed_bytes::{BlockHandle, FileSize, TableBlockOffset},
};
use super::block_iter::BlockIter;


/// The hardcoded `filter.` prefix used before a filter's name (in metaindex block entries
/// corresponding to filters).
pub(super) const FILTER_META_PREFIX: &[u8] = b"filter.";


#[derive(Debug)]
pub(super) struct MetaindexBlockIter<'a>(&'a [u8], BlockIter, FileSize);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> MetaindexBlockIter<'a> {
    #[inline]
    pub fn new(
        metaindex_block: &'a [u8],
        table_size:      FileSize,
    ) -> Result<Self, (TableBlockOffset, CorruptedBlockError)> {
        let iter = BlockIter::new(metaindex_block)?;
        Ok(Self(metaindex_block, iter, table_size))
    }

    /// Get the handle of the filter block corresponding to the given `policy` (if there is one).
    pub fn get_filter_handle<Policy: FilterPolicy>(
        &mut self,
        policy: &InternalFilterPolicy<Policy>,
    ) -> Result<Option<BlockHandle>, MetaindexIterError> {
        self.1
            .try_seek_by::<_, Infallible>(self.0, |key| {
                Ok(MetaindexComparator::key_cmp_policy_name(key, policy))
            })
            .map_err(|seek_err| {
                match seek_err {
                    BlockSeekError::Block(block_err) => MetaindexIterError::Block(block_err),
                    BlockSeekError::Cmp(infallible)  => match infallible {}
                }
            })?;

        let Some(maybe_filter_entry) = self.1.current(self.0) else {
            // There is no entry with a key greater than or equal to the policy's key.
            return Ok(None);
        };
        if MetaindexComparator::key_cmp_policy_name(maybe_filter_entry.key, policy).is_ne() {
            // There is an entry with a key greater than the policy's key, but not one equal to it.
            return Ok(None);
        }

        // The filter entry exists, and we found it.
        let (filter_block_handle, _) = BlockHandle::decode(maybe_filter_entry.value, self.2)
            .map_err(MetaindexIterError::Handle)?;

        Ok(Some(filter_block_handle))
    }

    #[inline]
    #[must_use]
    pub const fn current_entry_offset(&self) -> TableBlockOffset {
        self.1.current_entry_offset()
    }

    #[inline]
    #[must_use]
    pub const fn current_value_offset(&self) -> TableBlockOffset {
        self.1.current_value_offset()
    }
}

/// Regardless of the comparator settings of a LevelDB database, its metaindex blocks always use
/// the normal lexicographic ordering on byte slices.
#[derive(Debug, Clone, Copy)]
struct MetaindexComparator;

impl MetaindexComparator {
    /// Compare a `key` against the name of the given `policy` (with an added
    /// [`FILTER_META_PREFIX`]), with respect to the ordering used by the metaindex block of an
    /// SSTable (namely, the lexicographic ordering on byte slices).
    ///
    /// This should only be used for the metaindex block.
    #[must_use]
    fn key_cmp_policy_name<Policy: FilterPolicy>(
        key:    &[u8],
        policy: &InternalFilterPolicy<Policy>,
    ) -> Ordering {
        if let Some(key_filter_name) = key.strip_prefix(FILTER_META_PREFIX) {
            key_filter_name.cmp(policy.0.name())
        } else {
            key.cmp(FILTER_META_PREFIX)
        }
    }
}
