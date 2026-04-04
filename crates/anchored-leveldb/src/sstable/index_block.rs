use anchored_skiplist::Comparator as _;

use crate::{pub_traits::cmp_and_policy::LevelDBComparator, table_format::InternalComparator};
use crate::{
    all_errors::types::{
        BlockHandleCorruption, BlockSeekError, CorruptedBlockError, CorruptedTableError,
        IndexIterError, InvalidInternalKey,
    },
    pub_typed_bytes::{BlockHandle, FileSize, TableBlockOffset},
    typed_bytes::{EncodedInternalKey, InternalKey, UnvalidatedInternalKey},
};
use super::block_iter::BlockIter;


/// A circular (rather than fused) iterator through the index block of an SSTable.
///
/// After a block's contents are passed to [`IndexBlockIter::new`] or [`IndexBlockIter::set`], all
/// methods of the `IndexBlockIter` value **must** be provided references to the same block
/// contents, until [`IndexBlockIter::set`] or [`IndexBlockIter::clear`] is called. Only when
/// calling [`IndexBlockIter::set`] may the block used be changed. Note that the iterator resulting
/// from [`IndexBlockIter::new_empty`] or [`IndexBlockIter::clear`] must not have any block
/// provided to it until [`IndexBlockIter::set`] is called.
///
/// For methods which take a `Cmp` comparator, it is required for logical correctness
/// that the block's keys were sorted in the comparator's order.
///
/// # Errors
/// After a corruption error, the `IndexBlockIter` is in an unpredictable corrupt state, so further
/// calls to any methods other than [`IndexBlockIter::set`] or [`IndexBlockIter::clear`] may result
/// in spurious corruption errors or other strange results.
///
/// # Panics
/// All `IndexBlockIter` methods may assume that block contents are provided correctly, as described
/// above. (However, the block contents may be corrupt; that will result in errors being returned,
/// rather than panics.)
#[derive(Debug)]
pub(super) struct IndexBlockIter(BlockIter, FileSize);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl IndexBlockIter {
    #[inline]
    #[must_use]
    pub const fn new_empty() -> Self {
        Self(BlockIter::new_empty(), FileSize(0))
    }

    #[inline]
    pub fn new(
        index_block: &[u8],
        table_size:  FileSize,
    ) -> Result<Self, (TableBlockOffset, CorruptedBlockError)> {
        Ok(Self(BlockIter::new(index_block)?, table_size))
    }

    pub fn set(
        &mut self,
        index_block: &[u8],
        table_size:  FileSize,
    ) -> Result<(), (TableBlockOffset, CorruptedBlockError)> {
        self.1 = table_size;
        self.0.set(index_block)
    }

    #[inline]
    pub fn clear(&mut self) {
        self.0.clear();
        self.1 = FileSize(0);
    }

    #[inline]
    pub fn reset(&mut self) {
        self.0.reset();
        self.1 = FileSize(0);
    }

    #[inline]
    #[must_use]
    pub const fn current_entry_offset(&self) -> TableBlockOffset {
        self.0.current_entry_offset()
    }

    #[inline]
    #[must_use]
    pub const fn current_value_offset(&self) -> TableBlockOffset {
        self.0.current_value_offset()
    }

    pub fn next(
        &mut self,
        index_block: &[u8],
    ) -> Result<Option<BlockHandle>, IndexIterError> {
        if let Some(entry) = self.0.next(index_block)? {
            // The values of index blocks should be internal keys.
            Ok(Some(BlockHandle::decode(entry.value, self.1)?.0))
        } else {
            Ok(None)
        }
    }

    pub fn current(
        &self,
        index_block: &[u8],
    ) -> Result<Option<BlockHandle>, BlockHandleCorruption> {
        if let Some(entry) = self.0.current(index_block) {
            // The values of index blocks should be internal keys.
            Ok(Some(BlockHandle::decode(entry.value, self.1)?.0))
        } else {
            Ok(None)
        }
    }

    pub fn current_mapped_err<InvalidKey, Decompression>(
        &self,
        index_block: &[u8],
    ) -> Result<Option<BlockHandle>, CorruptedTableError<InvalidKey, Decompression>> {
        self.current(index_block)
            .map_err(|handle_err| CorruptedTableError::CorruptedDataBlockHandle(
                self.current_value_offset(),
                handle_err,
            ))
    }

    pub fn prev(
        &mut self,
        index_block: &[u8],
    ) -> Result<Option<BlockHandle>, IndexIterError> {
        if let Some(entry) = self.0.prev(index_block)? {
            // The values of index blocks should be internal keys.
            Ok(Some(BlockHandle::decode(entry.value, self.1)?.0))
        } else {
            Ok(None)
        }
    }

    pub fn try_seek<Cmp: LevelDBComparator>(
        &mut self,
        index_block: &[u8],
        cmp:         &InternalComparator<Cmp>,
        min_bound:   InternalKey<'_>,
    ) -> Result<(), BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>> {
        self.0.try_seek_by(index_block, |key| {
            // The keys of index blocks should be internal keys.
            let key = UnvalidatedInternalKey(key);
            let key = EncodedInternalKey::validate(key, cmp.validate_user())?;

            Ok(cmp.cmp(key.as_internal_key(), min_bound))
        })
    }

    pub fn try_seek_before<Cmp: LevelDBComparator>(
        &mut self,
        index_block:        &[u8],
        cmp:                &InternalComparator<Cmp>,
        strict_upper_bound: InternalKey<'_>,
    ) -> Result<(), BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>> {
        self.0.try_seek_before_by(index_block, |key| {
            // The keys of index blocks should be internal keys.
            let key = UnvalidatedInternalKey(key);
            let key = EncodedInternalKey::validate(key, cmp.validate_user())?;

            Ok(cmp.cmp(key.as_internal_key(), strict_upper_bound))
        })
    }
}
