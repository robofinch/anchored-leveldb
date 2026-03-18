use std::{ops::Range, sync::Arc};

use anchored_skiplist::Comparator as _;

use crate::table_format::InternalComparator;
use crate::{
    all_errors::types::{
        BlockSeekError, CorruptedBlockError, CorruptedTableError, InvalidInternalKey,
    },
    pub_traits::{cmp_and_policy::LevelDBComparator, pool::ByteBuffer},
    pub_typed_bytes::{BlockHandle, BlockType, ShortSlice, TableBlockOffset},
    typed_bytes::{
        EncodedInternalKey, InternalEntry, InternalKey, MaybeUserValue, UnvalidatedInternalEntry,
        UnvalidatedInternalKey,
    },
};
use super::block_iter::{BlockEntry, BlockIter};


/// A circular (rather than fused) iterator through a daata block of an SSTable.
///
/// After a block's contents are passed to [`DataBlockIter::new`] or [`DataBlockIter::set`], all
/// methods of the `DataBlockIter` value **must** be provided references to the same block
/// contents, until [`DataBlockIter::set`] or [`DataBlockIter::clear`] is called. Only when
/// calling [`DataBlockIter::set`] may the block used be changed. Note that the iterator resulting
/// from [`DataBlockIter::new_empty`] or [`DataBlockIter::clear`] must not have any block
/// provided to it until [`DataBlockIter::set`] is called.
///
/// For methods which take a `Cmp` comparator, it is required for logical correctness
/// that the block's keys were sorted in the comparator's order.
///
/// # Errors
/// After a corruption error, the `DataBlockIter` is in an unpredictable corrupt state, so further
/// calls to any methods other than [`DataBlockIter::set`] or [`DataBlockIter::clear`] may result
/// in spurious corruption errors or other strange results.
///
/// # Panics
/// All `DataBlockIter` methods may assume that block contents are provided correctly, as described
/// above. (However, the block contents may be corrupt; that will result in errors being returned,
/// rather than panics.)
#[derive(Debug)]
pub(super) struct DataBlockIter(BlockIter);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl DataBlockIter {
    /// # Correctness
    /// Must be called on an entry of a data block.
    fn map_entry<'a, Cmp: LevelDBComparator>(
        entry: Option<BlockEntry<'a, 'a>>,
        cmp:   &InternalComparator<Cmp>,
    ) -> Result<Option<InternalEntry<'a>>, InvalidInternalKey<Cmp::InvalidKeyError>> {
        if let Some(current) = entry {
            // The values of data blocks should be user values. (Or meaningless data, in the
            // case of tombstones.) And the keys should be internal keys (but might be corrupt).
            #[expect(clippy::expect_used, reason = "could only fail if `BlockIter` has a bug")]
            let entry = UnvalidatedInternalEntry(
                UnvalidatedInternalKey(current.key),
                ShortSlice::new(current.value).map(MaybeUserValue)
                    .expect("`BlockIter::current`'s `value` should be at most `u32::MAX` bytes"),
            );

            Ok(Some(InternalEntry::validate(entry, cmp.validate_user())?))
        } else {
            Ok(None)
        }
    }

    #[inline]
    #[must_use]
    pub const fn new_empty() -> Self {
        Self(BlockIter::new_empty())
    }

    #[inline]
    pub fn new(data_block: &[u8]) -> Result<Self, (TableBlockOffset, CorruptedBlockError)> {
        Ok(Self(BlockIter::new(data_block)?))
    }

    pub fn set(
        &mut self,
        data_block: &[u8],
    ) -> Result<(), (TableBlockOffset, CorruptedBlockError)> {
        self.0.set(data_block)
    }

    #[inline]
    pub fn clear(&mut self) {
        self.0.clear();
    }

    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.0.valid()
    }

    pub fn next<'a, Cmp: LevelDBComparator>(
        &'a mut self,
        data_block: &'a [u8],
        cmp:        &InternalComparator<Cmp>,
    ) -> Result<
        Option<InternalEntry<'a>>,
        BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>,
    > {
        let entry = self.0.next(data_block).map_err(BlockSeekError::Block)?;
        Self::map_entry(entry, cmp).map_err(BlockSeekError::Cmp)
    }

    pub fn current<'a, Cmp: LevelDBComparator>(
        &'a self,
        data_block: &'a [u8],
        cmp:        &InternalComparator<Cmp>,
    ) -> Result<Option<InternalEntry<'a>>, InvalidInternalKey<Cmp::InvalidKeyError>> {
        let entry = self.0.current(data_block);
        Self::map_entry(entry, cmp)
    }

    pub fn current_mapped_err<'a, Cmp: LevelDBComparator, Decompression>(
        &'a self,
        data_block:        &'a [u8],
        data_block_handle: BlockHandle,
        cmp:               &InternalComparator<Cmp>,
    ) -> Result<
        Option<InternalEntry<'a>>,
        CorruptedTableError<Cmp::InvalidKeyError, Decompression>,
    > {
        self.current(data_block, cmp)
            .map_err(|key_err| CorruptedTableError::InvalidInternalKey(
                BlockType::Data,
                data_block_handle,
                self.current_entry_offset(),
                key_err,
            ))
    }

    /// Consume this iterator, and convert it into the current `key` buffer and `value` range.
    ///
    /// If `self.valid()` is currently `true` and `self` is set to some block `block`, then
    /// `self.current()` would return a `Some(_)` entry consisting of `&key` and `&block[value]`.
    ///
    /// Additionally, in that case, the returned value range is guaranteed to have length at most
    /// `u32::MAX`.
    #[inline]
    #[must_use]
    pub fn into_raw_current(self) -> (Vec<u8>, Range<usize>) {
        self.0.into_raw_current()
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

    pub fn prev<'a, Cmp: LevelDBComparator>(
        &'a mut self,
        data_block: &'a [u8],
        cmp:        &InternalComparator<Cmp>,
    ) -> Result<
        Option<InternalEntry<'a>>,
        BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>,
    > {
        let entry = self.0.prev(data_block).map_err(BlockSeekError::Block)?;
        Self::map_entry(entry, cmp).map_err(BlockSeekError::Cmp)
    }

    pub fn try_seek<Cmp: LevelDBComparator>(
        &mut self,
        data_block: &[u8],
        cmp:        &InternalComparator<Cmp>,
        min_bound:  InternalKey<'_>,
    ) -> Result<(), BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>> {
        self.0.try_seek_by(data_block, |key| {
            // The keys of data blocks should be internal keys (but might be corrupt).
            let key = UnvalidatedInternalKey(key);
            let key = EncodedInternalKey::validate(key, cmp.validate_user())?;

            Ok(cmp.cmp(key.as_internal_key(), min_bound))
        })
    }

    pub fn try_seek_before<Cmp: LevelDBComparator>(
        &mut self,
        data_block:         &[u8],
        cmp:                &InternalComparator<Cmp>,
        strict_upper_bound: InternalKey<'_>,
    ) -> Result<(), BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>> {
        self.0.try_seek_before_by(data_block, |key| {
            // The keys of data blocks should be internal keys (but might be corrupt).
            let key = UnvalidatedInternalKey(key);
            let key = EncodedInternalKey::validate(key, cmp.validate_user())?;

            Ok(cmp.cmp(key.as_internal_key(), strict_upper_bound))
        })
    }
}

/// Refers to an entry in an SSTable.
#[derive(Debug)]
pub(crate) struct TableEntry<PooledBuffer> {
    block: Arc<PooledBuffer>,
    key:   Vec<u8>,
    value: Range<usize>,
}

impl<PooledBuffer: ByteBuffer> TableEntry<PooledBuffer> {
    /// Returns a [`TableEntry`] which refers to the current entry of `iter`, or `None` if
    /// `iter` is not `valid()`.
    ///
    /// # Errors
    /// Returns an error if `key` is not a valid internal key.
    ///
    /// # Panics
    /// `(key, value)` must have been returned by `iter.into_raw_current()` for some `iter`
    /// which was `valid()` and set to `block`. Otherwise, panics may occur.
    ///
    /// (`iter` must have been a [`DataBlockIter`] or [`BlockIter`].)
    #[inline]
    pub(super) fn new<Cmp: LevelDBComparator>(
        block: Arc<PooledBuffer>,
        key:   Vec<u8>,
        value: Range<usize>,
        cmp:   &InternalComparator<Cmp>,
    ) -> Result<Self, InvalidInternalKey<Cmp::InvalidKeyError>> {
        let _key = EncodedInternalKey::validate(
            UnvalidatedInternalKey(&key),
            cmp.validate_user(),
        )?;

        Ok(Self {
            block,
            key,
            value,
        })
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<PooledBuffer: ByteBuffer> TableEntry<PooledBuffer> {
    #[inline]
    #[must_use]
    pub fn entry(&self) -> (EncodedInternalKey<'_>, MaybeUserValue<'_>) {
        (
            // On construction, we check that `EncodedInternalKey::validate` succeeds as required.
            EncodedInternalKey::new_unchecked(&self.key),
            // At construction, the user asserts that `self.value` came from a valid `DataBlockIter`
            // or `BlockIter` set to `self.block`, which guarantees that the `value` range has
            // length at most `u32::MAX`.
            // The `.clone()` is needed because `Range` is not `Copy`.
            #[expect(clippy::indexing_slicing, reason = "validated by caller of constructor")]
            MaybeUserValue(ShortSlice::new_unchecked(&self.block.as_slice()[self.value.clone()])),
        )
    }

    #[inline]
    #[must_use]
    pub fn key(&self) -> EncodedInternalKey<'_> {
        // Correctness: see `self.entry()`.
        EncodedInternalKey::new_unchecked(&self.key)
    }

    #[inline]
    #[must_use]
    pub fn value(&self) -> MaybeUserValue<'_> {
        // Correctness: see `self.entry()`.
        // The `.clone()` is needed because `Range` is not `Copy`.
        #[expect(clippy::indexing_slicing, reason = "validated by caller of constructor")]
        MaybeUserValue(ShortSlice::new_unchecked(&self.block.as_slice()[self.value.clone()]))
    }
}
