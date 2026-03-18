use std::ops::Range;

use anchored_skiplist::Comparator as _;

use crate::{pub_typed_bytes::TableBlockOffset, table_format::InternalComparator};
use crate::{
    all_errors::types::{BlockSeekError, CorruptedBlockError, InvalidInternalKey},
    pub_traits::{cmp_and_policy::LevelDBComparator, pool::ByteBuffer},
    typed_bytes::{
        EncodedInternalKey, InternalKey, MaybeUserValue, UnvalidatedInternalEntry,
        UnvalidatedInternalKey,
    },
};
use super::block_iter::BlockIter;


#[derive(Debug)]
pub(super) struct DataBlockIter<'a>(&'a [u8], BlockIter);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> DataBlockIter<'a> {
    #[inline]
    pub fn new(data_block: &'a [u8]) -> Result<Self, (TableBlockOffset, CorruptedBlockError)> {
        let iter = BlockIter::new(data_block)?;
        Ok(Self(data_block, iter))
    }

    pub fn try_seek<Cmp: LevelDBComparator>(
        &mut self,
        cmp:       &InternalComparator<Cmp>,
        min_bound: InternalKey<'_>,
    ) -> Result<(), BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>> {
        self.1.try_seek_by(self.0, |key| {
            // The keys of data blocks should be internal keys (but might be corrupt).
            let key = UnvalidatedInternalKey(key);
            let key = EncodedInternalKey::validate(key, cmp.validate_user())?;

            Ok(cmp.cmp(key.as_internal_key(), min_bound))
        })
    }

    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.1.valid()
    }

    pub fn current(&self) -> Option<UnvalidatedInternalEntry<'_>> {
        // The values of data blocks should be user values. (Or meaningless data, in the
        // case of tombstones.) And the keys should be internal keys (but might be corrupt).
        #[expect(clippy::expect_used, reason = "could only fail if `BlockIter` has a bug")]
        self.1.current(self.0)
            .map(|entry|
                UnvalidatedInternalEntry(
                UnvalidatedInternalKey(entry.key),
                MaybeUserValue::new(entry.value)
                    .expect("`BlockIter::current`'s `value` should be at most `u32::MAX` bytes"),
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
        self.1.into_raw_current()
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

/// Refers to an entry in an SSTable.
#[derive(Debug)]
pub(crate) struct TableEntry<PooledBuffer> {
    block: PooledBuffer,
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
        block: PooledBuffer,
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
            MaybeUserValue::new_unchecked(&self.block.as_slice()[self.value.clone()]),
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
        MaybeUserValue::new_unchecked(&self.block.as_slice()[self.value.clone()])
    }
}
