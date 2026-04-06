#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to perform Polonius-style early returns of borrows"),
)]

#[cfg(not(feature = "polonius"))]
use std::mem;
use std::{ops::Range, sync::Arc};

use anchored_skiplist::Comparator as _;

use crate::table_format::InternalComparator;
use crate::{
    all_errors::types::{BlockSeekError, CorruptedBlockError, InvalidInternalKey},
    pub_traits::{cmp_and_policy::LevelDBComparator, pool::ByteBuffer},
    pub_typed_bytes::{ShortSlice, TableBlockOffset},
    typed_bytes::{
        EncodedInternalEntry, EncodedInternalKey, InternalKey, MaybeUserValue,
        UnvalidatedInternalEntry, UnvalidatedInternalKey,
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
/// After an error occurs, the iterator [`reset`]s itself, but it does not have a sticky error
/// state.
///
/// # Panics
/// All `DataBlockIter` methods may assume that block contents are provided correctly, as described
/// above. (However, the block contents may be corrupt; that will result in errors being returned,
/// rather than panics.)
///
/// [`reset`]: DataBlockIter::reset
#[derive(Debug)]
pub(super) struct DataBlockIter(
    /// # Correctness invariant
    /// If `valid`, then the current entry should be a valid `EncodedInternalEntry`.
    BlockIter,
);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl DataBlockIter {
    /// # Correctness
    /// Must be called on an entry of a data block.
    #[must_use]
    const fn to_unvalidated<'a>(entry: BlockEntry<'a, 'a>) -> UnvalidatedInternalEntry<'a> {
        #![expect(clippy::expect_used, reason = "could only fail if `BlockIter` has a bug")]
        let value = ShortSlice::new(entry.value)
            .expect("`BlockIter`'s `value` should be at most `u32::MAX` bytes");

        // The values of data blocks should be user values. (Or meaningless data, in the
        // case of tombstones.) And the keys should be internal keys (but might be corrupt).
        UnvalidatedInternalEntry(UnvalidatedInternalKey(entry.key), MaybeUserValue(value))
    }

    /// # Correctness
    /// Must be called on an entry of a data block.
    fn map_entry<'a, Cmp: LevelDBComparator>(
        entry: Option<BlockEntry<'a, 'a>>,
        cmp:   &InternalComparator<Cmp>,
    ) -> Result<Option<EncodedInternalEntry<'a>>, InvalidInternalKey<Cmp::InvalidKeyError>> {
        if let Some(encoded_entry) = entry.map(Self::to_unvalidated) {
            // The values of data blocks should be user values. (Or meaningless data, in the
            // case of tombstones.) And the keys should be internal keys (but might be corrupt).
            Ok(Some(EncodedInternalEntry::validate(encoded_entry, cmp.validate_user())?))
        } else {
            Ok(None)
        }
    }

    /// Create a new iterator not associated with any data block.
    #[inline]
    #[must_use]
    pub const fn new_empty() -> Self {
        Self(BlockIter::new_empty())
    }

    /// Create a new iterator associated with the given data block.
    #[inline]
    pub fn new(data_block: &[u8]) -> Result<Self, (TableBlockOffset, CorruptedBlockError)> {
        Ok(Self(BlockIter::new(data_block)?))
    }

    /// Set the iterator to be associated with the given data block.
    pub fn set(
        &mut self,
        data_block: &[u8],
    ) -> Result<(), (TableBlockOffset, CorruptedBlockError)> {
        self.0.set(data_block)
    }

    /// Clear any association with a data block.
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
        Option<EncodedInternalEntry<'a>>,
        BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>,
    > {
        let entry_result = match self.0.next(data_block) {
            Ok(entry) => Self::map_entry(entry, cmp).map_err(BlockSeekError::Cmp),
            Err(err)  => Err(BlockSeekError::Block(err)),
        };

        match entry_result {
            Ok(entry) => {
                // SAFETY: We are only transmuting a lifetime, so we need to worry about
                // borrowck and aliasing rules. This is a known-to-be-sound Polonius-style
                // conditional return of a borrow, and we confirm that this is sound by testing
                // it under Polonius.

                #[cfg(not(feature = "polonius"))]
                let entry = unsafe {
                    mem::transmute::<
                        Option<EncodedInternalEntry<'_>>,
                        Option<EncodedInternalEntry<'_>>,
                    >(entry)
                };

                Ok(entry)
            }
            Err(err) => {
                self.reset();
                Err(err)
            }
        }
    }

    #[must_use]
    pub fn current<'a>(&'a self, data_block: &'a [u8]) -> Option<EncodedInternalEntry<'a>> {
        // We can correctly call `EncodedInternalEntry::new_unchecked` because we validate
        // the current entry of `self.0` whenever we mutate `self.0`, and otherwise `reset`
        // `self.0`.
        self.0.current(data_block)
            .map(Self::to_unvalidated)
            .map(EncodedInternalEntry::new_unchecked)
    }

    /// Consume this iterator, and convert it into the current `key` buffer and `value` range.
    ///
    /// If `self.valid()` is currently `true` and `self` is set to some block `block`, then
    /// `self.current()` would return a `Some(_)` entry consisting of `&key` and `&block[value]`.
    ///
    /// Additionally, in that case, the returned value range is guaranteed to have length at most
    /// `u32::MAX`, and the returned key buffer is guaranteed to be a validated user key.
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
        Option<EncodedInternalEntry<'a>>,
        BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>,
    > {
        let entry_result = match self.0.next(data_block) {
            Ok(entry) => Self::map_entry(entry, cmp).map_err(BlockSeekError::Cmp),
            Err(err)  => Err(BlockSeekError::Block(err)),
        };

        match entry_result {
            Ok(entry) => {
                // SAFETY: We are only transmuting a lifetime, so we need to worry about
                // borrowck and aliasing rules. This is a known-to-be-sound Polonius-style
                // conditional return of a borrow, and we confirm that this is sound by testing
                // it under Polonius.

                #[cfg(not(feature = "polonius"))]
                let entry = unsafe {
                    mem::transmute::<
                        Option<EncodedInternalEntry<'_>>,
                        Option<EncodedInternalEntry<'_>>,
                    >(entry)
                };

                Ok(entry)
            }
            Err(err) => {
                self.reset();
                Err(err)
            }
        }
    }

    /// Reset the iterator to its initial position, before the first entry and after the last
    /// entry (if there are any entries in the collection).
    ///
    /// The iterator will then not be `valid()`.
    ///
    /// Note that this does **not** [`clear`] the iterator.
    ///
    /// [`clear`]: DataBlockIter::clear
    #[inline]
    pub fn reset(&mut self) {
        self.0.reset();
    }

    pub fn try_seek<Cmp: LevelDBComparator>(
        &mut self,
        data_block:  &[u8],
        cmp:         &InternalComparator<Cmp>,
        lower_bound: InternalKey<'_>,
    ) -> Result<(), BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>> {
        let result = self.0.try_seek_by(data_block, |key| {
            // The keys of data blocks should be internal keys (but might be corrupt).
            let key = UnvalidatedInternalKey(key);
            let key = EncodedInternalKey::validate(key, cmp.validate_user())?;

            Ok(cmp.cmp(key.as_internal_key(), lower_bound))
        });

        let result = result.and_then(|()| {
            match Self::map_entry(self.0.current(data_block), cmp) {
                Ok(_entry) => Ok(()),
                Err(err)   => Err(BlockSeekError::Cmp(err)),
            }
        });

        if result.is_err() {
            self.reset();
        }

        result
    }

    pub fn try_seek_before<Cmp: LevelDBComparator>(
        &mut self,
        data_block:         &[u8],
        cmp:                &InternalComparator<Cmp>,
        strict_upper_bound: InternalKey<'_>,
    ) -> Result<(), BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>> {
        let result = self.0.try_seek_before_by(data_block, |key| {
            // The keys of data blocks should be internal keys (but might be corrupt).
            let key = UnvalidatedInternalKey(key);
            let key = EncodedInternalKey::validate(key, cmp.validate_user())?;

            Ok(cmp.cmp(key.as_internal_key(), strict_upper_bound))
        });

        let result = result.and_then(|()| {
            match Self::map_entry(self.0.current(data_block), cmp) {
                Ok(_entry) => Ok(()),
                Err(err)   => Err(BlockSeekError::Cmp(err)),
            }
        });

        if result.is_err() {
            self.reset();
        }

        result
    }
}

/// Refers to an entry in an SSTable.
#[derive(Debug)]
pub(crate) struct SSTableEntry<PooledBuffer> {
    block: Arc<PooledBuffer>,
    key:   Vec<u8>,
    value: Range<usize>,
}

impl<PooledBuffer: ByteBuffer> SSTableEntry<PooledBuffer> {
    /// Returns a [`SSTableEntry`] which refers to the current entry of `iter`, or `None` if
    /// `iter` is not `valid()`.
    ///
    /// # Correctness
    /// Panics or other errors may occur (either here or downstream) if `iter` is not set to
    /// `block`.
    #[inline]
    pub(super) fn new(iter: DataBlockIter, block: Arc<PooledBuffer>) -> Option<Self> {
        if iter.valid() {
            let (key, value) = iter.into_raw_current();
            Some(Self { block, key, value })
        } else {
            None
        }
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<PooledBuffer: ByteBuffer> SSTableEntry<PooledBuffer> {
    #[inline]
    #[must_use]
    pub fn entry(&self) -> (EncodedInternalKey<'_>, MaybeUserValue<'_>) {
        (self.key(), self.value())
    }

    #[inline]
    #[must_use]
    pub fn key(&self) -> EncodedInternalKey<'_> {
        // At construction, the user asserts that `self.value` came from a valid `DataBlockIter`
        // set to `self.block`. We confirmed that the iter was `valid`, which, by the invariants
        // of `DataBlockIter`, implies that its key is a validated user key.
        EncodedInternalKey::new_unchecked(&self.key)
    }

    #[inline]
    #[must_use]
    pub fn value(&self) -> MaybeUserValue<'_> {
        // At construction, the user asserts that `self.value` came from a valid `DataBlockIter`
        // set to `self.block`. We confirmed that the iter was `valid`, which, by the invariants
        // of `DataBlockIter`, implies that its value iis guaranteed to have length at most
        // `u32::MAX`.
        // The `.clone()` is needed because `Range` is not `Copy`.
        #[expect(clippy::indexing_slicing, reason = "validated by caller of constructor")]
        let maybe_user_value = &self.block.as_slice()[self.value.clone()];

        #[expect(clippy::expect_used, reason = "validated by caller of constructor")]
        let maybe_user_value = ShortSlice::new(maybe_user_value)
            .expect("`SSTableEntry.value` should have length at most u32:::MAX");

        MaybeUserValue(maybe_user_value)
    }
}
