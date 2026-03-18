use anchored_skiplist::Comparator as _;

use crate::{pub_traits::cmp_and_policy::LevelDBComparator, table_format::InternalComparator};
use crate::{
    all_errors::types::{
        BlockHandleCorruption, BlockSeekError, CorruptedBlockError, CorruptedTableError,
        InvalidInternalKey,
    },
    pub_typed_bytes::{BlockHandle, TableBlockOffset},
    typed_bytes::{EncodedInternalKey, InternalKey, UnvalidatedInternalKey},
};
use super::block_iter::BlockIter;


#[derive(Debug)]
pub(super) struct IndexBlockIter<'a>(&'a [u8], BlockIter);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> IndexBlockIter<'a> {
    #[inline]
    pub fn new(index_block: &'a [u8]) -> Result<Self, (TableBlockOffset, CorruptedBlockError)> {
        let iter = BlockIter::new(index_block)?;
        Ok(Self(index_block, iter))
    }

    pub fn try_seek<Cmp: LevelDBComparator>(
        &mut self,
        cmp:       &InternalComparator<Cmp>,
        min_bound: InternalKey<'_>,
    ) -> Result<(), BlockSeekError<InvalidInternalKey<Cmp::InvalidKeyError>>> {
        self.1.try_seek_by(self.0, |key| {
            // The keys of index blocks should be internal keys.
            let key = UnvalidatedInternalKey(key);
            let key = EncodedInternalKey::validate(key, cmp.validate_user())?;

            Ok(cmp.cmp(key.as_internal_key(), min_bound))
        })
    }

    pub fn current(&self) -> Result<Option<BlockHandle>, BlockHandleCorruption> {
        if let Some(entry) = self.1.current(self.0) {
            // The values of index blocks should be internal keys.
            Ok(Some(BlockHandle::decode(entry.value)?.0))
        } else {
            Ok(None)
        }
    }

    pub fn current_mapped_err<InvalidKey, Decompression>(&self) -> Result<
        Option<BlockHandle>,
        CorruptedTableError<InvalidKey, Decompression>
    > {
        self.current()
            .map_err(|handle_err| CorruptedTableError::CorruptedDataBlockHandle(
                self.current_value_offset(),
                handle_err,
            ))
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
