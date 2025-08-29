use std::mem;
use std::borrow::Borrow;

use crate::block::{OwnedBlockIter, OwnedBlockIterPieces, TableBlock};
use crate::comparator::ComparatorAdapter;


#[derive(Debug)]
pub(super) struct CurrentIter<PooledBuffer, TableCmp>(CurrentIterState<PooledBuffer, TableCmp>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<PooledBuffer, TableCmp> CurrentIter<PooledBuffer, TableCmp> {
    #[inline]
    #[must_use]
    pub fn new_in_pieces(cmp:  ComparatorAdapter<TableCmp>) -> Self {
        Self(CurrentIterState::new_in_pieces(cmp))
    }

    #[inline]
    #[must_use]
    pub const fn is_initialized(&self) -> bool {
        self.0.is_initialized()
    }

    #[inline]
    #[must_use]
    pub const fn get_iter_ref(
        &self,
    ) -> Option<&OwnedBlockIter<PooledBuffer, ComparatorAdapter<TableCmp>>> {
        self.0.get_iter_ref()
    }

    #[inline]
    #[must_use]
    pub const fn get_iter_mut(
        &mut self,
    ) -> Option<&mut OwnedBlockIter<PooledBuffer, ComparatorAdapter<TableCmp>>> {
        self.0.get_iter_mut()
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<PooledBuffer, TableCmp> CurrentIter<PooledBuffer, TableCmp>
where
    PooledBuffer: Borrow<Vec<u8>>,
{
    pub fn convert_to_pieces(&mut self) {
        self.0.convert_to_pieces();
    }

    pub fn initialize(
        &mut self,
        block_contents: PooledBuffer,
    ) -> &mut OwnedBlockIter<PooledBuffer, ComparatorAdapter<TableCmp>> {
        self.0.initialize(block_contents)
    }
}

#[derive(Debug)]
enum CurrentIterState<PooledBuffer, TableCmp> {
    Initialized(OwnedBlockIter<PooledBuffer, ComparatorAdapter<TableCmp>>),
    InPieces {
        cmp:  ComparatorAdapter<TableCmp>,
        iter: OwnedBlockIterPieces,
    },
    /// Invariant: must not be exposed outside of `Self::convert_to_pieces`
    /// and `Self::initialize`, and those functions must not call each other or themselves.
    BeingModified,
}

impl<PooledBuffer, TableCmp> CurrentIterState<PooledBuffer, TableCmp> {
    #[inline]
    #[must_use]
    fn new_in_pieces(cmp:  ComparatorAdapter<TableCmp>) -> Self {
        Self::InPieces {
            cmp,
            iter: OwnedBlockIterPieces::new(),
        }
    }

    #[inline]
    #[must_use]
    const fn is_initialized(&self) -> bool {
        matches!(self, Self::Initialized(_))
    }

    #[inline]
    #[must_use]
    const fn get_iter_ref(
        &self,
    ) -> Option<&OwnedBlockIter<PooledBuffer, ComparatorAdapter<TableCmp>>> {
        if let Self::Initialized(iter) = self {
            Some(iter)
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    const fn get_iter_mut(
        &mut self,
    ) -> Option<&mut OwnedBlockIter<PooledBuffer, ComparatorAdapter<TableCmp>>> {
        if let Self::Initialized(iter) = self {
            Some(iter)
        } else {
            None
        }
    }
}

impl<PooledBuffer, TableCmp> CurrentIterState<PooledBuffer, TableCmp>
where
    PooledBuffer: Borrow<Vec<u8>>,
{
    fn convert_to_pieces(&mut self) {
        match self {
            // No action needed
            Self::InPieces { .. } => {}
            Self::Initialized(_) => {
                let taken_self = mem::replace(self, Self::BeingModified);
                #[expect(
                    clippy::unreachable,
                    reason = "`self` was Initialized, so `taken_self` is Initialized",
                )]
                let Self::Initialized(initialized) = taken_self else {
                    unreachable!();
                };

                let (block, iter_pieces) = initialized.into_pieces();

                *self = Self::InPieces {
                    cmp:  block.cmp,
                    iter: iter_pieces,
                };
            }
            #[expect(
                clippy::unreachable,
                reason = "an invariant of this type is that this variant is transient",
            )]
            Self::BeingModified => unreachable!(),
        }
    }

    fn initialize(
        &mut self,
        block_contents: PooledBuffer,
    ) -> &mut OwnedBlockIter<PooledBuffer, ComparatorAdapter<TableCmp>> {
        let taken_self = mem::replace(self, Self::BeingModified);

        match taken_self {
            Self::InPieces { cmp, iter } => {
                let block = TableBlock::new(block_contents, cmp);
                *self = Self::Initialized(OwnedBlockIter::from_pieces(block, iter));
            }
            Self::Initialized(initialized) => {
                let (block, iter_pieces) = initialized.into_pieces();
                let block = TableBlock::new(block_contents, block.cmp);
                *self = Self::Initialized(OwnedBlockIter::from_pieces(block, iter_pieces));
            }
            #[expect(
                clippy::unreachable,
                reason = "an invariant of this type is that this variant is transient",
            )]
            Self::BeingModified => unreachable!(),
        }

        #[expect(
            clippy::unreachable,
            reason = "After each reachable branch above, `self` is `Initialized`",
        )]
        if let Self::Initialized(initialized) = self {
            initialized
        } else {
            unreachable!()
        }
    }
}
