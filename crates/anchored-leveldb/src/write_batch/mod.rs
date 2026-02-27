/// `WriteBatch`, `BorrowedWriteBatch`, `ChainedWriteBatches`.
mod batches;
/// `WriteEntry`, `WriteBatchIter`, `ChainedWriteBatchIter`.
mod iter;


pub use self::{
    batches::{BorrowedWriteBatch, ChainedWriteBatches, WriteBatch},
    iter::{WriteBatchIter, WriteEntry},
};
pub(crate) use self::iter::ChainedWriteBatchIter;
