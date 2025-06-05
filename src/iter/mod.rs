mod keys;
mod entries;
mod pooled_keys;
mod pooled_entries;

#[cfg(feature = "lender")]
mod lender_impls;
#[cfg(feature = "lending-iterator")]
mod lending_iterator_impls;


use crate::leveldb::LevelDBGenerics;

pub use self::{entries::Entries, keys::Keys};
pub use self::{
    pooled_entries::{PooledEntries, OwnedEntryRef},
    pooled_keys::{PooledKeys, OwnedKeyRef},
};


use std::fmt::Debug;


pub trait IterGenerics: Debug {
}

impl<T: LevelDBGenerics> IterGenerics for T {
}
