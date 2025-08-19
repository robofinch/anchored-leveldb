mod keys;
mod entries;
mod pooled_keys;
mod pooled_entries;


use crate::leveldb::LevelDBGenerics;

pub use self::{entries::Entries, keys::Keys};
pub use self::{
    pooled_entries::{PooledEntries, OwnedEntryRef},
    pooled_keys::{PooledKeys, OwnedKeyRef},
};


use std::fmt::Debug;


pub trait IterGenerics {
}

impl<T: LevelDBGenerics> IterGenerics for T {
}
