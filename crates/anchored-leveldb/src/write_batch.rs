#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};


#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct WriteBatch {
}

impl WriteBatch {
    pub fn new() -> Self {
        todo!()
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        todo!()
    }

    pub fn delete(&mut self, key: &[u8]) {
        todo!()
    }

    pub fn length(&self) -> u32 {
        todo!()
    }

    pub fn clear(&mut self) {
        todo!()
    }

    // If `LevelDB::write_batch` doesn't end up needing ownership over the `WriteBatch`,
    // then we should provide functions that would make it easier to try to reuse `WriteBatch`
    // allocations.

    // pub fn byte_capacity(&self) -> usize {
    //     todo!()
    // }

    // pub fn shrink_to_fit(&mut self) {
    //     todo!()
    // }

    // pub fn shrink_to(&mut self, min_capacity: usize) {
    //     todo!()
    // }
}
