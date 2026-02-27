use std::{mem, sync::{Arc, Mutex, MutexGuard}};

use clone_behavior::{MirroredClone, Speed};
use oorandom::Rand64;

use crate::{pub_traits::cmp_and_policy::LevelDBComparator, utils::UnwrapPoison as _};
use super::format::{MemtableSkiplist, MemtableSkiplistReader};


const POOL_SIZE: usize = 4;


/// A pool of up to [`POOL_SIZE`] reset skiplists.
/// (And a PRNG piggy-backing off of the mutex.)
///
/// # Invariants
/// They are sorted in decreasing order of capacity, and all skiplists are placed at the
/// start. (That is, there's a prefix of `Some`s and a suffix of `None`s).
pub(super) struct MemtablePool<Cmp> {
    unwrap_poison: bool,
    pool:          Arc<Mutex<([MemtableSlot<Cmp>; POOL_SIZE], Rand64)>>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Cmp: LevelDBComparator> MemtablePool<Cmp> {
    #[inline]
    #[must_use]
    pub fn new(
        unwrap_poison: bool,
        prng:          Rand64,
    ) -> Self {
        Self {
            unwrap_poison,
            pool: Arc::new(Mutex::new((Default::default(), prng))),
        }
    }

    fn lock(&self) -> MutexGuard<'_, ([MemtableSlot<Cmp>; POOL_SIZE], Rand64)> {
        self.pool.lock_unwrapping_poison(self.unwrap_poison)
    }

    pub fn get(&self) -> Result<MemtableSkiplist<Cmp>, u64> {
        let mut pool = self.lock();

        if let Some((skiplist, _)) = pool.0[0].0.take() {
            pool.0.rotate_left(1);

            Ok(skiplist)
        } else {
            Err(pool.1.rand_u64())
        }
    }

    pub fn return_reader(&self, reader: MemtableSkiplistReader<Cmp>) {
        if let Some(mut inserting) = reader.into_reset() {
            let inserting_capacity = inserting.chunk_capacity();
            let mut inserting = (inserting, inserting_capacity);

            let mut pool = self.lock();

            for pool_entry in &mut pool.0 {
                if let Some(pooled) = &mut pool_entry.0 {
                    // If the skiplist we're inserting has a greater capacity, put it into the
                    // pool at this position. Then, the previously-inserted skiplist will have a
                    // chance to be inserted into later slots.
                    if pooled.1 < inserting.1 {
                        mem::swap(&mut inserting, pooled);
                    }
                } else {
                    // This slot (and any following slot) is empty.
                    pool_entry.0 = Some(inserting);
                    return;
                }
            }

            // All slots are full, and whatever skiplist is currently in `inserting` has the
            // least capacity.
            drop(pool);
            drop(inserting);
        }
    }
}

impl<Cmp> Clone for MemtablePool<Cmp> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            unwrap_poison: self.unwrap_poison,
            pool:          Arc::clone(&self.pool),
        }
    }
}

impl<Cmp, S: Speed> MirroredClone<S> for MemtablePool<Cmp> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        self.clone()
    }
}

pub(super) struct MemtableSlot<Cmp>(Option<(MemtableSkiplist<Cmp>, usize)>);

impl<Cmp> Default for MemtableSlot<Cmp> {
    #[inline]
    fn default() -> Self {
        Self(None)
    }
}
