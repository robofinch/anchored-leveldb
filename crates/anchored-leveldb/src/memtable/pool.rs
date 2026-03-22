use std::num::NonZeroUsize;
use std::{iter, mem};
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::{Arc, Mutex, MutexGuard},
};

use clone_behavior::{MirroredClone, Speed};
use oorandom::Rand64;

use crate::{pub_traits::cmp_and_policy::LevelDBComparator, utils::UnwrapPoison as _};
use super::format::{MemtableSkiplist, MemtableSkiplistReader};


/// A pool of several reset skiplists (and a PRNG used to seed skiplist PRNGs).
///
/// # Invariants
/// They are sorted in decreasing order of capacity, and all skiplists are placed at the
/// start. (That is, there's a prefix of `Some`s and a suffix of `None`s).
#[expect(clippy::type_complexity, reason = "despite the nesting, remains quite readable")]
pub(super) struct MemtablePool<Cmp> {
    unwrap_poison: bool,
    pool:          Arc<Mutex<(Box<[MemtableSlot<Cmp>]>, Rand64)>>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Cmp: LevelDBComparator> MemtablePool<Cmp> {
    #[inline]
    #[must_use]
    pub fn new(
        unwrap_poison: bool,
        pool_size:     NonZeroUsize,
        prng:          Rand64,
    ) -> Self {
        let pool = iter::repeat_with(MemtableSlot::default)
            .take(pool_size.get())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            unwrap_poison,
            pool: Arc::new(Mutex::new((pool, prng))),
        }
    }

    fn lock(&self) -> MutexGuard<'_, (Box<[MemtableSlot<Cmp>]>, Rand64)> {
        self.pool.lock_unwrapping_poison(self.unwrap_poison)
    }

    /// Get a skiplist from the pool, or the seed for a new skiplist.
    pub fn get(&self) -> Result<MemtableSkiplist<Cmp>, u64> {
        let mut pool = self.lock();

        #[expect(
            clippy::indexing_slicing,
            reason = "`pool_size: NonZeroUsize`, so indexing with `[0]` succeeds",
        )]
        if let Some((skiplist, _)) = pool.0[0].0.take() {
            pool.0.rotate_left(1);

            Ok(skiplist)
        } else {
            Err(pool.1.rand_u64())
        }
    }

    /// Drop a skiplist reader, adding the skiplist to the pool if the reader held the skiplist's
    /// last refcount.
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

impl<Cmp> Debug for MemtablePool<Cmp> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("MemtablePool")
            .field("unwrap_poison", &self.unwrap_poison)
            .field("pool",          &self.pool)
            .finish()
    }
}

impl<Cmp, S: Speed> MirroredClone<S> for MemtablePool<Cmp> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        self.clone()
    }
}

struct MemtableSlot<Cmp>(Option<(MemtableSkiplist<Cmp>, usize)>);

impl<Cmp> Default for MemtableSlot<Cmp> {
    #[inline]
    fn default() -> Self {
        Self(None)
    }
}

impl<Cmp> Debug for MemtableSlot<Cmp> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        if let Some((_, capacity)) = &self.0 {
            write!(f, "Some(capacity: {capacity}, ..)")
        } else {
            write!(f, "None")
        }
    }
}
