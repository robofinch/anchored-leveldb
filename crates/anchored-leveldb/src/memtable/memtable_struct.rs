#![expect(unsafe_code, reason = "deconstruct a type (`MemtableReader`) which impls Drop")]

use std::{mem, ptr};
use std::mem::ManuallyDrop;

use clone_behavior::FastMirroredClone;
use oorandom::Rand64;

use crate::{
    pub_traits::cmp_and_policy::LevelDBComparator,
    table_format::InternalComparator,
    write_batch::ChainedWriteBatchIter,
};
use crate::typed_bytes::{EncodedInternalEntry, InternalEntry, LookupKey};
use super::pool::MemtablePool;
use super::{
    format::{MemtableEntryEncoder, MemtableSkiplist, MemtableSkiplistReader},
    iter::{MemtableIter, MemtableLendingIter},
};


pub(crate) struct Memtable<Cmp> {
    skiplist:      MemtableSkiplist<Cmp>,
    init_capacity: usize,
    pool:          MemtablePool<Cmp>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Cmp: LevelDBComparator> Memtable<Cmp> {
    #[inline]
    #[must_use]
    pub fn new(
        init_capacity: usize,
        unwrap_poison: bool,
        seed:          u128,
        cmp:           InternalComparator<Cmp>,
    ) -> Self {
        let mut prng = Rand64::new(seed);

        let skiplist = MemtableSkiplist::new_with_cmp(init_capacity, prng.rand_u64(), cmp);

        Self {
            skiplist,
            init_capacity,
            pool: MemtablePool::new(unwrap_poison, prng),
        }
    }

    pub fn reader(&self) -> MemtableReader<Cmp> {
        MemtableReader::new(
            self.skiplist.reader(),
            self.pool.fast_mirrored_clone(),
        )
    }

    pub fn take(&mut self) -> ImmutableMemtable<Cmp>
    where
        Cmp: FastMirroredClone,
    {
        let new_skiplist = self.pool.get().unwrap_or_else(|seed| {
            let cmp = self.skiplist.cmp().fast_mirrored_clone();
            MemtableSkiplist::new_with_cmp(self.init_capacity, seed, cmp)
        });

        let old_skiplist = mem::replace(&mut self.skiplist, new_skiplist);

        ImmutableMemtable(MemtableReader::new(
            old_skiplist.into_reader(),
            self.pool.fast_mirrored_clone(),
        ))
    }

    /// Writes an entry into this memtable. It should have a unique [`SequenceNumber`]. (If that
    /// condition fails to hold, the database may become corrupted.)
    pub fn insert_entry(&mut self, entry: InternalEntry<'_>) {
        // Note that we return `BufferAllocError`s when space fails to be allocated for data
        // read from a table file. Since a table file could, hypothetically, contain malicious data
        // or something (or just corrupted data), that seems sensible. However, we should trust
        // the user when it comes to this data invocation's allocations.
        // The possible causes of an allocation failure are:
        // - `ENTRY_ALIGN` is not a power of two (our `ENTRY_ALIGN` is `1`, which is a power of 2).
        // - The encoded entry's size is so large that the layout's size exceeds `isize::MAX`.
        // - The encoded entry is so large that the allocator (which falls back to the global
        //   allocator) could not allocate enough space.
        // Since the first does not apply, and we disregard the second two, we use `expect`.
        #[expect(clippy::expect_used, reason = "This should be user-trusted data")]
        self.skiplist
            .insert_with(MemtableEntryEncoder::new(entry))
            .expect("failed to allocate space in memtable");
    }

    /// Writes every entry in a series of [`WriteBatch`]es into this memtable, giving a unique
    /// [`SequenceNumber`] to each.
    ///
    /// # Panics or Correctness Errors
    /// This function assumes that no entry with a sequence number greater than
    /// `last_sequence_number` has been previously inserted into the memtable.
    ///
    /// If this condition is not met, the database may become corrupted.
    #[inline]
    pub fn insert_write_batches(&mut self, batches: ChainedWriteBatchIter<'_>) {
        batches.for_each(|entry| self.insert_entry(entry));
    }

    /// Get an entry with the given user key and a sequence number less than or equal to the given
    /// sequence number in the lookup key, if there is such an entry in the memtable. If there are
    /// multiple such entries, the one with the greatest sequence number is returned.
    #[must_use]
    pub fn get<'a>(&'a self, lookup_key: LookupKey<'_>) -> Option<EncodedInternalEntry<'a>> {
        self.iter().get(lookup_key)
    }

    /// Returns a close lower bound for the total number of bytes allocated by this memtable.
    #[inline]
    #[must_use]
    pub fn allocated_bytes(&mut self) -> usize {
        self.skiplist.allocated_bytes()
    }

    #[inline]
    #[must_use]
    pub fn iter(&self) -> MemtableIter<'_, Cmp> {
        MemtableIter::new(self.skiplist.iter())
    }
}

impl<'a, Cmp: LevelDBComparator> IntoIterator for &'a Memtable<Cmp> {
    type IntoIter = MemtableIter<'a, Cmp>;
    type Item = EncodedInternalEntry<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub(crate) struct ImmutableMemtable<Cmp: LevelDBComparator>(MemtableReader<Cmp>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Cmp: LevelDBComparator> ImmutableMemtable<Cmp> {
    /// Get an entry with the given user key and a sequence number less than or equal to the given
    /// sequence number in the lookup key, if there is such an entry in the memtable. If there are
    /// multiple such entries, the one with the greatest sequence number is returned.
    #[must_use]
    pub fn get<'a>(&'a self, lookup_key: LookupKey<'_>) -> Option<EncodedInternalEntry<'a>> {
        self.0.get(lookup_key)
    }

    #[inline]
    #[must_use]
    pub fn iter(&self) -> MemtableIter<'_, Cmp> {
        self.0.iter()
    }

    #[inline]
    #[must_use]
    pub fn lending_iter(self) -> MemtableLendingIter<Cmp> {
        self.0.lending_iter()
    }
}

impl<'a, Cmp: LevelDBComparator> IntoIterator for &'a ImmutableMemtable<Cmp> {
    type IntoIter = MemtableIter<'a, Cmp>;
    type Item = EncodedInternalEntry<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub(crate) struct MemtableReader<Cmp: LevelDBComparator> {
    skiplist: ManuallyDrop<MemtableSkiplistReader<Cmp>>,
    pool:     MemtablePool<Cmp>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Cmp: LevelDBComparator> MemtableReader<Cmp> {
    #[inline]
    #[must_use]
    const fn new(skiplist: MemtableSkiplistReader<Cmp>, pool:  MemtablePool<Cmp>) -> Self {
        Self {
            skiplist: ManuallyDrop::new(skiplist),
            pool,
        }
    }

    /// Get an entry with the given user key and a sequence number less than or equal to the given
    /// sequence number in the lookup key, if there is such an entry in the memtable. If there are
    /// multiple such entries, the one with the greatest sequence number is returned.
    #[must_use]
    pub fn get<'a>(&'a self, lookup_key: LookupKey<'_>) -> Option<EncodedInternalEntry<'a>> {
        self.iter().get(lookup_key)
    }

    #[inline]
    #[must_use]
    pub fn iter(&self) -> MemtableIter<'_, Cmp> {
        MemtableIter::new(self.skiplist.iter())
    }

    #[inline]
    #[must_use]
    pub fn lending_iter(self) -> MemtableLendingIter<Cmp> {
        let this = ManuallyDrop::new(self);
        // SAFETY: Since `this` is a valid `MemtableReader` (which is not `repr(packed)`),
        // we have that `&raw const this.skiplist` is valid for reads, is properly aligned,
        // and has an initialized pointee. Additionally, we do not trigger a double-drop
        // by copying a `!Copy` value, since we have disabled the source's destructor with
        // `ManuallyDrop`, and never again access its `skiplist` field.
        //
        // (Also, this is a common way to deconstruct types which implement `Drop` and have
        // `!Copy` fields.)
        let skiplist = unsafe { ptr::read(&raw const this.skiplist) };
        // SAFETY: Same as above, but for the `pool` field.
        let pool = unsafe { ptr::read(&raw const this.pool) };

        MemtableLendingIter::new(ManuallyDrop::into_inner(skiplist).lending_iter(), pool)
    }
}

impl<'a, Cmp: LevelDBComparator> IntoIterator for &'a MemtableReader<Cmp> {
    type IntoIter = MemtableIter<'a, Cmp>;
    type Item = EncodedInternalEntry<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<Cmp: LevelDBComparator> Drop for MemtableReader<Cmp> {
    fn drop(&mut self) {
        // SAFETY: Since this is the destructor of the struct which owns the `ManuallyDrop`
        // value, and since we do not use it again in this function (not even by moving it),
        // this is sound.
        let skiplist = unsafe { ManuallyDrop::take(&mut self.skiplist) };
        self.pool.return_reader(skiplist);
    }
}
