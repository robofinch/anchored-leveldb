use std::cmp::Ordering;

use clone_behavior::{DeepClone, MirroredClone, Speed};
use generic_container::{FragileContainer, GenericContainer};

use anchored_sstable::{
    format_options::{LexicographicComparator, TableComparator as _},
    perf_options::{BloomPolicy as SSTableBloomPolicy, BloomPolicyName, TableFilterPolicy as _},
};

use super::trait_equivalents::{FilterPolicy, LevelDBComparator};


// ================================================================
//  Add an ID to LexicographicComparator
// ================================================================

#[derive(Default, Debug, Clone, Copy)]
pub struct BytewiseComparator;

impl LevelDBComparator for BytewiseComparator {
    #[inline]
    fn name(&self) -> &'static [u8] {
        b"leveldb.BytewiseComparator"
    }

    #[inline]
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        LexicographicComparator.cmp(lhs, rhs)
    }

    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        LexicographicComparator.find_short_separator(from, to, separator);
    }

    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        LexicographicComparator.find_short_successor(key, successor);
    }
}

impl<S: Speed> MirroredClone<S> for BytewiseComparator {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<S: Speed> DeepClone<S> for BytewiseComparator {
    #[inline]
    fn deep_clone(&self) -> Self {
        *self
    }
}

// ================================================================
//  Add a Name to SSTableBloomPolicy
// ================================================================

#[derive(Debug, Clone, Copy)]
enum Name {}

impl BloomPolicyName for Name {
    const NAME: &'static [u8] = b"leveldb.BuiltinBloomFilter2";
}

#[derive(Default, Debug, Clone, Copy)]
pub struct BloomPolicy(SSTableBloomPolicy<Name>);

impl BloomPolicy {
    /// The number of filter bits to use per key. The default `BloomPolicy` filter uses 10 bits per
    /// key to get a false positive rate just under 1%.
    ///
    /// The filter is clamped to having at most around 43 bits per key, with a resulting false
    /// positive rate just under 0.0000001% (1e-7 percent).
    ///
    /// See <https://en.wikipedia.org/wiki/Bloom_filter#:~:text=9.6%20bits%20per%20element>.
    #[must_use]
    pub fn new(bits_per_key: u8) -> Self {
        Self(SSTableBloomPolicy::new(bits_per_key))
    }
}

impl FilterPolicy for BloomPolicy {
    #[inline]
    fn name(&self) -> &'static [u8] {
        self.0.name()
    }

    fn create_filter(&self, flattened_keys: &[u8], key_offsets: &[usize], filter: &mut Vec<u8>) {
        self.0.create_filter(flattened_keys, key_offsets, filter);
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        self.0.key_may_match(key, filter)
    }
}

impl<S: Speed> MirroredClone<S> for BloomPolicy {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        // No way to mutate the inner `bits_per_key` is exposed.
        *self
    }
}

impl<S: Speed> DeepClone<S> for BloomPolicy {
    #[inline]
    fn deep_clone(&self) -> Self {
        *self
    }
}

// ================================================================
//  Dyn implementations
// ================================================================

impl<C: FragileContainer<dyn LevelDBComparator>> LevelDBComparator for C {
    fn name(&self) -> &'static [u8] {
        let comparator: &dyn LevelDBComparator = &*self.get_ref();
        comparator.name()
    }

    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        let comparator: &dyn LevelDBComparator = &*self.get_ref();
        comparator.cmp(lhs, rhs)
    }

    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        let comparator: &dyn LevelDBComparator = &*self.get_ref();
        comparator.find_short_separator(from, to, separator);
    }

    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        let comparator: &dyn LevelDBComparator = &*self.get_ref();
        comparator.find_short_successor(key, successor);
    }
}

impl<C: FragileContainer<dyn FilterPolicy>> FilterPolicy for C {
    fn name(&self) -> &'static [u8] {
        let inner: &dyn FilterPolicy = &*self.get_ref();
        inner.name()
    }

    fn create_filter(&self, flattened_keys: &[u8], key_offsets: &[usize], filter: &mut Vec<u8>) {
        let inner: &dyn FilterPolicy = &*self.get_ref();
        inner.create_filter(flattened_keys, key_offsets, filter);
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        let inner: &dyn FilterPolicy = &*self.get_ref();
        inner.key_may_match(key, filter)
    }
}

// ================================================================
//  GenericContainer implementations
// ================================================================

impl<Cmp, C> LevelDBComparator for GenericContainer<Cmp, C>
where
    Cmp: LevelDBComparator,
    C:   FragileContainer<Cmp>,
{
    fn name(&self) -> &'static [u8] {
        let cmp: &Cmp = &self.container.get_ref();
        cmp.name()
    }

    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        let cmp: &Cmp = &self.container.get_ref();
        cmp.cmp(lhs, rhs)
    }

    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        let cmp: &Cmp = &self.container.get_ref();
        cmp.find_short_separator(from, to, separator);
    }

    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        let cmp: &Cmp = &self.container.get_ref();
        cmp.find_short_successor(key, successor);
    }
}

impl<Policy, C> FilterPolicy for GenericContainer<Policy, C>
where
    Policy: FilterPolicy,
    C:      FragileContainer<Policy>,
{
    fn name(&self) -> &'static [u8] {
        let policy: &Policy = &self.container.get_ref();
        policy.name()
    }

    fn create_filter(&self, flattened_keys: &[u8], key_offsets: &[usize], filter: &mut Vec<u8>) {
        let policy: &Policy = &self.container.get_ref();
        policy.create_filter(flattened_keys, key_offsets, filter);
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        let policy: &Policy = &self.container.get_ref();
        policy.key_may_match(key, filter)
    }
}
