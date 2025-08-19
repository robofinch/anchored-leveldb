use clone_behavior::{IndependentClone, MirroredClone, Speed};

use anchored_sstable::{
    BloomPolicy as SSTableBloomPolicy,
    BloomPolicyName,
    FilterPolicy,
    TableComparator,
    DefaultComparator as SSTableDefaultComparator,
    DefaultComparatorID,
};


// ================================
//  Add a Name to BloomPolicy
// ================================
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
        *self
    }
}

impl<S: Speed> IndependentClone<S> for BloomPolicy {
    #[inline]
    fn independent_clone(&self) -> Self {
        *self
    }
}

// ================================
//  Add an ID to DefaultComparator
// ================================

#[derive(Debug, Clone, Copy)]
enum ID {}

impl DefaultComparatorID for ID {
    const ID: &'static [u8] = b"leveldb.BytewiseComparator";
}

#[derive(Default, Debug, Clone, Copy)]
pub struct DefaultComparator(SSTableDefaultComparator<ID>);

impl TableComparator for DefaultComparator {
    #[inline]
    fn id(&self) -> &'static [u8] {
        self.0.id()
    }

    #[inline]
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> std::cmp::Ordering {
        self.0.cmp(lhs, rhs)
    }

    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        self.0.find_short_separator(from, to, separator);
    }

    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        self.0.find_short_successor(key, successor);
    }
}

impl<S: Speed> MirroredClone<S> for DefaultComparator {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<S: Speed> IndependentClone<S> for DefaultComparator {
    #[inline]
    fn independent_clone(&self) -> Self {
        *self
    }
}

// ================================================================
//  Adapters for the internal key format
// ================================================================

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalFilterPolicy<Policy>(Policy);

impl<Policy: FilterPolicy> FilterPolicy for InternalFilterPolicy<Policy> {
    #[inline]
    fn name(&self) -> &'static [u8] {
        self.0.name()
    }

    fn create_filter(&self, flattened_keys: &[u8], key_offsets: &[usize], filter: &mut Vec<u8>) {
        todo!()
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        todo!()
    }
}

impl<Policy, S> MirroredClone<S> for InternalFilterPolicy<Policy>
where
    Policy: MirroredClone<S>,
    S:      Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}

impl<Policy, S> IndependentClone<S> for InternalFilterPolicy<Policy>
where
    Policy: IndependentClone<S>,
    S:      Speed,
{
    #[inline]
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalComparator<Cmp>(Cmp);

impl<Cmp: TableComparator> TableComparator for InternalComparator<Cmp> {
    fn id(&self) -> &'static [u8] {
        todo!()
    }

    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> std::cmp::Ordering {
        todo!()
    }

    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        todo!()
    }

    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        todo!()
    }
}
