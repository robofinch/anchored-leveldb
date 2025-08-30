use std::cmp::Ordering;

use clone_behavior::{IndependentClone, MirroredClone, Speed};

use anchored_sstable::options::{FilterPolicy as SSTableFilterPolicy, TableComparator};

use super::trait_equivalents::{FilterPolicy, LevelDBComparator};


#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalComparator<Cmp>(Cmp);

impl<Cmp: LevelDBComparator> TableComparator for InternalComparator<Cmp> {
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        todo!()
    }

    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        todo!()
    }

    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        todo!()
    }
}

impl<Cmp, S> MirroredClone<S> for InternalComparator<Cmp>
where
    Cmp: MirroredClone<S>,
    S:   Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}

impl<Cmp, S> IndependentClone<S> for InternalComparator<Cmp>
where
    Cmp: IndependentClone<S>,
    S:   Speed,
{
    #[inline]
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalFilterPolicy<Policy>(Policy);

impl<Policy: FilterPolicy> SSTableFilterPolicy for InternalFilterPolicy<Policy> {
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
