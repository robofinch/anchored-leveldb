use std::cmp::Ordering;

use anchored_skiplist::Comparator;

use clone_behavior::{MirroredClone, Speed};

use crate::pub_traits::cmp_and_policy::LevelDBComparator;
use crate::typed_bytes::{InternalKey, UserKey};


#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalComparator<Cmp>(Cmp);

impl<Cmp: LevelDBComparator> Comparator<InternalKey<'_>, InternalKey<'_>>
for InternalComparator<Cmp>
{
    #[inline]
    fn cmp(&self, lhs: InternalKey<'_>, rhs: InternalKey<'_>) -> Ordering {
        match self.cmp_user(lhs.0, rhs.0) {
            Ordering::Equal => {},
            non_equal @ (Ordering::Less | Ordering::Greater) => return non_equal,
        }

        // Swapped lhs and rhs to sort decreasing. Note that the sequence number comparison
        // takes precedence over the entry type comparison, since the entry type is stored
        // in the least significant byte.
        rhs.1.raw_inner().cmp(&lhs.1.raw_inner())
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Cmp: LevelDBComparator> InternalComparator<Cmp> {
    #[inline]
    #[must_use]
    pub fn cmp_user(&self, lhs: UserKey<'_>, rhs: UserKey<'_>) -> Ordering {
        self.0.cmp(lhs.inner(), rhs.inner())
    }
}

impl<Cmp: MirroredClone<S>, S: Speed> MirroredClone<S> for InternalComparator<Cmp> {
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalFilterPolicy<Policy>(Policy);

impl<Policy: MirroredClone<S>, S: Speed> MirroredClone<S> for InternalFilterPolicy<Policy> {
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}
