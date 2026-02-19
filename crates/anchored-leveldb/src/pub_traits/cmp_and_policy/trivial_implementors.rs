use std::convert::Infallible;

use clone_behavior::{DeepClone, MirroredClone, Speed};

use super::traits::{CoarserThan, EquivalenceRelation, FilterPolicy};


/// The trivial equivalence relation that compares everything as equal.
#[derive(Default, Debug, Clone, Copy)]
pub struct AllEqual;

impl EquivalenceRelation for AllEqual {}

impl<S: Speed> MirroredClone<S> for AllEqual {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<S: Speed> DeepClone<S> for AllEqual {
    #[inline]
    fn deep_clone(&self) -> Self {
        *self
    }
}

impl<T: ?Sized + EquivalenceRelation> CoarserThan<T> for AllEqual {}


/// An uninhabited type which implements [`FilterPolicy`].
///
/// In particular, `Option<NoFilterPolicy>` is a zero-sized type that can take the place of a
/// generic type similar to `Option<impl FilterPolicy>`.
#[derive(Debug, Clone, Copy)]
pub enum NoFilterPolicy {}

#[expect(clippy::uninhabited_references, reason = "this filter policy can never be constructed")]
impl FilterPolicy for NoFilterPolicy {
    type Eq          = AllEqual;
    type FilterError = Infallible;

    fn name(&self) -> &'static [u8] {
        match *self {}
    }

    fn create_filter(&self, _: &[u8], _: &[usize], _: &mut Vec<u8>) -> Result<(), Infallible> {
        match *self {}
    }

    fn key_may_match(&self, _: &[u8], _: &[u8]) -> bool {
        match *self {}
    }
}

#[expect(clippy::uninhabited_references, reason = "this filter policy can never be constructed")]
impl<S: Speed> MirroredClone<S> for NoFilterPolicy {
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

#[expect(clippy::uninhabited_references, reason = "this filter policy can never be constructed")]
impl<S: Speed> DeepClone<S> for NoFilterPolicy {
    fn deep_clone(&self) -> Self {
        *self
    }
}
