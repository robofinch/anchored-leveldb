use std::cmp::Ordering;

use super::Comparator;


#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalComparator<C>(pub(crate) C);

// TODO: should I really implement Comparator for this?
// Might provide opportunity for confusion later.
impl<C: Comparator> InternalComparator<C> {
    #[inline]
    fn id(&self) -> &'static str {
        self.0.id()
    }

    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        // some::helper::function(&self.0, a, b)
        todo!()
    }

    fn find_shortest_separator(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
        todo!()
    }

    fn find_shortest_successor(&self, key: &[u8]) -> Vec<u8> {
        todo!()
    }
}
