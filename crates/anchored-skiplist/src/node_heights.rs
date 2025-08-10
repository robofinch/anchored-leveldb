use oorandom::Rand32;


/// The maximum height of skiplist implementations in this crate.
///
/// With the [`random_node_height`] function, one node is generated with this maximum height per
/// approximately 4 million entries inserted into the skiplist (on average).
pub(crate) const MAX_HEIGHT: usize = 12;


/// A simple PRNG trait, used for generating random heights for nodes in a skiplist.
pub(crate) trait Prng32 {
    /// Produces a random `u32` in the range `[0, u32::MAX]`.
    ///
    /// (See [`oorandom::Rand32::rand_u32`]; this function is the same interface.)
    #[must_use]
    fn rand_u32(&mut self) -> u32;
}

impl Prng32 for Rand32 {
    #[inline]
    fn rand_u32(&mut self) -> u32 {
        // Inherent impls take priority over traits, so this is the inherent method
        // of `Rand32` a.k.a. `Self`
        Self::rand_u32(self)
    }
}

/// Return a random value in `1..=MAX_HEIGHT`, in a geometric distribution (higher values
/// are exponentially less likely).
///
/// Technically, `MAX_HEIGHT` is `4/3` more likely than it would be in an exact and unbounded
/// geometric distribution, since what would be higher values are capped to `MAX_HEIGHT`.
pub(crate) fn random_node_height<P: Prng32>(prng: &mut P) -> usize {
    // Skiplists choose a random height with a geometric distribution.
    // The height is increased with probability `1/n`, with `n=2` and `n=4` seeming to be
    // common options. `n=4` uses less memory, and is what Google's LevelDB implementation uses.
    let mut height = 1;
    while height < MAX_HEIGHT && prng.rand_u32() % 4 == 0 {
        height += 1;
    }
    height
}
