// Import paths are not currently stable.
#![allow(
    warnings,
    reason = "this crate is very unstable. Allow checks to be done on full repo without noise.",
)]

pub mod compactor;
pub mod error;
pub mod iter;
pub mod leveldb;
pub mod logger;
pub mod options;
// pub mod read_only_leveldb;
pub mod write_batch;


mod sstable_trait_implementations;


#[expect(clippy::redundant_pub_crate, reason = "emphasize that contents are internal")]
mod utils {
    use std::iter;


    /// Make it more semantically clear what is meant instead of "4".
    pub(crate) const U32_BYTES: usize = 4;

    // See https://users.rust-lang.org/t/how-to-find-common-prefix-of-two-byte-slices-effectively/25815/4
    /// Get the length of the prefix that two byte slices have in common.
    ///
    /// The returned value is at most the length of the shorter byte slice.
    pub(crate) fn common_prefix_len(lhs: &[u8], rhs: &[u8]) -> usize {
        // TODO: compare 128 and 64 for `N`
        chunked_common_prefix_len::<128>(lhs, rhs)
    }

    fn chunked_common_prefix_len<const N: usize>(lhs: &[u8], rhs: &[u8]) -> usize {
        #![expect(clippy::indexing_slicing, reason = "`offset <= lhs.len().min(rhs.len())`")]

        let offset = iter::zip(lhs.chunks_exact(N), rhs.chunks_exact(N))
            .take_while(|(left, right)| left == right)
            .count()
            * N;

        offset + iter::zip(&lhs[offset..], &rhs[offset..])
            .take_while(|(left, right)| left == right)
            .count()
    }
}

// Not sure where in the module hierarchy this will end up
#[derive(Debug, Clone)]
pub struct Snapshot {

}
