#[cfg(feature = "clone-behavior")]
use clone_behavior::{Speed, MirroredClone};

/// Result for functions whose return type depends on the `SYNC` const generic.
///
/// # Guarantees for `unsafe` code
/// `unsafe` code can assume that this crate's usage of `MaybeSync` is semantically correct;
/// for instance, in a branch where `SYNC` is known to be `true`, it can be unsafely assumed that
/// the `Unsync` variant will not be returned by a method of `MaybeSyncArc<SYNC, _>` provided
/// by this crate. (Naturally, this guarantee cannot extend to arbitrary other crates.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MaybeSync<S, U> {
    /// Used when `SYNC` is `true`.
    Sync(S),
    /// Used when `SYNC` is `false`.
    Unsync(U),
}

#[cfg(feature = "clone-behavior")]
impl<Sync, Unsync, S> MirroredClone<S> for MaybeSync<Sync, Unsync>
where
    Sync:   MirroredClone<S>,
    Unsync: MirroredClone<S>,
    S:      Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        match self {
            Self::Sync(sync)     => Self::Sync(sync.mirrored_clone()),
            Self::Unsync(unsync) => Self::Unsync(unsync.mirrored_clone()),
        }
    }
}
