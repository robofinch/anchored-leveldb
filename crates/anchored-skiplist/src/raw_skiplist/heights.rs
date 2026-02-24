#![expect(unsafe_code, reason = "Allow internal mutability to be unsafely externally synchronized")]
// The external synchronization is not strictly necessary, but the `unsafe` code for lifetime
// erasure and the node format is already complicated (and unavoidable). Might as well put in
// marginally more effort in, and avoid the overhead of a mutex.

use core::{cell::UnsafeCell, num::NonZeroU8};
use core::fmt::{Formatter, Result as FmtResult};

use oorandom::Rand32;


/// The maximum height of skiplist implementations in this crate.
///
/// With the [`random_node_height`] function, one node is generated with this maximum height per
/// approximately 4 million entries inserted into the skiplist (on average).
// Note that this lint is triggered on older versions of clippy but not newer versions.
#[allow(clippy::unwrap_used, reason = "This is confirmed to succeed at compile-time")]
pub(super) const MAX_HEIGHT: NonZeroU8 = NonZeroU8::new(12).unwrap();
#[expect(clippy::as_conversions, reason = "`usize::from` is not available in const contexts")]
pub(super) const MAX_HEIGHT_USIZE: usize = MAX_HEIGHT.get() as usize;


pub(super) struct ExternallySynchronizedRand32(UnsafeCell<Rand32>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl ExternallySynchronizedRand32 {
    /// Get a new PRNG wrapping a [`Rand32`] with the given `seed`.
    #[inline]
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self(UnsafeCell::new(Rand32::new(seed)))
    }

    /// Return a random value in `1..=MAX_HEIGHT`, in a geometric distribution (higher values
    /// are exponentially less likely).
    ///
    /// Technically, [`MAX_HEIGHT`] is `4/3` more likely than it would be in an exact and unbounded
    /// geometric distribution, since what would be higher values are capped to `MAX_HEIGHT`.
    ///
    /// # Safety
    /// Methods on [`ExternallySynchronizedRand32`] should not be called concurrently. That is,
    /// calls to this method must *not* race with other calls to methods of `self` (in particular,
    /// [`self.random_node_height()`] and [`self.debug(_)`]).
    ///
    /// [`self.random_node_height()`]: ExternallySynchronizedRand32::random_node_height
    /// [`self.debug(_)`]: ExternallySynchronizedRand32::debug
    #[must_use]
    pub unsafe fn random_node_height(&self) -> NonZeroU8 {
        let prng: *mut Rand32 = self.0.get();
        // SAFETY: we need to uphold the aliasing rules. Since we only access this field
        // in `Self::random_node_height` and `Self::debug`, both of which are marked `unsafe`
        // and require that the caller ensure that calls to them are not concurrent, and since
        // we never expose the PRNG reference outside those functions, we know that accesses to this
        // field must be unique. Therefore, we uphold the aliasing requirements for
        // an exclusive/mutable borrow.
        let prng = unsafe { &mut *prng };

        // Skiplists choose a random height with a geometric distribution.
        // The height is increased with probability `1/n`, with `n=2` and `n=4` seeming to be
        // common options. `n=4` uses less memory, and is what Google's LevelDB implementation uses.
        let mut height = 1;
        while height < MAX_HEIGHT.get() && prng.rand_u32() % 4 == 0 {
            height += 1;
        }
        #[expect(clippy::expect_used, reason = "easy to see that this succeeds")]
        NonZeroU8::new(height).expect("`1 <= height <= MAX_HEIGHT.get() == 12")
    }

    #[expect(dead_code, reason = "TODO: debug impl stuff")]
    /// # Safety
    /// Methods on [`ExternallySynchronizedRand32`] should not be called concurrently. That is,
    /// calls to this method must *not* race with other calls to methods of `self` (in particular,
    /// [`self.random_node_height()`] and [`self.debug(_)`]).
    ///
    /// [`self.random_node_height()`]: ExternallySynchronizedRand32::random_node_height
    /// [`self.debug(_)`]: ExternallySynchronizedRand32::debug
    pub unsafe fn debug(&self, f: &mut Formatter<'_>) -> FmtResult {
        let prng: *mut Rand32 = self.0.get();
        // SAFETY: we need to uphold the aliasing rules. Since we only access this field
        // in `Self::random_node_height` and `Self::debug`, both of which are marked `unsafe`
        // and require that the caller ensure that calls to them are not concurrent, and since
        // we never expose the PRNG reference outside those functions, we know that accesses to this
        // field must be unique. Therefore, we uphold the aliasing requirements for
        // an exclusive/mutable borrow.
        let prng = unsafe { &mut *prng };

        f.debug_tuple("ExternallySynchronizedRand32").field(prng).finish()
    }
}

// SAFETY: `Rand32` is `Send` since it's a POD struct (loosely speaking, not the `bytemuck` sense).
// We do not do any funky refcounted cloning. We can send this struct to other threads normally.
// Note that `Mutex<Rand32>: Send`.
unsafe impl Send for ExternallySynchronizedRand32 {}
// SAFETY: the only methods which can be called concurrently (based on their signatures) are marked
// `unsafe` and require, as safety preconditions, that they are not called concurrently.
// Therefore, allowing multiple threads to have a reference to the same
// `ExternallySynchronizedRand32` is not unsound. Note that `Mutex<Rand32>: Sync`.
unsafe impl Sync for ExternallySynchronizedRand32 {}
