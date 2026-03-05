use std::sync::{Mutex, MutexGuard, PoisonError};


pub(crate) trait UnwrapPoison<T: ?Sized> {
    fn lock_unwrapping_poison(&self, unwrap_poison: bool) -> MutexGuard<'_, T>;
}

impl<T: ?Sized> UnwrapPoison<T> for Mutex<T> {
    #[inline]
    fn lock_unwrapping_poison(&self, unwrap_poison: bool) -> MutexGuard<'_, T> {
        let poison_result = self.lock();

        if unwrap_poison {
            #[expect(
                clippy::expect_used,
                reason = "unwrapping poison is common, and if the user so chooses, \
                          they can instead ignore it",
            )]
            poison_result.expect("poisoned std::sync::Mutex")
        } else {
            poison_result.unwrap_or_else(PoisonError::into_inner)
        }
    }
}
