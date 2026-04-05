use std::sync::PoisonError;


pub(crate) trait UnwrapPoison<T> {
    fn unwrap_poison(self, unwrap_poison: bool) -> T;
}

impl<T> UnwrapPoison<T> for Result<T, PoisonError<T>> {
    fn unwrap_poison(self, unwrap_poison: bool) -> T {
        if unwrap_poison {
            #[expect(
                clippy::expect_used,
                reason = "unwrapping poison is common, and if the user so chooses, \
                          they can instead ignore it",
            )]
            self.expect("poisoned std::sync::Mutex")
        } else {
            self.unwrap_or_else(PoisonError::into_inner)
        }
    }
}
