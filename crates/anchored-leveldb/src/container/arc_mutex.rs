use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::MutexPoisoned;

use super::MutableContainer;


impl<T> MutableContainer<T> for Arc<Mutex<T>> {
    const MUT_CONTAINER_NAME: &str = "ArcMutexContainer";

    type Error = MutexPoisoned;
    type MutRef<'a> = MutexGuard<'a, T> where T: 'a;

    #[inline]
    fn new_mut_container(t: T) -> Self {
        Arc::new(Mutex::new(t))
    }

    #[inline]
    fn try_get_mut<'a>(&'a mut self) -> Result<Self::MutRef<'a>, Self::Error> {
        Ok(self.lock()?)
    }
}
