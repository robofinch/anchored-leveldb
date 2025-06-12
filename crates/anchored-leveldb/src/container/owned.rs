use std::convert::Infallible;

use super::{Container, MutableContainer};


#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct Owned<T>(pub T);

impl<T> AsRef<T> for Owned<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> Container<T> for Owned<T> {
    const CONTAINER_NAME: &str = "OwnedContainer";

    #[inline]
    fn new_container(t: T) -> Self {
        Owned(t)
    }

    #[inline]
    fn into_inner(self) -> Option<T> {
        Some(self.0)
    }
}

impl<T> MutableContainer<T> for Owned<T> {
    const MUT_CONTAINER_NAME: &str = "OwnedContainer";

    type Error = Infallible;
    type MutRef<'a> = &'a mut T where T: 'a;

    #[inline]
    fn new_mut_container(t: T) -> Self {
        Owned(t)
    }

    #[inline]
    fn try_get_mut<'a>(&'a mut self) -> Result<Self::MutRef<'a>, Self::Error> {
        Ok(&mut self.0)
    }
}
