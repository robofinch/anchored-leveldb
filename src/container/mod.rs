mod arc;
mod rc;
mod arc_mutex;
mod rc_refcell;
mod owned;


pub use self::{owned::Owned, rc_refcell::AlreadyBorrowed};


use std::ops::DerefMut;
use std::fmt::{Debug, Formatter, Result as FmtResult};


pub trait Container<T>: AsRef<T> {
    const CONTAINER_NAME: &str;

    fn new_container(t: T) -> Self;
    fn into_inner(self) -> Option<T>;

    fn debug_fmt(&self, f: &mut Formatter<'_>) -> FmtResult
    where
        T: Debug,
    {
        write!(f, "{} holding: {:?}", Self::CONTAINER_NAME, self.as_ref())
    }
}

pub trait MutableContainer<T> {
    const MUT_CONTAINER_NAME: &str;

    type Error:      Debug;
    // TODO: determine whether / when RwLock is worth it instead of Mutex
    // type Ref<'a>:    Deref   <Target = T> where Self: 'a;
    type MutRef<'a>: DerefMut<Target = T> where Self: 'a;

    fn new_mut_container(t: T) -> Self;

    // fn try_get_ref<'a>(&'a mut self) -> Result<Self::Ref<'a>,    Self::Error>;
    fn try_get_mut<'a>(&'a mut self) -> Result<Self::MutRef<'a>, Self::Error>;

    fn debug_fmt(&self, f: &mut Formatter<'_>) -> FmtResult
    where
        T: Debug,
    {
        write!(f, "{} (a MutableContainer)", Self::MUT_CONTAINER_NAME)
    }
}
