use std::fmt::Debug;
use std::ops::{Deref, DerefMut};


pub trait Container<T> {
    type Ref<'a>:  Deref<Target = T> where Self: 'a;
    type RefError: Debug;

    fn new_container(t: T) -> Self;
    fn into_inner(self) -> Option<T>;

    fn try_get_ref(&self) -> Result<Self::Ref<'_>, Self::RefError>;
}

pub trait MutableContainer<T>: Container<T> {
    type RefMut<'a>:  DerefMut<Target = T> where Self: 'a;
    type RefMutError: Debug;

    fn try_get_mut<'a>(&'a mut self) -> Result<Self::RefMut<'a>, Self::RefMutError>;
}
