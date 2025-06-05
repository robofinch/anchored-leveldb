use std::rc::Rc;

use super::Container;


impl<T> Container<T> for Rc<T> {
    const CONTAINER_NAME: &str = "RcContainer";

    #[inline]
    fn new_container(t: T) -> Self {
        Rc::new(t)
    }

    #[inline]
    fn into_inner(self) -> Option<T> {
        Rc::into_inner(self)
    }
}
