use std::sync::Arc;

use super::Container;


impl<T> Container<T> for Arc<T> {
    const CONTAINER_NAME: &str = "ArcContainer";

    #[inline]
    fn new_container(t: T) -> Self {
        Arc::new(t)
    }

    #[inline]
    fn into_inner(self) -> Option<T> {
        Arc::into_inner(self)
    }
}
