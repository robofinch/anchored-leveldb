use std::{error::Error, rc::Rc};
use std::{
    cell::{RefCell, RefMut},
    fmt::{Display, Formatter, Result as FmtResult},
};

use super::MutableContainer;


impl<T> MutableContainer<T> for Rc<RefCell<T>> {
    const MUT_CONTAINER_NAME: &str = "RcRefCellContainer";

    type Error = AlreadyBorrowed;
    type MutRef<'a> = RefMut<'a, T> where T: 'a;

    #[inline]
    fn new_mut_container(t: T) -> Self {
        Rc::new(RefCell::new(t))
    }

    #[inline]
    fn try_get_mut<'a>(&'a mut self) -> Result<Self::MutRef<'a>, Self::Error> {
        self.try_borrow_mut().map_err(|_| AlreadyBorrowed)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AlreadyBorrowed;

impl Display for AlreadyBorrowed {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "A RefCell used by an RcRefCellContainer was not available to be borrowed")
    }
}

impl Error for AlreadyBorrowed {}
