use std::sync::Arc;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use generic_container::FragileContainer;

use crate::containers::{RefcountedFamily, RwCellFamily};


pub(crate) struct InternalCorruptionHandler<Refcounted: RefcountedFamily, RwCell: RwCellFamily> {
    // TODO: use proper error type instead of bool
    error:   Refcounted::Container<RwCell::Cell<bool>>,
    handler: Arc<InnerHandler<Refcounted, RwCell, dyn CorruptionHandler>>,
}

impl<Refcounted: RefcountedFamily, RwCell: RwCellFamily> Debug
for InternalCorruptionHandler<Refcounted, RwCell>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InternalCorruptionHandler")
            .field("error",   RwCell::debug(&self.error))
            .field("handler", &self.handler)
            .finish()
    }
}

// TODO: impl TableCorruptionHandler or whatever for this type
struct InnerHandler<Refcounted: RefcountedFamily, RwCell: RwCellFamily, CorruptionHandler: ?Sized> {
    error:        Refcounted::Container<RwCell::Cell<bool>>,
    user_handler: CorruptionHandler,
}

impl<Refcounted: RefcountedFamily, RwCell: RwCellFamily, CorruptionHandler: ?Sized> Debug
for InnerHandler<Refcounted, RwCell, CorruptionHandler>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InnerHandler")
            .field("error",        RwCell::debug(&self.error))
            .field("user_handler", &"<dyn CorruptionHandler>")
            .finish()
    }
}

pub trait CorruptionHandler {
    // TODO: provide information to corruption handler.
    // TODO: document potential panics or deadlocks on _all_ similar handlers, or just
    // in one centralized place.
    /// ## Potential Panics or Deadlocks
    /// This handler may be called while a lock in the database has been acquired. Do not call
    /// methods on the database from this function.
    fn corruption(&self);
}

impl<C: FragileContainer<dyn CorruptionHandler>> CorruptionHandler for C {
    fn corruption(&self) {
        let handler: &dyn CorruptionHandler = &*self.get_ref();
        handler.corruption();
    }
}
