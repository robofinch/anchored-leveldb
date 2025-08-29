#![expect(unsafe_code, reason = "Re-add Send and Sync impls removed by PhantomData")]

use std::marker::PhantomData;
use std::fmt::{Debug, Formatter, Result as FmtResult};


pub(super) struct TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>(
    PhantomData<(CompList, Policy, TableCmp, File, Cache, Pool)>,
);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<CompList, Policy, TableCmp, File, Cache, Pool>
    TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool> Debug
for TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("TableGenerics").field(&self.0).finish()
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool> Clone
for TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool> Copy
for TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{}

// Safety: we don't actually store any data in this struct
unsafe impl<CompList, Policy, TableCmp, File, Cache, Pool> Send
for TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{}

// Safety: we don't actually store any data in this struct
unsafe impl<CompList, Policy, TableCmp, File, Cache, Pool> Sync
for TableGenerics<CompList, Policy, TableCmp, File, Cache, Pool>
{}
