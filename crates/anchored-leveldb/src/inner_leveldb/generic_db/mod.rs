// These modules do not define new types; they are solely for categorizing the
// `impl` blocks of `InnerGenericDB`.
mod init_destruct;
mod put_delete_get;
mod other_read_write;
mod debug_and_stats;


use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::MirroredClone as _;
use new_clone_behavior::{FastMirroredClone as _, MirroredClone, Speed};

use crate::table_traits::InternalComparator;
use crate::{
    containers::{FragileRwCell as _, RwCellFamily as _},
    leveldb_generics::{
        LdbContainer, LdbFullShared, LdbLockedFullShared, LdbPooledBuffer, LdbRwCell,
        LevelDBGenerics,
    },
};
use super::{db_shared_access::DBSharedAccess, write_impl::DBWriteImpl};
use super::{db_data::{DBShared, DBSharedMutable}};


pub(crate) struct InnerGenericDB<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>>(
    #[expect(clippy::type_complexity, reason = "a bunch of type aliases are used to simplify it")]
    LdbContainer<LDBG, (
        DBShared<LDBG, WriteImpl>,
        LdbRwCell<LDBG, DBSharedMutable<LDBG, WriteImpl>>,
    )>
);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    #[inline]
    #[must_use]
    pub fn shared(&self) -> &DBShared<LDBG, WriteImpl> {
        &self.0.0
    }

    #[inline]
    #[must_use]
    pub fn cmp(&self) -> &InternalComparator<LDBG::Cmp> {
        &self.0.0.table_options.comparator
    }

    #[inline]
    #[must_use]
    pub const fn shared_access(&self) -> &DBSharedAccess<LDBG, WriteImpl> {
        DBSharedAccess::from_ref(self)
    }
}

// Temporary implementations without corruption handlers
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    #[inline]
    #[must_use]
    pub(super) fn ldb_shared(&self) -> LdbFullShared<'_, LDBG, WriteImpl> {
        (self.shared(), self.shared_mutable())
    }

    #[inline]
    #[must_use]
    pub(super) fn ldb_locked_shared(&self) -> LdbLockedFullShared<'_, LDBG, WriteImpl> {
        (self.shared(), self.shared_mutable().write())
    }

    #[inline]
    #[must_use]
    pub(super) fn shared_mutable(&self) -> &LdbRwCell<LDBG, DBSharedMutable<LDBG, WriteImpl>> {
        &self.0.1
    }
}

impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> Clone
for InnerGenericDB<LDBG, WriteImpl>
{
    #[inline]
    fn clone(&self) -> Self {
        self.fast_mirrored_clone()
    }
}

impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>, S: Speed> MirroredClone<S>
for InnerGenericDB<LDBG, WriteImpl>
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.fast_mirrored_clone())
    }
}

impl<LDBG, WriteImpl> Debug for InnerGenericDB<LDBG, WriteImpl>
where
    LDBG:                     LevelDBGenerics,
    LDBG::FS:                 Debug,
    LDBG::Skiplist:           Debug,
    LDBG::Policy:             Debug,
    LDBG::Cmp:                Debug,
    LDBG::Pool:               Debug,
    LdbPooledBuffer<LDBG>:    Debug,
    WriteImpl:                DBWriteImpl<LDBG>,
    WriteImpl::Shared:        Debug,
    WriteImpl::SharedMutable: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("DB")
            .field(&self.0.0)
            .field(LDBG::RwCell::debug(&self.0.1))
            .finish()
    }
}
