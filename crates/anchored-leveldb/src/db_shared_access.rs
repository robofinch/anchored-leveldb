#![expect(unsafe_code, reason = "cast an inner type reference to a transparent wrapper reference")]

use std::ops::Deref;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use new_clone_behavior::{FastMirroredClone as _, MirroredClone, Speed};

use crate::{db_data::DBShared, generic_db::InnerGenericDB};
use crate::leveldb_generics::{LdbPooledBuffer, LdbSharedWriteData, LevelDBGenerics};


/// Access only the shared data of a database, in a reference-counted container.
#[repr(transparent)]
pub(crate) struct DBSharedAccess<LDBG: LevelDBGenerics>(InnerGenericDB<LDBG>);

impl<LDBG: LevelDBGenerics> DBSharedAccess<LDBG> {
    #[inline]
    #[must_use]
    pub(crate) const fn from_ref(db: &InnerGenericDB<LDBG>) -> &Self {
        let db: *const InnerGenericDB<LDBG> = db;
        let this: *const Self = db.cast();

        // SAFETY: since `DBSharedAccess` is #[repr(transparent)] without any additional
        // alignment requirements, `this` is a non-null and properly-aligned pointer to memory
        // dereferenceable for `size_of::<Self>()` bytes, pointing to a valid value of `Self`,
        // on the basis that `db` is a non-null and properly-aligned pointer to a type
        // whose alignment, size, and valid bit patterns are the same as for valid values of `Self`.
        // Lastly, the output lifetime is the same as the input lifetime, and both are
        // shared references, so Rust's aliasing rules are satisfied.
        unsafe { &*this }
    }
}

impl<LDBG: LevelDBGenerics> Deref for DBSharedAccess<LDBG> {
    type Target = DBShared<LDBG>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.shared()
    }
}

impl<LDBG: LevelDBGenerics> Clone for DBSharedAccess<LDBG> {
    #[inline]
    fn clone(&self) -> Self {
        self.fast_mirrored_clone()
    }
}

impl<LDBG: LevelDBGenerics, S: Speed> MirroredClone<S> for DBSharedAccess<LDBG> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.fast_mirrored_clone())
    }
}

impl<LDBG> Debug for DBSharedAccess<LDBG>
where
    LDBG:                     LevelDBGenerics,
    LDBG::FS:                 Debug,
    LDBG::Policy:             Debug,
    LDBG::Cmp:                Debug,
    LDBG::Pool:               Debug,
    LdbPooledBuffer<LDBG>:    Debug,
    LdbSharedWriteData<LDBG>: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("DBSharedAccess").field(self.0.shared()).finish()
    }
}
