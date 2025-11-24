use std::mem;
use std::ops::Deref;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::MirroredClone;
use generic_container::FragileTryContainer as _;

use crate::{
    containers::RefcountedFamily,
    leveldb_generics::LevelDBGenerics,
    leveldb_iter::InternalIter,
};
use crate::{
    file_tracking::{Level, StartSeekCompaction},
    inner_leveldb::{db_shared_access::DBSharedAccess, write_impl::DBWriteImpl},
};
use super::version_struct::Version;


#[derive(Debug, Clone, Copy)]
pub(crate) struct NeedsSeekCompaction {
    pub needs_seek_compaction: bool,
    pub version_is_current:    bool,
}

pub(crate) struct CurrentVersion<Refcounted: RefcountedFamily> {
    version:         Refcounted::Container<Version<Refcounted>>,
    /// If a certain level in the database is too large (that is, the total size in bytes of
    /// all files associated with a certain [`Level`] is too large), a "size compaction" needs to
    /// be performed in order to move data to a higher and larger level.
    ///
    /// A size compaction is never triggered on the maximum-numbered level.
    size_compaction: Option<Level>,
    seek_compaction: Option<StartSeekCompaction<Refcounted>>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily> CurrentVersion<Refcounted> {
    #[inline]
    #[must_use]
    pub fn new_empty() -> Self {
        Self {
            version:         Refcounted::Container::new_container(Version::new_empty()),
            size_compaction: None,
            seek_compaction: None,
        }
    }

    #[must_use]
    pub fn new(version: Version<Refcounted>) -> Self {
        let size_compaction = version.compute_size_compaction();
        Self {
            version:         Refcounted::Container::new_container(version),
            size_compaction,
            seek_compaction: None,
        }
    }

    /// Change the current version to `new_version`, and return the old version.
    #[must_use]
    pub fn set(
        &mut self,
        new_version: Version<Refcounted>,
    ) -> Refcounted::Container<Version<Refcounted>> {
        self.size_compaction = new_version.compute_size_compaction();
        self.seek_compaction = None;

        mem::replace(&mut self.version, Refcounted::Container::new_container(new_version))
    }

    #[must_use]
    pub const fn refcounted_version(&self) -> &Refcounted::Container<Version<Refcounted>> {
        &self.version
    }

    #[must_use]
    pub const fn size_compaction(&self) -> Option<Level> {
        self.size_compaction
    }

    #[must_use]
    pub const fn seek_compaction(&self) -> Option<&StartSeekCompaction<Refcounted>> {
        self.seek_compaction.as_ref()
    }

    #[must_use]
    pub fn needs_seek_compaction(
        &mut self,
        maybe_current_version: &Refcounted::Container<Version<Refcounted>>,
        start_seek_compaction: StartSeekCompaction<Refcounted>,
    ) -> NeedsSeekCompaction {
        if Refcounted::ptr_eq(&self.version, maybe_current_version) {
            if self.seek_compaction.is_none() {
                // We didn't already note that we need a seek compaction,
                // and it is actually this current version which needs a seek compaction.
                self.seek_compaction = Some(start_seek_compaction);
            }
            NeedsSeekCompaction {
                needs_seek_compaction: true,
                version_is_current:    true,
            }
        } else {
            NeedsSeekCompaction {
                needs_seek_compaction: self.seek_compaction.is_some(),
                version_is_current:    false,
            }
        }
    }

    #[must_use]
    pub const fn needs_compaction(&self) -> bool {
        self.size_compaction.is_some() || self.seek_compaction.is_some()
    }

    /// Append iterators over this version's files to the provided `iters` vector.
    ///
    /// In particular, an [`InternalIter::Table`] iterator is added for each level-0 file, and a
    /// [`InternalIter::Level`] iterator is added for each nonzero level.
    pub fn add_iterators<LDBG, WriteImpl>(
        &self,
        shared_data: &DBSharedAccess<LDBG, WriteImpl>,
        iters:       &mut Vec<InternalIter<LDBG, WriteImpl>>,
    )
    where
        LDBG:      LevelDBGenerics<Refcounted = Refcounted>,
        WriteImpl: DBWriteImpl<LDBG>,
    {
        Version::add_iterators(&self.version, shared_data, iters);
    }
}

impl<Refcounted: RefcountedFamily> Deref for CurrentVersion<Refcounted> {
    type Target = Version<Refcounted>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.version
    }
}

impl<Refcounted: RefcountedFamily> Debug for CurrentVersion<Refcounted> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CurrentVersion")
            .field("version",         Refcounted::debug(&self.version))
            .field("size_compaction", &self.size_compaction)
            .field("seek_compaction", &self.seek_compaction)
            .finish()
    }
}

pub(crate) struct OldVersions<Refcounted: RefcountedFamily> {
    old_versions:       Vec<Refcounted::WeakContainer<Version<Refcounted>>>,
    collection_counter: usize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily> OldVersions<Refcounted> {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            old_versions:       Vec::new(),
            collection_counter: 1,
        }
    }

    pub fn add_old_version(&mut self, version: Refcounted::Container<Version<Refcounted>>) {
        let weak_version = Refcounted::downgrade(&version);
        drop(version);
        if !Refcounted::can_be_upgraded(&weak_version) {
            // If `version` was the last reference to the old version, there's no need to
            // push it to `self.old_versions`. Do nothing.
            return;
        }

        self.maybe_collect_garbage();
        self.old_versions.push(weak_version);
        if self.collection_counter % 2 == 0 {
            self.collection_counter += 1;
        }
    }

    pub fn live(&mut self) -> impl Iterator<Item = Refcounted::Container<Version<Refcounted>>> {
        self.maybe_collect_garbage();
        self.old_versions.iter().filter_map(Refcounted::upgrade)
    }

    #[must_use]
    pub fn has_old_versions(&mut self) -> bool {
        self.collect_garbage();
        !self.old_versions.is_empty()
    }

    fn maybe_collect_garbage(&mut self) {
        if let Some(decremented) = self.collection_counter.checked_sub(1) {
            self.collection_counter = decremented;
        } else {
            self.collect_garbage();
        }
    }

    fn collect_garbage(&mut self) {
        self.old_versions.retain(Refcounted::can_be_upgraded);
        {
            #![expect(clippy::integer_division, reason = "intentional")]
            self.collection_counter = self.old_versions.len() / 2;
        }
    }
}

impl<Refcounted: RefcountedFamily> Debug for OldVersions<Refcounted> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        struct DebugInner<'a, Refcounted: RefcountedFamily>(
            &'a [Refcounted::WeakContainer<Version<Refcounted>>],
        );

        impl<Refcounted: RefcountedFamily> Debug for DebugInner<'_, Refcounted> {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                f.debug_list()
                    .entries(self.0.iter().map(|weak| {
                        if Refcounted::can_be_upgraded(weak) {
                            "(Live Version)"
                        } else {
                            "(Dead Version)"
                        }
                    }))
                    .finish()
            }
        }

        f.debug_struct("OldVersions")
            .field("old_versions",       &DebugInner::<Refcounted>(&self.old_versions))
            .field("collection_counter", &self.collection_counter)
            .finish()
    }
}
