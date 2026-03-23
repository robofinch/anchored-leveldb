use std::mem;
use std::ops::Deref;
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::{Arc, Weak},
};

use crate::{
    file_tracking::StartSeekCompaction,
    options::pub_options::SizeCompactionOptions,
    pub_typed_bytes::Level,
};
use super::version_struct::Version;


#[derive(Debug, Clone, Copy)]
pub(crate) struct NeedsSeekCompaction {
    pub needs_seek_compaction: bool,
    pub version_is_current:    bool,
}

#[derive(Debug)]
pub(crate) struct CurrentVersion {
    version:        Arc<Version>,
    /// If a certain level in the database is too large (that is, the total size in bytes of
    /// all files associated with a certain [`Level`] is too large), a "size compaction" needs to
    /// be performed in order to move data to a higher and larger level.
    ///
    /// A size compaction is never triggered on the maximum-numbered level.
    size_compaction: Option<Level>,
    seek_compaction: Option<StartSeekCompaction>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl CurrentVersion {
    #[inline]
    #[must_use]
    pub fn new_empty() -> Self {
        Self {
            version:         Arc::new(Version::new_empty()),
            size_compaction: None,
            seek_compaction: None,
        }
    }

    #[must_use]
    pub fn new(version: Version, size_opts: SizeCompactionOptions) -> Self {
        let size_compaction = version.compute_size_compaction(size_opts);
        Self {
            version:         Arc::new(version),
            size_compaction,
            seek_compaction: None,
        }
    }

    /// Change the current version to `new_version`, and return the old version.
    #[must_use]
    pub fn set(
        &mut self,
        new_version: Version,
        size_opts:   SizeCompactionOptions
    ) -> Arc<Version> {
        self.size_compaction = new_version.compute_size_compaction(size_opts);
        self.seek_compaction = None;

        mem::replace(&mut self.version, Arc::new(new_version))
    }

    #[must_use]
    pub const fn version(&self) -> &Arc<Version> {
        &self.version
    }

    #[must_use]
    pub const fn size_compaction(&self) -> Option<Level> {
        self.size_compaction
    }

    #[must_use]
    pub const fn seek_compaction(&self) -> Option<&StartSeekCompaction> {
        self.seek_compaction.as_ref()
    }

    #[must_use]
    pub fn needs_seek_compaction(
        &mut self,
        maybe_current_version: &Arc<Version>,
        start_seek_compaction: StartSeekCompaction,
    ) -> NeedsSeekCompaction {
        if Arc::ptr_eq(&self.version, maybe_current_version) {
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
}

impl Deref for CurrentVersion {
    type Target = Version;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.version
    }
}

pub(crate) struct OldVersions {
    old_versions:       Vec<Weak<Version>>,
    collection_counter: usize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl OldVersions {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            old_versions:       Vec::new(),
            collection_counter: 1,
        }
    }

    pub fn add_old_version(&mut self, version: Arc<Version>) {
        let weak_version = Arc::downgrade(&version);
        drop(version);
        if weak_version.strong_count() == 0 {
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

    pub fn live(&mut self) -> impl Iterator<Item = Arc<Version>> {
        self.maybe_collect_garbage();
        self.old_versions.iter().filter_map(Weak::upgrade)
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
        self.old_versions.retain(|weak| weak.strong_count() > 0);
        {
            #![expect(clippy::integer_division, reason = "intentional")]
            self.collection_counter = self.old_versions.len() / 2;
        }
    }
}

impl Debug for OldVersions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        struct DebugInner<'a>(&'a [Weak<Version>]);

        impl Debug for DebugInner<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                f.debug_list()
                    .entries(self.0.iter().map(|weak| {
                        if weak.strong_count() > 0 {
                            "(Live Version)"
                        } else {
                            "(Dead Version)"
                        }
                    }))
                    .finish()
            }
        }

        f.debug_struct("OldVersions")
            .field("old_versions",       &DebugInner(&self.old_versions))
            .field("collection_counter", &self.collection_counter)
            .finish()
    }
}
