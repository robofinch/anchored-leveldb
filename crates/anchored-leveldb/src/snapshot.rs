#![expect(clippy::module_name_repetitions, reason = "clarity; unsync what? list of what?")]

use std::mem;
use std::{cmp::Ordering, num::NonZeroU64};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{AnySpeed, ConstantTime, LogTime, MirroredClone};
use generic_container::FragileTryContainer as _;
use generic_container::kinds::{ArcKind, RcKind};

use crate::format::SequenceNumber;
use crate::containers::{
    MutexKind, RefCellKind, RefcountedFamily, FragileRwCell as _, RwCellFamily,
};


// ================================================================
//  Public Snapshot types
// ================================================================

pub type UnsyncSnapshot = Snapshot<RcKind, RefCellKind>;
pub type SyncSnapshot = Snapshot<ArcKind, MutexKind>;

pub struct Snapshot<Refcounted: RefcountedFamily, RwCell: RwCellFamily> {
    inner: Refcounted::Container<InnerSnapshot<Refcounted, RwCell>>,
}

impl<Refcounted, RwCell> Snapshot<Refcounted, RwCell>
where
    Refcounted: RefcountedFamily,
    RwCell:     RwCellFamily,
{
    #[inline]
    #[must_use]
    fn new(
        sequence_number: SequenceNumber,
        list:            Refcounted::Container<RwCell::Cell<SnapshotList<Refcounted, RwCell>>>,
    ) -> Self {
        Self {
            inner: Refcounted::Container::new_container(InnerSnapshot::new(sequence_number, list)),
        }
    }

    #[inline]
    #[must_use]
    pub(crate) fn sequence_number(&self) -> SequenceNumber {
        self.inner.sequence_number
    }
}

impl<Refcounted, RwCell> Debug for Snapshot<Refcounted, RwCell>
where
    Refcounted: RefcountedFamily,
    RwCell:     RwCellFamily,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Snapshot")
            .field("inner", Refcounted::debug(&self.inner))
            .finish()
    }
}

impl<Refcounted, RwCell> Clone for Snapshot<Refcounted, RwCell>
where
    Refcounted: RefcountedFamily,
    RwCell:     RwCellFamily,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.mirrored_clone(),
        }
    }
}

macro_rules! mirrored_clone {
    ($($speed:ident),*$(,)?) => {
        $(
            impl<Refcounted, RwCell> MirroredClone<$speed> for Snapshot<Refcounted, RwCell>
            where
                Refcounted: RefcountedFamily,
                RwCell:     RwCellFamily,
            {
                fn mirrored_clone(&self) -> Self {
                    Self {
                        inner: self.inner.mirrored_clone(),
                    }
                }
            }
        )*
    };
}

mirrored_clone!(ConstantTime, LogTime, AnySpeed);

// ================================================================
//  Internal supporting types
// ================================================================

struct InnerSnapshot<Refcounted: RefcountedFamily, RwCell: RwCellFamily> {
    sequence_number: SequenceNumber,
    list:            Refcounted::Container<RwCell::Cell<SnapshotList<Refcounted, RwCell>>>,
}

impl<Refcounted, RwCell> InnerSnapshot<Refcounted, RwCell>
where
    Refcounted: RefcountedFamily,
    RwCell:     RwCellFamily,
{
    #[inline]
    #[must_use]
    const fn new(
        sequence_number: SequenceNumber,
        list:            Refcounted::Container<RwCell::Cell<SnapshotList<Refcounted, RwCell>>>,
    ) -> Self {
        Self { sequence_number, list }
    }
}

impl<Refcounted, RwCell> Debug for InnerSnapshot<Refcounted, RwCell>
where
    Refcounted: RefcountedFamily,
    RwCell:     RwCellFamily,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InnerSnapshot")
            .field("sequence_number", &self.sequence_number)
            .field("list",            RwCell::debug(&self.list))
            .finish()
    }
}

impl<Refcounted, RwCell> Drop for InnerSnapshot<Refcounted, RwCell>
where
    Refcounted: RefcountedFamily,
    RwCell:     RwCellFamily,
{
    fn drop(&mut self) {
        SnapshotList::<Refcounted, RwCell>::remove_snapshot(
            &self.list,
            self.sequence_number,
        );
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SequenceNumberWithNiche(NonZeroU64);

#[expect(clippy::fallible_impl_from, reason = "is actually infallible")]
impl From<SequenceNumber> for SequenceNumberWithNiche {
    #[inline]
    fn from(sequence_number: SequenceNumber) -> Self {
        // Because `SequenceNumber::MAX_SEQUENCE_NUMBER` is just under 2^56, shifting it left
        // one bit does not overflow, nor does adding 1 to the shifted-in 0 bit.
        let modified_seq_num: u64 = (sequence_number.inner() << 1_u8) | 1;

        #[expect(clippy::unwrap_used, reason = "OR'd a 1 into the number, so is nonzero")]
        Self(NonZeroU64::new(modified_seq_num).unwrap())
    }
}

impl From<SequenceNumberWithNiche> for SequenceNumber {
    #[inline]
    fn from(sequence_number: SequenceNumberWithNiche) -> Self {
        // Shifting right one bit shifts out the 1 which was OR'd in above
        // and returns the inner sequence number to its original value, noting that
        // `SequenceNumber::MAX_SEQUENCE_NUMBER` is under 2^56 and thus has multiple leading zero
        // bits.
        Self::new_unchecked(sequence_number.0.get() >> 1)
    }
}

type MaybeSnapshotToDrop<Refcounted, RwCell> = Option<Snapshot<Refcounted, RwCell>>;

// ================================================================
//  The type managing Snapshots
// ================================================================

pub(crate) struct SnapshotList<Refcounted: RefcountedFamily, RwCell: RwCellFamily> {
    snapshots: Vec<Option<SequenceNumberWithNiche>>,
    /// Index of the oldest snapshot.
    ///
    /// Any entry in `snapshots` whose index strictly after `newest` and strictly before
    /// `oldest`, wrapping around the end of the `Vec`, should be `None`.
    ///
    /// The value is irrelevant if `newest` is `None`.
    oldest:        usize,
    /// The [`Snapshot`] with the newest and highest sequence number, followed by the index
    /// of the corresponding entry in `snapshots`.
    newest:        Option<(Snapshot<Refcounted, RwCell>, usize)>,
    /// Number of entries in `snapshots` which are `Some`.
    ///
    /// Should be zero if and only if `newest` is `None`.
    num_snapshots: usize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted, RwCell> SnapshotList<Refcounted, RwCell>
where
    Refcounted: RefcountedFamily,
    RwCell:     RwCellFamily,
{
    #[inline]
    #[must_use]
    pub fn new() -> Refcounted::Container<RwCell::Cell<Self>> {
        Refcounted::Container::new_container(RwCell::Cell::new_rw_cell(Self {
            snapshots:     Vec::new(),
            oldest:        0,
            newest:        None,
            num_snapshots: 0,
        }))
    }

    #[must_use]
    pub fn newest_sequence_number(&self) -> Option<SequenceNumber> {
        Some(self.newest.as_ref()?.0.sequence_number())
    }

    #[must_use]
    pub fn oldest_sequence_number(&self) -> Option<SequenceNumber> {
        if self.newest.is_none() {
            None
        } else {
            #[expect(
                clippy::indexing_slicing,
                clippy::unwrap_used,
                reason = "`oldest` always points to an in-bounds `Some` if `newest` is not `None`",
            )]
            Some(SequenceNumber::from(self.snapshots[self.oldest].unwrap()))
        }
    }

    #[must_use]
    pub fn get_snapshot(
        list:            &Refcounted::Container<RwCell::Cell<Self>>,
        sequence_number: SequenceNumber,
    ) -> Option<Snapshot<Refcounted, RwCell>> {
        let mut this_mut = list.write();
        let this: &mut Self = &mut this_mut;
        let list_handle = list.mirrored_clone();

        let snapshots = this.inner_get_snapshot(list_handle, sequence_number);

        // We must ensure that the fragile RwCell has no live references when any old
        // snapshot is dropped.
        drop(this_mut);
        snapshots.map(|(_old_snapshot_to_drop, snapshot)| snapshot)
    }

    fn remove_snapshot(
        list:            &Refcounted::Container<RwCell::Cell<Self>>,
        sequence_number: SequenceNumber,
    ) {
        let mut this_mut = list.write();
        let this: &mut Self = &mut this_mut;

        let maybe_snapshot_to_drop = this.inner_remove_snapshot(sequence_number);

        // We must ensure that the fragile RwCell has no live references when any old
        // snapshot is dropped.
        drop(this_mut);
        drop(maybe_snapshot_to_drop);
    }

    /// Returns `None` if `sequence_number` is older than some of the sequence numbers of
    /// previously-taken snapshots.
    ///
    /// Otherwise, returns `Some((maybe_old_newest_snapshot, snapshot_to_return))`.
    ///
    /// Because [`SnapshotList`]s are stored in fragile mutable containers, and dropping a
    /// [`Snapshot`] attempts to mutate its parent [`SnapshotList`], the borrow on the
    /// [`SnapshotList`]'s container must be ended before any [`Snapshot`] may be dropped.
    /// `maybe_old_newest_snapshot` may contain a [`Snapshot`] which needs to be dropped.
    ///
    /// The second part of the tuple is the snapshot of the indicated sequence number.
    #[must_use]
    fn inner_get_snapshot(
        &mut self,
        list_handle:     Refcounted::Container<RwCell::Cell<Self>>,
        sequence_number: SequenceNumber,
    ) -> Option<(MaybeSnapshotToDrop<Refcounted, RwCell>, Snapshot<Refcounted, RwCell>)> {
        if let Some(newest) = &mut self.newest {
            match newest.0.sequence_number().cmp(&sequence_number) {
                Ordering::Less => {
                    let next_snapshot = Snapshot::new(sequence_number, list_handle);

                    // Note that `self.snapshots.len() > 0` because `newest` should refer to
                    // something in `self.snapshots`.
                    let pre_compaction_next_idx = (newest.1 + 1) % self.snapshots.len();
                    let (newest, next_idx) = if pre_compaction_next_idx >= self.oldest {
                        // Compact and expand
                        self.expand_length();
                        #[expect(
                            clippy::unwrap_used,
                            reason = "compaction should not remove `Some` elements",
                        )]
                        let new_newest = self.newest.as_mut().unwrap();
                        let post_compaction_next_idx = (new_newest.1 + 1) % self.snapshots.len();
                        (new_newest, post_compaction_next_idx)
                    } else {
                        (newest, pre_compaction_next_idx)
                    };

                    #[expect(
                        clippy::indexing_slicing,
                        reason = "index has `% self.snapshots.len()` applied to it",
                    )]
                    {
                        self.snapshots[next_idx] = Some(SequenceNumberWithNiche::from(
                            sequence_number,
                        ));
                    };
                    self.num_snapshots += 1;
                    let (old_newest, _) = mem::replace(newest, (next_snapshot.clone(), next_idx));

                    Some((Some(old_newest), next_snapshot))
                }
                Ordering::Equal => Some((None, newest.0.clone())),
                Ordering::Greater => None,
            }
        } else {
            let first_snapshot = Snapshot::new(sequence_number, list_handle);

            if self.snapshots.is_empty() {
                self.snapshots.resize(4, None);
            }

            #[expect(
                clippy::indexing_slicing,
                reason = "self.snapshots cannot be empty after either above branch",
            )]
            {
                self.snapshots[0] = Some(SequenceNumberWithNiche::from(sequence_number));
            };
            self.num_snapshots += 1;
            self.oldest = 0;
            self.newest = Some((first_snapshot.clone(), 0));
            Some((None, first_snapshot))
        }
    }

    #[must_use]
    fn inner_remove_snapshot(
        &mut self,
        sequence_number: SequenceNumber,
    ) -> MaybeSnapshotToDrop<Refcounted, RwCell> {
        if let Some((newest_snapshot, newest_idx)) = &self.newest {
            if newest_snapshot.sequence_number() == sequence_number {
                #[expect(
                    clippy::indexing_slicing,
                    reason = "the index in `self.newest` (if `Some`) should be valid",
                )]
                {
                    self.snapshots[*newest_idx] = None;
                };
                self.num_snapshots -= 1;
                self.newest.take().map(|(snapshot_to_drop, _)| snapshot_to_drop)
            } else {
                let sequence_number = SequenceNumberWithNiche::from(sequence_number);

                let snapshot_idx = if self.oldest <= *newest_idx {
                    // Everything's in one piece
                    #[expect(clippy::indexing_slicing, reason = "see reasoning below")]
                    {
                        self.oldest + binary_search(
                            &self.snapshots[self.oldest..=*newest_idx],
                            sequence_number,
                        )?
                    }
                } else {
                    // [oldest..newest] wraps around the end of the list
                    #[expect(
                        clippy::indexing_slicing,
                        reason = "`self.newest` is `Some`, so `self.oldest` should be valid",
                    )]
                    let idx_from_oldest = binary_search(
                        &self.snapshots[self.oldest..],
                        sequence_number,
                    );

                    #[expect(
                        clippy::indexing_slicing,
                        reason = "the index in `self.newest` (if `Some`) should be valid",
                    )]
                    let idx_from_zero = binary_search(
                        &self.snapshots[..=*newest_idx],
                        sequence_number,
                    );

                    match (idx_from_oldest, idx_from_zero) {
                        (Some(idx_from_oldest), _) => self.oldest + idx_from_oldest,
                        (_, Some(idx_from_zero))   => idx_from_zero,
                        (None, None)               => return None,
                    }
                };

                #[expect(
                    clippy::indexing_slicing,
                    reason = "`snapshot_idx` is in the range `self.oldest..=*newest_idx",
                )]
                {
                    self.snapshots[snapshot_idx] = None;
                };
                self.num_snapshots -= 1;

                // This loop will halt at `self.newest` at the latest
                #[expect(
                    clippy::indexing_slicing,
                    reason = "`self.newest` is `Some`, so `self.oldest` should be valid",
                )]
                while self.snapshots[self.oldest].is_none() {
                    // The `expect` reason is for the first iteration. In later iterations,
                    // the modulus makes it valid.
                    self.oldest = (self.oldest + 1) % self.snapshots.len();
                }

                self.maybe_compact();

                None
            }
        } else {
            None
        }
    }

    fn expand_length(&mut self) {
        if self.snapshots.len() < 4 {
            self.snapshots.resize(4, None);
        } else {
            self.snapshots.resize(2 * self.snapshots.len(), None);
        }
        self.maybe_compact();
    }

    fn maybe_compact(&mut self) {
        let Some((_, newest_idx_mut)) = &mut self.newest else {
            return;
        };
        let newest_idx = *newest_idx_mut;

        let width = if self.oldest <= newest_idx {
            newest_idx - self.oldest + 1
        } else {
            1 + newest_idx + (self.snapshots.len() - self.oldest)
        };

        // If the snapshot list is too wide compared to how many snapshots are stored in it,
        // compact the list to make future removals more efficient.
        #[expect(clippy::integer_division, reason = "intentional; rough heuristic")]
        if width > 4 && self.num_snapshots / 4 < width {
            let mut compacted_idx = self.oldest;
            let mut uncompacted_idx = (self.oldest + 1) % self.snapshots.len();

            // At worst, we will wrap around to the start and halt.
            while uncompacted_idx != self.oldest {
                #[expect(clippy::indexing_slicing, reason = "we take the indexes mod slice length")]
                if let Some(sequence_number) = self.snapshots[uncompacted_idx].take() {
                    compacted_idx = (compacted_idx + 1) % self.snapshots.len();
                    self.snapshots[compacted_idx] = Some(sequence_number);

                    if uncompacted_idx == newest_idx {
                        *newest_idx_mut = compacted_idx;
                        // Everything after `newest` will be `None` up until we wrap back to
                        // the oldest.
                        return;
                    }
                }

                uncompacted_idx = (uncompacted_idx + 1) % self.snapshots.len();
            }
        }
    }
}

#[must_use]
fn binary_search(
    snapshots:       &[Option<SequenceNumberWithNiche>],
    sequence_number: SequenceNumberWithNiche,
) -> Option<usize> {
    let mut lower_bound = 0;
    let mut upper_bound = snapshots.len().checked_sub(1)?;

    'outer: while lower_bound <= upper_bound {
        let initial_midpoint = lower_bound.midpoint(upper_bound);
        let mut final_midpoint = initial_midpoint;

        let midpoint_sequence = loop {
            #[expect(
                clippy::indexing_slicing,
                reason = "we index only if `final_midpoint <= upper_bound < snapshots.len()`",
            )]
            if final_midpoint > upper_bound {
                upper_bound = initial_midpoint.checked_sub(1)?;
                continue 'outer;
            } else if let Some(midpoint_sequence) = snapshots[final_midpoint] {
                break midpoint_sequence;
            } else {
                final_midpoint += 1;
            }
        };

        match midpoint_sequence.cmp(&sequence_number) {
            Ordering::Less => {
                lower_bound = final_midpoint + 1;
            }
            Ordering::Equal => return Some(final_midpoint),
            Ordering::Greater => {
                upper_bound = initial_midpoint.checked_sub(1)?;
            }
        }
    }

    None
}

impl<Refcounted, RwCell> Debug for SnapshotList<Refcounted, RwCell>
where
    Refcounted: RefcountedFamily,
    RwCell:     RwCellFamily,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("SnapshotList")
            .field("snapshots",     &self.snapshots)
            .field("oldest",        &self.oldest)
            .field("newest",        &self.newest)
            .field("num_snapshots", &self.num_snapshots)
            .finish()
    }
}
