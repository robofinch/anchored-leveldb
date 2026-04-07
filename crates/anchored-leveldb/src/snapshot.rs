use std::sync::{Arc, Mutex};

use clone_behavior::{MirroredClone, Speed};

use crate::pub_typed_bytes::SequenceNumber;


// ================================================================
//  Public Snapshot type
// ================================================================

#[derive(Debug, Clone)]
pub struct Snapshot {
    inner: Arc<InnerSnapshot>,
}

impl Snapshot {
    /// `node_index` must be the index of a node in `list` dedicated to this snapshot.
    #[inline]
    #[must_use]
    fn new(
        sequence_number: SequenceNumber,
        node_index:      usize,
        list:            Arc<Mutex<SnapshotList>>,
    ) -> Self {
        Self {
            inner: Arc::new(InnerSnapshot {
                sequence_number,
                node_index,
                list,
            }),
        }
    }

    #[inline]
    #[must_use]
    pub(crate) fn sequence_number(&self) -> SequenceNumber {
        self.inner.sequence_number
    }
}

impl<S: Speed> MirroredClone<S> for Snapshot {
    fn mirrored_clone(&self) -> Self {
        self.clone()
    }
}

// ================================================================
//  Internal supporting types
// ================================================================

#[derive(Debug)]
struct InnerSnapshot {
    sequence_number: SequenceNumber,
    /// Must be the index of a node in `self.list` dedicated to this snapshot.
    node_index:      usize,
    list:            Arc<Mutex<SnapshotList>>,
}

impl Drop for InnerSnapshot {
    fn drop(&mut self) {
        // We only call this once, so it `self.node_index` should be in the list.
        SnapshotList::remove_snapshot(&self.list, self.node_index);
    }
}

#[derive(Debug, Clone, Copy)]
struct SnapshotListNode {
    /// If this node is in the free list, this value is a random old garbage value.
    value: SequenceNumber,
    /// The oldest newer node (possibly wrapping around to the oldest node).
    next:  usize,
    /// The newest older node (possibly wrapping around to the newest node).
    prev:  usize,
}

// ================================================================
//  The type managing Snapshots
// ================================================================

#[derive(Debug)]
pub(crate) struct SnapshotList {
    /// A circular doubly-linked list (with a series of `Some(SequenceNumber)` entries followed
    /// by a free list).
    snapshots:     Vec<SnapshotListNode>,
    /// Index of the oldest sequence number (if any). Equal to `usize::MAX` iff the list is empty.
    oldest: usize,
    /// Index of the newest sequence number (if any). Equal to `usize::MAX` iff the list is empty.
    newest: usize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl SnapshotList {
    /// Create a new empty list.
    #[inline]
    #[must_use]
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            snapshots: Vec::new(),
            oldest:    usize::MAX,
            newest:    usize::MAX,
        }))
    }

    /// Whether all created snapshots have been discarded (vacuously including the case where
    /// no snapshots have been created).
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.oldest == usize::MAX
    }

    /// The sequence number of the most-recently created [`Snapshot`] among those that have not
    /// been discarded.
    ///
    /// If the sequence numbers of new snapshots are nondecreasing, this is also the highest
    /// sequence number of any snapshot in the `SnapshotList`.
    #[must_use]
    pub fn newest_sequence_number(&self) -> Option<SequenceNumber> {
        self.snapshots.get(self.newest).map(|node| node.value)
    }

    /// The sequence number of the least-recently created [`Snapshot`] among those that have not
    /// been discarded.
    ///
    /// If the sequence numbers of new snapshots are nondecreasing, this is also the lowest
    /// sequence number of any snapshot in the `SnapshotList`.
    #[must_use]
    pub fn oldest_sequence_number(&self) -> Option<SequenceNumber> {
        self.snapshots.get(self.oldest).map(|node| node.value)
    }

    /// Create a new snapshot.
    ///
    /// There is no de-duplication and no checks that the given `sequence_number` is greater than
    /// or equal to the sequence numbers of previously-obtained snapshots.
    #[must_use]
    pub fn get_snapshot(
        list:            &Arc<Mutex<Self>>,
        sequence_number: SequenceNumber,
    ) -> Snapshot {
        #[expect(clippy::expect_used, reason = "poison can only arise from a bug in SnapshotList")]
        let mut list_mut = list.lock().expect("`SnapshotList`'s mutex was poisoned");
        let (oldest, newest) = (list_mut.oldest, list_mut.newest);

        let node_index = if let Some(&newest_node) = list_mut.snapshots.get(newest) {
            let newest_next = newest_node.next;
            #[expect(
                clippy::indexing_slicing,
                reason = "if `newest` is in-bounds, \
                          then `oldest` and `newest_next` should also be in-bounds",
            )]
            if newest_next == oldest {
                // List is full. Insert a new node after the newest node (and before the oldest
                // one).
                let new_node = list_mut.snapshots.len();
                list_mut.snapshots.push(SnapshotListNode {
                    value: sequence_number,
                    next:  oldest,
                    prev:  newest,
                });
                list_mut.snapshots[newest].next = new_node;
                list_mut.snapshots[oldest].prev = new_node;
                new_node
            } else {
                // The next element is part of the free list, which we can just convert into
                // part of the normal list.
                list_mut.snapshots[newest_next].value = sequence_number;
                newest_next
            }
        } else {
            if let Some(first) = list_mut.snapshots.first_mut() {
                // The entire list was the free list. We can just use the first node as the
                // oldest & newest node.
                first.value = sequence_number;
            } else {
                list_mut.snapshots.push(SnapshotListNode {
                    value: sequence_number,
                    next:  0,
                    prev:  0,
                });
            }
            list_mut.oldest = 0;
            0
        };
        list_mut.newest = node_index;

        // `node_index` is the index of a node in `list` which is dedicated to this snapshot.
        Snapshot::new(sequence_number, node_index, Arc::clone(list))
    }

    /// # Panics
    /// May panic if `node_index` is not the index of a node in the list.
    fn remove_snapshot(list: &Mutex<Self>, node_index: usize) {
        #[expect(clippy::expect_used, reason = "poison can only arise from a bug in SnapshotList")]
        let mut list_mut = list.lock().expect("`SnapshotList`'s mutex was poisoned");

        // Since `self.snapshots[node_index]` should be in the list `node_next` and `node_prev`
        // should be as well. Since the list is nonempty, `newest` (and `newest_next`) should
        // also be in the list.
        #[expect(clippy::indexing_slicing, reason = "`node_index` is assumed to be in the list")]
        match (node_index == list_mut.oldest, node_index == list_mut.newest) {
            (true, true) => {
                list_mut.oldest = usize::MAX;
                list_mut.newest = usize::MAX;
            }
            (true, false) => {
                let node_next = list_mut.snapshots[node_index].next;
                list_mut.oldest = node_next;
            }
            (false, true) => {
                let node_prev = list_mut.snapshots[node_index].prev;
                list_mut.newest = node_prev;
            }
            (false, false) => {
                // Remove the node
                let node_next = list_mut.snapshots[node_index].next;
                let node_prev = list_mut.snapshots[node_index].prev;
                list_mut.snapshots[node_next].prev = node_prev;
                list_mut.snapshots[node_prev].next = node_next;

                // Put the node into the start of the free list, just after the newest node.
                let newest = list_mut.newest;
                let newest_next = list_mut.snapshots[newest].next;
                list_mut.snapshots[newest].next = node_index;
                list_mut.snapshots[newest_next].prev = node_index;
            }
        }
    }
}
