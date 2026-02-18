//! The node format used by this crate's skiplist is, semantically, a custom DST with
//! two unsized fields whose length metadata is included within the node allocation, such that
//! pointers to nodes are thin (have size `usize`). Since this is not a proper "type", per se,
//! its parts shall be referred to as "components" rather than fields.
//!
//! The format is as follows (with the semantic meaning, size in bytes, and type of each component):
//!
//! <pre overflow-x: scroll>
//! ┌────────────────────────┬────────────────┬────┬───────────┬───────────┬────────────────────────┬────────────────────────┐
//! │              padding_0 │ skip[height-1] │ .. │   skip[0] │    height │              padding_1 │              user_data │
//! │    computed at runtime │      ptr-sized │ .. │ ptr-sized │         1 │   computed at comptime │ unknown, user_data_len │
//! │ [MaybeUninit< u8 >; R] │      AtomicPtr │ .. │ AtomicPtr │ NonZeroU8 │ [MaybeUninit< u8 >; N] │           user-defined │
//! └────────────────────────┴────────────────┴────┴───────────┴─────────│─┴────────────────────────┴────────────────────────┘
//! A pointer to a node points to the `height` component here ───────────┘
//! </pre>
//!
//! # Overall invariants
//! ## Pointers to nodes
//! As indicated above, a pointer to a node (in the [`Link`] or [`NodeRef`] wrappers for
//! [`AtomicPtr`] or [`NonNull`]) points to the `height` component in particular, rather than the
//! start of the node allocation. This ensures that the node format is self-describing *and* that
//! the offset from the node pointer to desired components can be more easily computed. In
//! particular, the offsets of `user_data` and `skip[0]` (which is the link to the next node in the
//! skiplist, if any) do not depend on the value of `height`.
//!
//! ## Aliasing
//! From the time that a pointer to the node is first published up until the time that the
//! [`Bump`] of the node is dropped or reset, all components of the node should be considered to be
//! immutably aliased; that is, shared aliasing rules apply. In particular, the initialization
//! of `height` and `user_data` must strictly happen-before (in the atomic sense) a pointer to the
//! node is published, and we can also take the opportunity to perform non-atomic writes to the
//! `skip[_]` components before publishing the node.
//!
//! In particular, after the creation and publishing of a node, we only ever:
//! - read the `height` component,
//! - perform atomic accesses on `skip[_]` components (which may, via internal mutability,
//!   perform writes), and
//! - execute [`decode_entry`] or [`decode_key`] on `user_data`. Note that the user is informed that
//!   those methods only have shared access to the data.
//!
//! ## Alignment
//! The addresses of every node allocation, `skip[_]` component, and `height` component are
//! guaranteed to be multiples of the align of [`AtomicPtr<()>`]. Additionally, the address of the
//! `user_data` component must be a multiple of [`ENTRY_ALIGN`]. The alignment of the node
//! allocation is set to the larger of the two alignments. There are two main cases for how
//! `N` and `R`, the length of the `padding_1` and `padding_0` components, are chosen in order
//! to ensure a node's components are properly aligned.
//!
//! - If `ENTRY_ALIGN <= LINK_ALIGN`, then `R = 0`, `N = ENTRY_ALIGN - 1`, and the node allocation
//!   is aligned to `LINK_ALIGN`. The `skip[_]` and `height` components are then aligned to
//!   `LINK_ALIGN` since `padding_0` is 0-sized and `LINK_SIZE` is a multiple of `LINK_ALIGN`.
//!   Since alignments are powers of two, their addresses are also multiples of `ENTRY_ALIGN`. The
//!   address of `user_data` is thus the sum of the address of `height` (`k * ENTRY_ALIGN`), the
//!   size of `height` (`1`), and the size of `padding_1` (`ENTRY_ALIGN - 1`), which is
//!   `(k+1) * ENTRY_ALIGN`. Thus, `user_data` is aligned to `ENTRY_ALIGN`.
//!
//! - If `ENTRY_ALIGN > LINK_ALIGN`, then `N = LINK_ALIGN - 1`,
//!   `R = header_size.next_multiple_of(ENTRY_ALIGN) - header_size` where
//!   `header_size = height * LINK_SIZE + LINK_ALIGN`, and the node allocation is aligned to
//!   `ENTRY_ALIGN`. The address of `user_data` is equal to sum of the address of the node
//!   allocation (`k * ENTRY_ALIGN`), the size of `padding_0` (`R`), the total size of the
//!   `height`-many `skip[_]` components (`height * LINK_SIZE`), the size of `height` (`1`),
//!   and the size of `padding_1` (`LINK_ALIGN - 1`). The sum is
//!   `k * ENTRY_ALIGN + R + height * LINK_SIZE + LINK_ALIGN == k * ENTRY_ALIGN + R + header_size`,
//!   which equals `k * ENTRY_ALIGN + header_size.next_multiple_of(ENTRY_ALIGN)`, which is
//!   a multiple of `ENTRY_ALIGN`. Therefore, the `user_data` component is aligned to `ENTRY_ALIGN`.
//!
//!   The `height` component, then, is at offset `-N - 1 == -LINK_ALIGN` from `user_data`, which is
//!   aligned to `ENTRY_ALIGN` and thus also `LINK_ALIGN`. Since `LINK_SIZE` is a multiple of
//!   `LINK_ALIGN` and the address of `skip[i]` plus `(i+1) * LINK_SIZE` equals the address of
//!   `height`, we then have that every `skip[_]` component is aligned to `LINK_ALIGN`.
//!
//! # Invariants of each component
//! ## `padding_0` and `padding_1`
//! See above for the `R` and `N` values. These components are never read or written (well, never
//! read or written as anything other than `MaybeUninit` data).
//!
//! ## `skip[_]`
//! Each skip link must either be a null pointer or a pointer to the `height` component of another
//! node. For convenience, a [`Link`] wrapper is used.
//!
//! Each skiplist is required to use one [`Bump`] for all of its node allocations; in particular,
//! if `node_a` is referred to by one of the skip links of `node_b`, then they must be allocated in
//! the same [`Bump`].
//!
//! ### Non-safety ~~invariants~~ conventions
//! If `skip[i]` is a `Some(NodeRef)` link, then for each `j <= i`, `skip[j]` should also be
//! a `Some(NodeRef)` link. This *cannot* be an invariant, but is the steady-state behavior
//! of the program. It is, however, temporarily violated while a node is being published, and
//! concurrent readers may notice this convention be violated.
//!
//! Note that `skip[0]` is the link to the next node (if any), `skip[1]` skips 3 nodes on average,
//! `skip[2]` skips 15 nodes on average, `skip[3]` skips 63 nodes on average, and so on.
//!
//! ## `height`
//! The `height` component must be the number of skip links in the node.
//!
//! ### Non-safety invariant
//! The `height` should be at most [`MAX_HEIGHT`], which is 12. Unlike the "convention" for
//! `skip[_]` fields, panics may occur if this condition is violated.
//!
//! ## `user_data`
//! The `user_data` component must be valid user data, sound to pass to [`decode_entry`] or
//! [`decode_key`] *aside from* considerations of [`Sync`]ness.
//!
//! [`Bump`]: bumpalo::Bump
//! [`ENTRY_ALIGN`]: SkiplistFormat::ENTRY_ALIGN
//! [`decode_entry`]: SkiplistFormat::decode_entry
//! [`decode_key`]: SkiplistFormat::decode_key
//! [`NonNull`]: core::ptr::NonNull
//! [`Link`]: super::ref_and_link::Link
//! [`NodeRef`]: super::ref_and_link::NodeRef

use core::{alloc::Layout, num::NonZeroU8};

use variance_family::UpperBound;

use crate::{interface::SkiplistFormat, maybe_loom::AtomicPtr};
use super::super::heights::MAX_HEIGHT;


pub(super) const LINK_SIZE: usize = size_of::<AtomicPtr<u8>>();
pub(in super::super) const LINK_ALIGN: usize = align_of::<AtomicPtr<u8>>();

/// This seems like a stable guarantee, but it doesn't hurt to doubly-confirm it.
const _: () = assert!(LINK_SIZE % LINK_ALIGN == 0, "A type's align should divide its size");

/// If `ENTRY_ALIGN` is a power of two, returns the alignment that should be used for
/// node allocations of a skiplist with format `F`, in accord with the [module-level docs].
///
/// Otherwise, this function is merely guaranteed to successfully evaluate.
///
/// [module-level docs]: self
#[inline]
#[must_use]
pub(super) const fn node_align<F: SkiplistFormat<U>, U: UpperBound>() -> usize {
    if F::ENTRY_ALIGN.get() <= LINK_ALIGN {
        LINK_ALIGN
    } else {
        F::ENTRY_ALIGN.get()
    }
}

/// If `ENTRY_ALIGN` is a power of two, returns the size (in bytes) that should be used for
/// the `padding_0` component of node allocations of a skiplist with format `F`, in accord with the
/// [module-level docs].
///
/// Otherwise, this function is merely guaranteed to successfully evaluate.
///
/// [module-level docs]: self
#[inline]
#[must_use]
pub(super) fn padding_0_r<F: SkiplistFormat<U>, U: UpperBound>(
    node_height: NonZeroU8,
) -> usize {
    if F::ENTRY_ALIGN.get() <= LINK_ALIGN {
        0
    } else {
        // On 64-bit systems, `MAX_HEIGHT * LINK_SIZE + LINK_ALIGN == 104`,
        // which is nowhere near overflowing `u16` or `usize`. It should be provable that for no
        // possible pointer size of at least 8 bits does this overflow, unless
        // `loom::sync::AtomicPtr` were absurdly massive.
        let header_size = usize::from(node_height.get()) * LINK_SIZE + LINK_ALIGN;

        // Note that `header_size` is strictly less than `1 << 15` and that `ENTRY_ALIGN != 0`.
        // We will show that the next line of code does not panic, regardless of whether
        // `ENTRY_ALIGN` is a valid alignment.
        // - If `ENTRY_ALIGN < (1 << 15)`, then since the interval `[1 << 15, usize::MAX]`
        //   is at least `1 << 15` integers wide, some multiple of `ENTRY_ALIGN` must fall
        //   in that interval. That multiple is greater than or equal to `header_size` and fits
        //   in a `usize`. Therefore, `next_multiple_of` does not overflow.
        // - Else, `ENTRY_ALIGN >= (1 << 15) > header_size`, so `ENTRY_ALIGN` itself
        //   is the return value of `next_multiple_of`, and it does not overflow.
        // - `.next_multiple_of(rhs)` can fail only due to overflow or if `rhs == 0`. Neither can
        //   happen in this case. Then, since the return value is greater than or equal to
        //   `header_size`, the subtraction does not underflow.
        header_size.next_multiple_of(F::ENTRY_ALIGN.get()) - header_size
    }
}

/// If `ENTRY_ALIGN` is a power of two, returns the size (in bytes) that should be used for
/// the `padding_1` component of node allocations of a skiplist with format `F`, in accord with the
/// [module-level docs].
///
/// Otherwise, this function is merely guaranteed to successfully evaluate to some value strictly
/// less than `usize::MAX`.
///
/// [module-level docs]: self
#[inline]
#[must_use]
pub(super) const fn padding_1_n<F: SkiplistFormat<U>, U: UpperBound>() -> usize {
    if F::ENTRY_ALIGN.get() <= LINK_ALIGN {
        // Since `ENTRY_ALIGN` is nonzero, no underflow occurs.
        F::ENTRY_ALIGN.get() - 1
    } else {
        // We know that `LINK_ALIGN` is the alignment of an actual type and is thus a power of 2,
        // and is therefore nonzero.
        LINK_ALIGN - 1
    }
}

/// If `ENTRY_ALIGN` is a power of two, returns the offset from the `height` component of a node
/// allocation (that is, from a pointer to a node allocation in a [`Link`] or [`NodeRef`]) to the
/// `user_data` component, in accord with the [module-level docs].
///
/// Otherwise, this function is merely guaranteed to successfully evaluate.
///
/// [module-level docs]: self
#[inline]
#[must_use]
pub(super) const fn offset_to_user_data<F: SkiplistFormat<U>, U: UpperBound>() -> usize {
    1 + padding_1_n::<F, U>()
}

/// Returns the offset from the `skip[level]` component of a node allocation to the `height`
/// component of that allocation, in accord with the [module-level docs].
///
/// In other words, subtracting this offset from a pointer to the `height` component (or, from a
/// pointer to a node allocation in a [`Link`] or [`NodeRef`]) yields a pointer to the
/// `skip[level]` component of that node, if `level < height`.
///
/// [module-level docs]: self
#[inline]
#[must_use]
pub(super) fn neg_offset_to_skip(level: u8) -> usize {
    (usize::from(level) + 1) * LINK_SIZE
}

/// If [`node_layout`] called would succeed with these inputs and some possible `user_data_len`
/// value, then this function returns the offset of the `height` component from the start of the
/// node allocation, in accord with the [module-level docs].
///
/// # Panics
/// May panic if `node_layout::<F>(node_height, 0)` would fail.
///
/// [module-level docs]: self
pub(super) fn offset_from_allocation_start_to_height<F: SkiplistFormat<U>, U: UpperBound>(
    node_height: NonZeroU8,
) -> usize {
    padding_0_r::<F, U>(node_height) + LINK_SIZE * usize::from(node_height.get())
}

/// Computes the layout for a skiplist node allocation, in accord with the [module-level docs].
///
/// If `ENTRY_ALIGN` is not a power of 2 or is too large, or if `user_data_len` is too large,
/// then `None` is returned.
///
/// # Panics
/// The function is guaranteed to panic if `node_height` is greater than [`MAX_HEIGHT`].
///
/// [module-level docs]: self
#[inline]
#[must_use]
pub(super) fn node_layout<F: SkiplistFormat<U>, U: UpperBound>(
    node_height:   NonZeroU8,
    user_data_len: usize,
) -> Option<Layout> {
    if !F::ENTRY_ALIGN.get().is_power_of_two() {
        return None;
    }
    assert!(
        node_height <= MAX_HEIGHT,
        "BUG in anchored-skiplist: attempted to create a node with height {node_height}",
    );

    // Note that despite this module not having `unsafe` itself, the correctness of this function
    // (and the above functions) is critical.

    // Sum the sizes of `padding_0`, the `skip[_]` components, `height`, `padding_1`,
    // and `user_data`.
    #[expect(clippy::non_zero_suggestions, reason = "not helpful here")]
    let size = padding_0_r::<F, U>(node_height)
        .checked_add(LINK_SIZE.checked_mul(usize::from(node_height.get()))?)?
        .checked_add(1)?
        .checked_add(padding_1_n::<F, U>())?
        .checked_add(user_data_len)?;

    let align = node_align::<F, U>();

    Layout::from_size_align(size, align).ok()
}
