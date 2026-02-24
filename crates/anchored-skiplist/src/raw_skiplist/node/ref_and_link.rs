#![expect(unsafe_code, reason = "work with a custom node DST, use unsafe external synchronization")]
#![expect(clippy::undocumented_unsafe_blocks, reason = "temporary. TODO: fix this")]
// The node format is *extremely* unsafe. Some degree of `unsafe` is inevitable, to create a
// self-referential struct with the `Bump` allocator. "While we're at it", might as well save
// ~31 bytes per node by using one allocation with 2 DST fields and 1 byte for the first's length,
// instead of 2 separate allocations for the DST fields and a third allocation storing two fat
// pointers to the DST fields. Adding external synchronization on top isn't that much harder.
// The truly overengineered part is permitting higher alignments than 1 byte.

use core::{any, ptr, slice};
use core::{
    error::Error, marker::PhantomData, mem::MaybeUninit, num::NonZeroU8, ptr::NonNull,
    sync::atomic::Ordering,
};
use core::fmt::{Debug, Display, Formatter, Result as FmtResult};

use bumpalo::AllocErr as BumpAllocErr;
use variance_family::UpperBound;

use crate::maybe_loom::AtomicPtr;
use crate::interface::{EncodeWith, Entry, Key, SkiplistFormat};
use super::super::bump::ExternallySynchronizedBump;
use super::format::{
    LINK_ALIGN, LINK_SIZE,
    neg_offset_to_skip, node_layout, offset_from_allocation_start_to_height, offset_to_user_data,
};


/// Returned if either layout computation of a skiplist node fails or if the memory allocator could
/// not allocate space for the layout.
///
/// Layout computation can fail if [`ENTRY_ALIGN`] is not a power of 2 or if [`entry_size`] is too
/// large for the chosen alignment, accounting for overhead from the skiplist node.
///
/// [`ENTRY_ALIGN`]: SkiplistFormat::ENTRY_ALIGN
/// [`entry_size`]: EncodeWith::entry_size
#[derive(Debug, Default, Clone, Copy)]
pub struct AllocErr;

impl Display for AllocErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "an anchored-skiplist node could not be allocated")
    }
}

impl Error for AllocErr {}

#[repr(transparent)]
pub(in super::super) struct NodeBuilder<'a, F, U> {
    /// A pointer to the `height` component of the node allocation.
    ///
    /// # Safety invariants
    /// - `node_pointer` points to the `height` component of a node allocation.
    /// - The `height` component is initialized to the number of links in the node,
    ///   which equals the `node_height` argument given to `Self::new_node_with`.
    /// - The `height` component is at most [`MAX_HEIGHT`].
    /// - The `user_data` component is properly initialized by an encoder for the format `F`.
    /// - Nothing aliases `node_pointer`; that is, `node_pointer` has suitable provenance
    ///   for reading and writing the node allocation.
    ///
    /// [`MAX_HEIGHT`]: super::super::heights::MAX_HEIGHT
    node_pointer: NonNull<u8>,
    #[expect(clippy::type_complexity, reason = "it's a fairly simple invariant marker type")]
    _format:      PhantomData<fn(F, U) -> (F, U)>,
    /// # Safety invariant
    /// The node allocation must be valid for at least lifetime `'a`.
    _alloc_lt:    PhantomData<&'a ()>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a, F: SkiplistFormat<U>, U: UpperBound> NodeBuilder<'a, F, U> {
    /// # Safety
    /// This method calls [`ExternallySynchronizedBump::try_alloc_layout`]; the safety conditions
    /// of that function apply here. In particular, calls to this method must *not* race with
    /// other calls to `new_node_with` or methods of `ExternallySynchronizedBump` called on
    /// the given `bump`.
    ///
    /// # Panics
    /// The function is guaranteed to panic if `node_height` is greater than [`MAX_HEIGHT`].
    ///
    /// [`MAX_HEIGHT`]: super::super::heights::MAX_HEIGHT
    #[inline]
    pub unsafe fn new_node_with<E>(
        bump:        &'a ExternallySynchronizedBump,
        node_height: NonZeroU8,
        encoder:     E,
    ) -> Result<Self, AllocErr>
    where
        F: EncodeWith<E, U>,
    {
        let user_data_len = F::entry_size(&encoder);
        // Note that if `node_layout` does not panic, then `node_height`
        let layout = node_layout::<F, U>(node_height, user_data_len).ok_or(AllocErr)?;

        // SAFETY: Synchronization is guaranteed by caller.
        let node_allocation: NonNull<u8> = unsafe {
            bump.try_alloc_layout(layout)
        }.map_err(|BumpAllocErr| AllocErr)?;

        // SAFETY: since `node_layout::<F>(node_height, _)` succeeded,
        // `offset_from_allocation_start_to_height` returns the appropriate byte offset.
        // The `height` component is within bounds of the node allocation, so the offset does not
        // overflow `isize` or wrap outside the node allocation. This adds an offset in bytes,
        // since `node_allocation: NonNull<u8>`. Therefore, this call to `NonNull::add` is sound.
        let node_pointer: NonNull<u8> = unsafe {
            node_allocation.add(offset_from_allocation_start_to_height::<F, U>(node_height))
        };

        // SAFETY: `node_pointer` is valid for writes (it's contained in a single Rust allocation,
        // and `bumpalo` gave us exclusive/mutable access over the node allocation, so we have
        // appropriate provenance; it's non-null; and the write doesn't race with anything), and
        // it is trivially properly aligned for `u8`.
        // More importantly, we are correctly writing the `node_height` value to the `height`
        // component of the node allocation.
        unsafe {
            node_pointer.write(node_height.get());
        };

        // SAFETY: since `node_layout::<F>(node_height, _)` succeeded, `F::ENTRY_ALIGN` must be
        // a power of two, so `offset_to_user_data::<F>` returns the appropriate byte offset.
        // The `user_data` component is within bounds of the node allocation (if only for 0 bytes),
        // so the offset does not overflow `isize` or wrap outside the node allocation. This adds an
        // offset in bytes, since `node_pointer: NonNull<u8>`. Therefore, this call to
        // `NonNull::add` is sound.
        let user_data_pointer = unsafe {
            node_pointer.add(offset_to_user_data::<F, U>())
        };
        let user_data = user_data_pointer.as_ptr().cast::<MaybeUninit<u8>>();
        // SAFETY: the format of the node allocation has `user_data_len`-many bytes at the end,
        // in the `user_data` component. The `user_data` pointer refers to the `user_data` component
        // of the node allocation, so:
        // - the pointer is valid for both reads and writes of size
        //   `user_data_len == size_of::<MaybeUninit<u8>>() * user_data_len`, since we have
        //   appropriate provenance (see below about aliasing), we don't race with anything,
        //   and it's contained in a single allocation.
        // - The pointer is non-null, since the null pointer is not contained in a Rust allocation.
        // - The pointer is trivially properly-aligned for `MaybeUninit<u8>`.
        // - `user_data` points to `user_data_len` consecutive properly initialized values of
        //   `MaybeUninit<u8>` (since any byte pattern, including uninit, is properly initialized
        //   for `MaybeUninit`).
        // - No other pointer or reference is used to access `user_data`'s pointee (aside from
        //   those derived from it) up until its last (and first) use below, since we control
        //   all pointers to it here (and since `bumpalo::Bump` gives out mutable access to
        //   allocations).
        // - The total size of the slice cannot be larger than `isize::MAX` and it cannot wrap
        //   around the address space, since it is contained inside a Rust allocation, and Rust
        //   allocations cannot do those things.
        let user_data = unsafe { slice::from_raw_parts_mut(user_data, user_data_len) };

        // SAFETY: the length of `user_data` is `user_data_len == F::entry_size(&encoder)`.
        // We know that `encoder` was not accessed between the call to `F::entry_size(&encoder)`
        // and this call, since we own `encoder` and did not do anything to it.
        // NOTE: if `encode_entry` panics, memory may be effectively leaked (become wasted in the
        // bump allocator until it's reset or dropped), but no unsoundness would occur.
        unsafe {
            F::encode_entry(encoder, user_data);
        };

        Ok(Self {
            // Safety invariant: TODO
            node_pointer,
            _format:   PhantomData,
            // Safety invariant: for at least lifetime `'a`, the source `bump` cannot be dropped
            // or reset (as the latter requires an exclusive/mutable borrow), and thus the
            // node allocation is valid for at least lifetime `'a`.
            _alloc_lt: PhantomData,
        })
    }

    #[inline]
    #[must_use]
    const fn basic_parts(&self) -> (u8, *const u8) {
        let height = unsafe { self.node_pointer.read() };

        let user_data = unsafe { self.node_pointer.add(offset_to_user_data::<F, U>()) };
        let user_data = user_data.as_ptr().cast_const();

        (height, user_data)
    }

    /// # Robust guarantees
    /// The length of the returned `&mut [MaybeUninit<Link<F, U>>]` value is guaranteed to
    /// equal the `node_height` argument passed to [`Self::new_node_with`].
    ///
    /// Additionally, it is guaranteed that that length is at most [`MAX_HEIGHT`].
    ///
    /// [`MAX_HEIGHT`]: super::super::heights::MAX_HEIGHT
    #[expect(clippy::type_complexity, reason = "it's a fairly simple tuple")]
    #[inline]
    #[must_use]
    pub fn parts(&mut self) -> (&mut [MaybeUninit<Link<F, U>>], Key<'_, F, U>) {
        let (height, user_data) = self.basic_parts();

        let highest_level = unsafe { height.unchecked_sub(1) };
        let highest_skip_offset = neg_offset_to_skip(highest_level);
        let highest_skip = unsafe { self.node_pointer.sub(highest_skip_offset) };
        #[expect(
            clippy::cast_ptr_alignment,
            reason = "we do pointer math to ensure proper alignment",
        )]
        let skips = highest_skip.as_ptr().cast::<MaybeUninit<Link<F, U>>>();
        let skips = unsafe { slice::from_raw_parts_mut(skips, usize::from(height)) };

        let key = unsafe { F::decode_key(user_data) };

        (skips, key)
    }

    /// # Safety
    /// All of the skip links of this [`NodeBuilder`], which may be written via
    /// [`NodeBuilder::parts`], must have been fully initialized, and any nodes referenced by those
    /// skip links must have been allocated in the same [`ExternallySynchronizedBump`] allocator as
    /// the node of this builder.
    #[inline]
    #[must_use]
    pub unsafe fn finish(self) -> NodeRef<'a, F, U> {
        NodeRef {
            // Safety invariant: TODO
            node_pointer: self.node_pointer,
            _format:      PhantomData,
            // Safety invariant: TODO
            _alloc_lt:    PhantomData,
        }
    }
}

impl<F, U> Debug for NodeBuilder<'_, F, U>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    for<'a> Entry<'a, F, U>: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        struct PartiallyInitNode<Entry>(u8, Entry);

        impl<Entry: Debug> Debug for PartiallyInitNode<Entry> {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                use alloc::format;

                f.debug_struct("PartiallyInitNode")
                    .field("skips",     &format!("[MaybeUninit<Link>; {}]", self.0))
                    .field("height",    &self.0)
                    .field("user_data", &self.1)
                    .finish()
            }
        }

        let (height, user_data) = self.basic_parts();

        let entry = unsafe { F::decode_entry(user_data) };

        f.debug_struct("NodeBuilder")
            .field("node",    &PartiallyInitNode(height, entry))
            .field("_format", &any::type_name::<F>())
            .finish()
    }
}

#[repr(transparent)]
pub(in super::super) struct NodeRef<'a, F, U> {
    /// A pointer to the `height` component of the node allocation.
    ///
    /// # Safety invariant
    /// `node_pointer` points to the `height` component of a fully-initialized node allocation.
    /// See the [`super::format`] module-level docs for more.
    node_pointer: NonNull<u8>,
    #[expect(clippy::type_complexity, reason = "it's a fairly simple invariant marker type")]
    _format:      PhantomData<fn(F, U) -> (F, U)>,
    /// # Safety invariant
    /// The node allocation must be valid for at least lifetime `'a`.
    _alloc_lt:    PhantomData<&'a ()>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a, F: SkiplistFormat<U>, U: UpperBound> NodeRef<'a, F, U> {
    /// # Safety
    /// The referenced node allocation must remain valid for lifetime `'b`.
    #[inline]
    #[must_use]
    pub unsafe fn extend_lifetime<'b>(self) -> NodeRef<'b, F, U> {
        NodeRef {
            // Safety invariant: transfers over.
            node_pointer: self.node_pointer,
            _format:      PhantomData,
            // Safety invariant: asserted by caller.
            _alloc_lt:    PhantomData,
        }
    }

    #[inline]
    #[must_use]
    pub fn erase(self) -> ErasedNodeRef<F, U> {
        ErasedNodeRef {
            node_pointer: self.node_pointer,
            _format:      PhantomData,
        }
    }

    #[inline]
    #[must_use]
    const fn user_data(self) -> *const u8 {
        let user_data = unsafe { self.node_pointer.add(offset_to_user_data::<F, U>()) };
        user_data.as_ptr().cast_const()
    }

    /// # Safety
    /// If the returned link is used to store a node reference, then the referenced node must
    /// be allocated in the same bump as the node referenced by `self`.
    #[must_use]
    unsafe fn skip(self, level: u8) -> Option<&'a Link<F, U>> {
        if level < self.height().get() {
            let skip_offset = neg_offset_to_skip(level);
            let skip = unsafe { self.node_pointer.sub(skip_offset) };
            let skip: *const Link<F, U> = skip.cast::<Link<F, U>>().as_ptr().cast_const();
            let skip = unsafe { &*skip };
            Some(skip)
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    pub fn entry(self) -> Entry<'a, F, U> {
        unsafe { F::decode_entry(self.user_data()) }
    }

    #[inline]
    #[must_use]
    pub fn key(self) -> Key<'a, F, U> {
        unsafe { F::decode_key(self.user_data()) }
    }

    #[must_use]
    pub fn next_node(self) -> Option<Self> {
        self.load_skip(0, Ordering::Acquire)
    }

    #[inline]
    #[must_use]
    pub fn ptr_eq(self, other: NodeRef<'_, F, U>) -> bool {
        ptr::addr_eq(self.node_pointer.as_ptr(), other.node_pointer.as_ptr())
    }

    #[must_use]
    pub const fn height(self) -> NonZeroU8 {
        let height = unsafe { self.node_pointer.read() };
        unsafe { NonZeroU8::new_unchecked(height) }
    }

    /// Load the skip link at the given level (which should be in `0..MAX_HEIGHT`).
    ///
    /// # Panics
    /// Panics if `order` is [`Release`] or [`AcqRel`].
    ///
    /// [`Release`]: Ordering::Release
    /// [`AcqRel`]:  Ordering::AcqRel
    #[must_use]
    pub fn load_skip(self, level: u8, order: Ordering) -> Option<Self> {
        // Using too high of a level, but still under `MAX_HEIGHT`, is still useful in searching
        // algorithms. There's no reason to panic on higher levels.
        // SAFETY: we do not use the link to store anything.
        let skip = unsafe { self.skip(level)? };
        unsafe { skip.load(order) }
    }

    /// # Safety
    /// The node referenced by the provided `node_ref` must have been allocated in the same
    /// [`ExternallySynchronizedBump`] allocator that `self` was allocated in.
    ///
    /// # Panics
    /// Panics if `order` is [`Acquire`] or [`AcqRel`].
    ///
    /// May or may not panic if `level >= self.height()`, that is, if there is no skip at the
    /// indicated `level` of this node.
    ///
    /// [`Acquire`]: Ordering::Acquire
    /// [`AcqRel`]:  Ordering::AcqRel
    pub unsafe fn store_some_skip(self, level: u8, node_ref: NodeRef<'_, F, U>, order: Ordering) {
        debug_assert!(
            level < self.height().get(),
            "should not try to set a nonexistent skip of a node",
        );

        // This *should* always be `Some`, but probably no reason to trigger a panic in release
        // mode.
        // SAFETY: As guaranteed by the caller, the node reference we store in the returned link
        // is allocated in the same bump as the node referenced by `self`.
        if let Some(skip) = unsafe { self.skip(level) } {
            skip.store_some(node_ref, order);
        }
    }
}

impl<F, U> Copy for NodeRef<'_, F, U> {}

impl<F, U> Clone for NodeRef<'_, F, U> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<F, U> Send for NodeRef<'_, F, U>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    for<'a> Entry<'a, F, U>: Sync,
    for<'a> Key<'a, F, U>: Sync,
{}

unsafe impl<F, U> Sync for NodeRef<'_, F, U>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    for<'a> Entry<'a, F, U>: Sync,
    for<'a> Key<'a, F, U>: Sync,
{}

impl<F, U> Debug for NodeRef<'_, F, U>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    for<'a> Entry<'a, F, U>: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        struct NonExhaustiveNodeRef;

        impl Debug for NonExhaustiveNodeRef {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                f.debug_struct("NodeRef").finish_non_exhaustive()
            }
        }

        struct Node<T>(T);

        impl<F, U> Debug for Node<&NodeRef<'_, F, U>>
        where
            F: SkiplistFormat<U>,
            U: UpperBound,
            for<'a> Entry<'a, F, U>: Debug,
        {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                use alloc::format;

                let mut f = f.debug_struct("Node");

                for skip_level in (0..self.0.height().get()).rev() {
                    let skip = self.0
                        .load_skip(skip_level, Ordering::Relaxed)
                        .map(|_| NonExhaustiveNodeRef);

                    f.field(&format!("skip[{skip_level}]"), &skip);
                }

                f.field("height",       &self.0.height())
                    .field("user_data", &self.0.entry())
                    .finish()
            }
        }

        f.debug_struct("NodeRef")
            .field("node",    &Node(self))
            .field("_format", &any::type_name::<F>())
            .finish()
    }
}

#[repr(transparent)]
pub(in super::super) struct ErasedNodeRef<F, U> {
    /// # Safety invariant
    /// `node_pointer` came from [`NodeRef::erase`].
    node_pointer: NonNull<u8>,
    #[expect(clippy::type_complexity, reason = "it's a fairly simple invariant marker type")]
    _format:      PhantomData<fn(F, U) -> (F, U)>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<F, U> ErasedNodeRef<F, U> {
    /// # Safety
    /// The referenced node allocation, if any, must be valid for at least lifetime `'a`.
    pub unsafe fn unerase<'a>(self) -> NodeRef<'a, F, U> {
        NodeRef {
            node_pointer: self.node_pointer,
            _format:      PhantomData,
            _alloc_lt:    PhantomData,
        }
    }
}

impl<F, U> Copy for ErasedNodeRef<F, U> {}

impl<F, U> Clone for ErasedNodeRef<F, U> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<F, U> Send for ErasedNodeRef<F, U>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    for<'a> Entry<'a, F, U>: Sync,
    for<'a> Key<'a, F, U>: Sync,
{}

unsafe impl<F, U> Sync for ErasedNodeRef<F, U>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    for<'a> Entry<'a, F, U>: Sync,
    for<'a> Key<'a, F, U>: Sync,
{}

impl<F, U> Debug for ErasedNodeRef<F, U>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ErasedNodeRef").finish_non_exhaustive()
    }
}

// `Link` is a `repr(transparent)` wrapper around the `AtomicPtr` type that `LINK_SIZE`
// and `LINK_ALIGN` are defined from. These assertions are included to doubly-ensure that the node
// format remains in-sync with the `Link` definition.
const _: () = assert!(
    size_of::<Link<(), ()>>() == LINK_SIZE,
    "Link should have size LINK_SIZE",
);
const _: () = assert!(
    align_of::<Link<(), ()>>() == LINK_ALIGN,
    "Link should have size LINK_ALIGN",
);

#[repr(transparent)]
pub(in super::super) struct Link<F, U> {
    /// Either a null pointer or a pointer to the `height` component of the node allocation.
    ///
    /// # Safety invariant
    /// `node_pointer` is either a null pointer or points to the `height` component of a
    /// fully-initialized node allocation. See the [`super::format`] module-level docs for more.
    node_pointer: AtomicPtr<u8>,
    #[expect(clippy::type_complexity, reason = "it's a fairly simple invariant marker type")]
    _format:      PhantomData<fn(F, U) -> (F, U)>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<F: SkiplistFormat<U>, U: UpperBound> Link<F, U> {
    #[inline]
    #[must_use]
    pub fn new(link: Option<NodeRef<'_, F, U>>) -> Self {
        if let Some(node_ref) = link {
            Self::new_some(node_ref)
        } else {
            Self::new_none()
        }
    }

    #[inline]
    #[must_use]
    pub fn new_some(node_ref: NodeRef<'_, F, U>) -> Self {
        Self {
            node_pointer: AtomicPtr::new(node_ref.node_pointer.as_ptr()),
            _format:      PhantomData,
        }
    }

    #[inline]
    #[must_use]
    pub fn new_none() -> Self {
        Self {
            node_pointer: AtomicPtr::new(ptr::null_mut()),
            _format:      PhantomData,
        }
    }

    /// # Panics
    /// Panics if `order` is [`Release`] or [`AcqRel`].
    ///
    /// # Safety
    /// The node allocation referenced by this link (if any) must be valid for at least
    /// lifetime `'a`.
    ///
    /// [`Release`]: Ordering::Release
    /// [`AcqRel`]:  Ordering::AcqRel
    #[must_use]
    pub unsafe fn load<'a>(&self, order: Ordering) -> Option<NodeRef<'a, F, U>> {
        let link = self.node_pointer.load(order);
        NonNull::new(link)
            .map(|linked_node| NodeRef {
                node_pointer: linked_node,
                _format:      PhantomData,
                _alloc_lt:    PhantomData,
            })
    }

    /// # Panics
    /// Panics if `order` is [`Acquire`] or [`AcqRel`].
    ///
    /// [`Acquire`]: Ordering::Acquire
    /// [`AcqRel`]:  Ordering::AcqRel
    pub fn store_some(&self, node_ref: NodeRef<'_, F, U>, order: Ordering) {
        self.node_pointer.store(node_ref.node_pointer.as_ptr(), order);
    }
}

impl<F: SkiplistFormat<U>, U: UpperBound> Default for Link<F, U> {
    #[inline]
    fn default() -> Self {
        Self::new_none()
    }
}

unsafe impl<F, U> Send for Link<F, U>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    for<'a> Entry<'a, F, U>: Sync,
    for<'a> Key<'a, F, U>: Sync,
{}

unsafe impl<F, U> Sync for Link<F, U>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    for<'a> Entry<'a, F, U>: Sync,
    for<'a> Key<'a, F, U>: Sync,
{}
