#[cfg(not(skiplist_loom))]
pub(crate) use core::sync::atomic::{AtomicPtr, AtomicU8};
#[cfg(not(skiplist_loom))]
pub(crate) use alloc::sync::Arc;

#[cfg(skiplist_loom)]
pub(crate) use loom::sync::Arc;
#[cfg(skiplist_loom)]
pub(crate) use loom::sync::atomic::{AtomicPtr, AtomicU8};
