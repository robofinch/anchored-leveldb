// See https://linebender.org/blog/doc-include for this README inclusion strategy
// File links are not supported by rustdoc
//!
//! [LICENSE-APACHE]: https://github.com/robofinch/anchored-leveldb/blob/main/LICENSE-APACHE
//! [LICENSE-MIT]: https://github.com/robofinch/anchored-leveldb/blob/main/LICENSE-MIT
//!
#![cfg_attr(feature = "clone-behavior", doc = " [`clone-behavior`]: clone_behavior")]
#![cfg_attr(feature = "kanal", doc = " [`kanal`]: kanal")]
#![cfg_attr(feature = "crossbeam-channel", doc = " [`crossbeam-channel`]: crossbeam_channel")]
//!
//! <style>
//! .rustdoc-hidden { display: none; }
//! </style>
#![cfg_attr(doc, doc = include_str!("../README.md"))]

mod bounded;
mod shared_bounded;
mod unbounded;
mod shared_unbounded;

mod pooled_resource;
mod other_utils;

mod buffer_pools;

mod channel;

// TODO: provide an option for unbounded pools to limit the number of unused items,
// and drop items which would push the unused items over the limit.

pub use self::{
    bounded::BoundedPool,
    shared_bounded::SharedBoundedPool,
    shared_unbounded::SharedUnboundedPool,
    unbounded::UnboundedPool,
};
pub use self::{
    buffer_pools::{
        BoundedBufferPool, SharedBoundedBufferPool,
        SharedUnboundedBufferPool, UnboundedBufferPool,
    },
    other_utils::{OutOfBuffers, ResetBuffer, ResetNothing, ResetResource, ResourcePoolEmpty},
    pooled_resource::{PooledBuffer, PooledResource},
};
