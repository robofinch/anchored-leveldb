// See https://linebender.org/blog/doc-include for this README inclusion strategy
// File links are not supported by rustdoc
//!
//! [LICENSE-APACHE]: https://github.com/robofinch/anchored-leveldb/blob/main/LICENSE-APACHE
//! [LICENSE-MIT]: https://github.com/robofinch/anchored-leveldb/blob/main/LICENSE-MIT
//!
#![cfg_attr(feature = "clone-behavior", doc = " [`clone-behavior`]: clone_behavior")]
#![cfg_attr(feature = "parking_lot", doc = " [`parking_lot`]: parking_lot")]
//!
//! <style>
//! .rustdoc-hidden { display: none; }
//! </style>
#![cfg_attr(doc, doc = include_str!("../README.md"))]

#![no_std]
#![warn(clippy::std_instead_of_alloc, clippy::std_instead_of_core, clippy::alloc_instead_of_core)]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

mod maybe_sync;

mod arc;
// mod strong_arc;

#[cfg(feature = "std")]
mod mutex;
#[cfg(feature = "std")]
mod rwlock;
#[cfg(feature = "std")]
mod would_block_error;


pub use self::maybe_sync::MaybeSync;
pub use self::arc::{MaybeSyncArc, MaybeSyncWeak};

#[cfg(feature = "std")]
pub use self::mutex::{MaybeSyncMutex, MaybeSyncMutexGuard};
#[cfg(feature = "std")]
pub use self::would_block_error::WouldBlockError;
