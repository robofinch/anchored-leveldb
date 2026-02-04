// See https://linebender.org/blog/doc-include for this README inclusion strategy
// File links are not supported by rustdoc
//!
//! [LICENSE-APACHE]: https://github.com/robofinch/anchored-leveldb/blob/main/LICENSE-APACHE
//! [LICENSE-MIT]: https://github.com/robofinch/anchored-leveldb/blob/main/LICENSE-MIT
//!
#![cfg_attr(feature = "clone-behavior", doc = " [`clone-behavior`]: clone_behavior")]
//!
//! <style>
//! .rustdoc-hidden { display: none; }
//! </style>
#![cfg_attr(doc, doc = include_str!("../README.md"))]

#![no_std]

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


pub use self::maybe_sync::MaybeSync;
pub use self::arc::{MaybeSyncArc, MaybeSyncWeak};

/// The versions of `Mutex` and `RwLock` used by this crate.
///
/// If the `parking_lot` feature is enabled, then `parking_lot`'s types are used;
/// otherwise, `std::sync`'s types are used.
#[cfg(feature = "std")]
pub mod std_or_parking_lot {
    #[cfg(not(feature = "parking_lot"))]
    pub use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
    #[cfg(feature = "parking_lot")]
    pub use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
}
