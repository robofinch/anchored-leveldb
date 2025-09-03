// Import paths are not currently stable.
#![allow(
    warnings,
    reason = "this crate is very unstable. Allow checks to be done on full repo without noise.",
)]

pub mod compactor;
pub mod error;
pub mod iter;
pub mod leveldb;
pub mod logger;
pub mod options;
// pub mod read_only_leveldb;


pub mod format; // TODO: make this private
pub mod public_format;
pub mod table_traits;

pub mod memtable;
pub mod table_cache; // I'm skeptical of whether this is needed
pub mod version; // Not yet implemented
pub mod write_batch;


// Not sure where in the module hierarchy this will end up
#[derive(Debug, Clone)]
pub struct Snapshot {

}
