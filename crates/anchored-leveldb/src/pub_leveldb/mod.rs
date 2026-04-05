mod structs;


// The below modules implement functions for `DBState` and `DB`.
mod construct;
mod destruct;
mod compaction;
mod put_delete_get;
mod other_read_write;
mod debug_and_stats;
// The below modules are private helpers for `DBState` and `DB`.
mod utils;
// Requires `unsafe`.
mod into_inner;

// A public free function.
mod destroy;

// later: repair_db
// later: clone_db, checkpoints


pub use self::destroy::irreversibly_destroy_entire_db;
pub use self::structs::{DB, DBState};
