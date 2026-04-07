// This module defines the structs and manages their reference counts.
// Therefore, it includes a somewhat eclectic cast of methods to encapsulate the reference counting.
mod structs;

// The below modules implement functions for `DBState` and `DB`.
mod construct;
mod destruct;
mod compaction;
mod put_delete_get;
mod other_read_write;
mod debug_and_stats;

// A public free function.
mod destroy;

// later: repair_db
// later: clone_db, checkpoints


pub use self::{destroy::irreversibly_destroy_entire_db, other_read_write::DBIter};
pub use self::structs::{DB, DBState};
