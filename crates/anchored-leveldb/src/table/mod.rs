/// A few utilities for lower-level details of the table file format.
///
/// Note that [`crate::table_format`] is about higher-level details of the format, which are more
/// specific to LevelDB.
mod file_format;

mod block_builder;
mod block_iter;

mod filter_block;

mod builder;
mod reader;
mod iter;
