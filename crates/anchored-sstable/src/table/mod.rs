mod build;
mod read;
mod format;
// mod iter;
mod table_struct;


#[expect(clippy::module_name_repetitions, reason = "clarity when used or reexported elsewhere")]
pub use self::{
    build::{TableBuilder, WriteTableOptions},
    format::{
        BLOCK_TRAILER_LEN, BlockHandle, FILTER_META_PREFIX,
        mask_checksum, TableFooter, unmask_checksum,
    },
    // iter::{TableIter, TableIterPieces},
    read::TableBlockReader,
    table_struct::{ReadTableOptions, Table},
};
