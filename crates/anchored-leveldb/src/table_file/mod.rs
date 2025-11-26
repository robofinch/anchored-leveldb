mod build_table;
mod read_table;


pub(crate) use self::{
    build_table::{build_table, TableFileBuilder},
    read_table::{get_table, InternalOptionalTableIter, InternalTableIter},
};


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableCacheKey {
    table_file_number: u64,
}
