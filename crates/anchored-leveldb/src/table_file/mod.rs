pub(crate) mod build_table;
pub(crate) mod read_table;


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableCacheKey {
    table_file_number: u64,
}
