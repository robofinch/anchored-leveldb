use bijective_enum_map::injective_enum_map;

use super::simple_newtypes::FileNumber;


#[derive(Debug, Clone, Copy)]
pub enum BlockType {
    Metaindex,
    Filter,
    Index,
    Data,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum EntryType {
    Deletion = 0,
    Value    = 1,
}

impl EntryType {
    pub(crate) const MIN_TYPE: Self = Self::Deletion;
    pub(crate) const MAX_TYPE: Self = Self::Value;
}

injective_enum_map! {
    EntryType, u8,
    Deletion <=> 0,
    Value    <=> 1,
}

#[derive(Debug, Clone, Copy)]
pub enum PhysicalRecordType {
    Zero,
    Full,
    First,
    Middle,
    Last,
}

injective_enum_map! {
    PhysicalRecordType, u8,
    Zero   <=> 0,
    Full   <=> 1,
    First  <=> 2,
    Middle <=> 3,
    Last   <=> 4,
}

impl PhysicalRecordType {
    pub(crate) const ALL_TYPES: [Self; 5] = [
        Self::Zero, Self::Full, Self::First, Self::Middle, Self::Last,
    ];
}

pub(crate) trait IndexRecordTypes<T> {
    #[must_use]
    fn infallible_index(&self, record_type: PhysicalRecordType) -> &T;
}

impl<T> IndexRecordTypes<T> for [T; PhysicalRecordType::ALL_TYPES.len()] {
    fn infallible_index(&self, record_type: PhysicalRecordType) -> &T {
        // We need to ensure that `0 <= usize::from(u8::from(record_type)) < self.len()`.
        // This holds, since `self.len() == PhysicalRecordType::ALL_TYPES.len() == 5`,
        // and `0 <= usize::from(u8::from(record_type)) < 5`.
        #[expect(
            clippy::indexing_slicing,
            reason = "See above. Not pressing enough to use `unsafe`",
        )]
        &self[usize::from(u8::from(record_type))]
    }
}

/// The source of an invalid internal key in a version edit.
#[derive(Debug, Clone, Copy)]
pub enum VersionEditKeyType {
    CompactionPointer,
    /// The smallest key of a table file was invalid.
    ///
    /// # Data
    /// The file number of the table file.
    SmallestFileKey(FileNumber),
    /// The largest key of a table file was invalid.
    ///
    /// # Data
    /// The file number of the table file.
    LargestFileKey(FileNumber),
}
