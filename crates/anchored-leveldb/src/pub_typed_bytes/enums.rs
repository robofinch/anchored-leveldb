use bijective_enum_map::injective_enum_map;


#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum EntryType {
    Deletion = 0,
    Value    = 1,
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

impl PhysicalRecordType {
    pub(crate) const ALL_TYPES: [Self; 5] = [
        Self::Zero, Self::Full, Self::First, Self::Middle, Self::Last,
    ];
}

injective_enum_map! {
    PhysicalRecordType, u8,
    Zero   <=> 0,
    Full   <=> 1,
    First  <=> 2,
    Middle <=> 3,
    Last   <=> 4,
}
