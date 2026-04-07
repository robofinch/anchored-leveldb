use bijective_enum_map::injective_enum_map;


#[derive(Debug, Clone, Copy)]
pub(crate) enum ContinueBackgroundCompaction {
    True,
    False,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ContinueReadingLogs {
    True,
    False,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ContinueSampling {
    True,
    False,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum BlockOnWrites {
    True,
    False,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum VersionEditTag {
    Comparator,
    LogNumber,
    NextFileNumber,
    LastSequence,
    CompactPointer,
    DeletedFile,
    NewFile,
    /// No longer used, but still tracked in case we read a database made by an old version
    /// of LevelDB.
    PrevLogNumber,
}

injective_enum_map! {
    VersionEditTag, u32,
    Comparator     <=> 1,
    LogNumber      <=> 2,
    NextFileNumber <=> 3,
    LastSequence   <=> 4,
    CompactPointer <=> 5,
    DeletedFile    <=> 6,
    NewFile        <=> 7,
    // Skipping 8 is intentional
    PrevLogNumber  <=> 9,
}
