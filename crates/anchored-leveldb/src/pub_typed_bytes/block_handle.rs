use super::offsets::FileOffset;


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: FileOffset,
    pub size:   u64,
}
