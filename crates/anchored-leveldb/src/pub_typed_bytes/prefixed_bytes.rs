#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct PrefixedBytes<'a>(&'a [u8]);
