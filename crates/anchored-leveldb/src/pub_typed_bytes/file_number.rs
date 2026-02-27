use crate::all_errors::types::OutOfFileNumbers;


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct FileNumber(pub u64);

impl FileNumber {
    #[inline]
    pub(crate) fn next(self) -> Result<Self, OutOfFileNumbers> {
        self.0.checked_add(1).map(Self).ok_or(OutOfFileNumbers)
    }
}
