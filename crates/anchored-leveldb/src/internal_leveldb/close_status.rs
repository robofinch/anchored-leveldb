use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::atomic::{AtomicU8, Ordering},
};

use bijective_enum_map::injective_enum_map;


#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub(crate) enum CloseStatus {
    Closed                 = 0,
    Closing                = 1,
    ClosingAfterCompaction = 2,
    Open                   = 3,
}

injective_enum_map! {
    CloseStatus, u8,
    Closed                 <=> 0,
    Closing                <=> 1,
    ClosingAfterCompaction <=> 2,
    Open                   <=> 3,
}

pub(crate) struct AtomicCloseStatus(AtomicU8);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl AtomicCloseStatus {
    #[inline]
    #[must_use]
    pub const fn new(close_status: CloseStatus) -> Self {
        #[expect(clippy::as_conversions, reason = "const-hack for lack of const `u8::from(_)`")]
        Self(AtomicU8::new(close_status as u8))
    }

    pub fn set(&self, close_status: CloseStatus) {
        self.0.store(u8::from(close_status), Ordering::Relaxed);
    }

    #[must_use]
    pub fn read(&self) -> CloseStatus {
        let close_status = self.0.load(Ordering::Relaxed);
        #[expect(
            clippy::expect_used,
            reason = "`self.0` is only set (to a `CloseStatus`) in `Self::new` and `Self::set`",
        )]
        CloseStatus::try_from(close_status)
            .expect("`AtomicCloseStatus` should only store valid `CloseStatus` values")
    }
}

impl Debug for AtomicCloseStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("AtomicCloseStatus").field(&self.read()).finish()
    }
}
