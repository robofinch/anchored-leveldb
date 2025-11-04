cfg_if::cfg_if! {
    if #[cfg(feature = "kanal")] {
        mod kanal_impl;
        pub(crate) use self::kanal_impl::*;
    } else if #[cfg(feature = "crossbeam-channel")] {
        mod crossbeam_impl;
        pub(crate) use self::crossbeam_impl::*;
    } else {
        pub(crate) mod fallback_impl;
        pub(crate) use self::fallback_impl::*;
    }
}

// Silence unused dependency warning
#[cfg(all(feature = "crossbeam-channel", feature = "kanal"))]
use crossbeam_channel as _;
