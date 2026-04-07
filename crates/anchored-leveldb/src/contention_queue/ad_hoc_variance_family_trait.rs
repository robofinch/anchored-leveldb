#![expect(unsafe_code, reason = "perform unsafe lifetime transmutes on a covariant type")]

use crate::{binary_block_log::Slices, pub_typed_bytes::FlushWrites};


/// # Safety
/// It must be sound to covariantly cast the `'varying` lifetime of `Self::Varying<'varying>`.
///
/// This trait is trivially sound to implement if the compiler recognizes `Self::Varying<'varying>`
/// as covariant over `'varying`, in which case `shorten` and `shorten_raw` can be implemented with
/// `{ long }` as its function body.
pub(crate) unsafe trait AdHocCovariantFamily {
    type Varying<'varying>;

    fn shorten_raw<'long, 'short>(
        long: *const *const Self::Varying<'long>
    ) -> *const *const Self::Varying<'short>
    where
        'long: 'short;
}

pub(crate) struct VaryingWriteCommand;

// SAFETY: `WriteCommand<'varying>` is covariant over `'varying`, as shown by how the compiler can
// covariantly coerce `*const *const Self::Varying<'long>` to `*const *const Self::Varying<'short>`.
unsafe impl AdHocCovariantFamily for VaryingWriteCommand {
    type Varying<'varying> = WriteCommand<'varying>;

    fn shorten_raw<'long, 'short>(
        long: *const *const Self::Varying<'long>
    ) -> *const *const Self::Varying<'short>
    where
        'long: 'short,
    {
        long
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum WriteCommand<'a> {
    Write(Slices<'a>),
    Flush(FlushWrites),
}
