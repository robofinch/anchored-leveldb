const CHECKSUM_MASK_DELTA: u32 = 0x_a282_ead8;


#[inline]
#[must_use]
pub(crate) const fn mask_checksum(unmasked: u32) -> u32 {
    unmasked.rotate_right(15).wrapping_add(CHECKSUM_MASK_DELTA)
}

#[inline]
#[must_use]
pub(crate) const fn unmask_checksum(masked: u32) -> u32 {
    masked.wrapping_sub(CHECKSUM_MASK_DELTA).rotate_left(15)
}
