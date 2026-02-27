use std::mem;
use std::convert::Infallible;


#[derive(Debug, Clone, Copy)]
pub(crate) struct Slices<'a>(&'a [u8], usize, &'a [&'a [u8]]);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> Slices<'a> {
    /// # Panics
    /// May panic if the total lengths of the input slices exceeds `usize::MAX`.
    #[inline]
    #[must_use]
    pub fn new(first: &'a [u8], following: &'a [&'a [u8]]) -> Self {
        #[expect(clippy::expect_used, reason = "Panic declared, and does not happen in practice")]
        let following_len = following
            .iter()
            .fold(0_usize, |acc, slice| {
                acc.checked_add(slice.len()).expect("total slice len overflow")
            });

        Self(first, following_len, following)
    }

    #[inline]
    #[must_use]
    pub const fn new_single(slice: &'a [u8]) -> Self {
        Self(slice, 0, &[])
    }

    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len() + self.1
    }

    #[inline]
    pub fn pop_prefix(&mut self, prefix_len: usize) {
        self.pop_and_fold_prefix(prefix_len, (), |(), _| ());
    }

    #[inline]
    pub fn fold_in_prefix<B, F>(mut self, prefix_len: usize, init: B, f: F) -> B
    where
        F: FnMut(B, &'a [u8]) -> B,
    {
        // Note that this operates on an owned *copy* of `self` that gets destroyed after this
        // function returns.
        self.pop_and_fold_prefix(prefix_len, init, f)
    }

    #[inline]
    pub fn for_each_in_prefix<F: FnMut(&'a [u8])>(mut self, prefix_len: usize, mut f: F) {
        // Note that this operates on an owned *copy* of `self` that gets destroyed after this
        // function returns.
        self.pop_and_fold_prefix(prefix_len, (), |(), slice| f(slice));
    }

    #[inline]
    pub fn pop_and_fold_prefix<B, F>(&mut self, prefix_len: usize, init: B, mut f: F) -> B
    where
        F: FnMut(B, &'a [u8]) -> B,
    {
        let Ok(output) = self.try_pop_and_fold_prefix::<_, _, Infallible>(
            prefix_len,
            init,
            |acc, slice| Ok(f(acc, slice)),
        );
        output
    }

    #[inline]
    pub fn try_fold_in_prefix<B, F, E>(mut self, prefix_len: usize, init: B, f: F) -> Result<B, E>
    where
        F: FnMut(B, &'a [u8]) -> Result<B, E>,
    {
        // Note that this operates on an owned *copy* of `self` that gets destroyed after this
        // function returns.
        self.try_pop_and_fold_prefix(prefix_len, init, f)
    }

    #[inline]
    pub fn try_for_each_in_prefix<F, E>(mut self, prefix_len: usize, mut f: F) -> Result<(), E>
    where
        F: FnMut(&'a [u8]) -> Result<(), E>,
    {
        // Note that this operates on an owned *copy* of `self` that gets destroyed after this
        // function returns.
        self.try_pop_and_fold_prefix(prefix_len, (), |(), slice| f(slice))
    }

    #[inline]
    pub fn try_pop_and_fold_prefix<B, F, E>(
        &mut self,
        mut prefix_len: usize,
        init:           B,
        mut f:          F,
    ) -> Result<B, E>
    where
        F: FnMut(B, &'a [u8]) -> Result<B, E>,
    {
        let mut new_first = mem::take(&mut self.0);
        let mut acc = init;

        loop {
            if let Some((prefix, new_first)) = new_first.split_at_checked(prefix_len) {
                self.0 = new_first;
                acc = f(acc, prefix)?;
                return Ok(acc);
            }

            // Process that entire slice. Note that underflow does not occur, since we'd
            // have entered the above `if` block if `prefix_len <= new_first.len()`.
            acc = f(acc, new_first)?;
            prefix_len -= new_first.len();

            if let Some((next_slice, following)) = self.2.split_first() {
                new_first = *next_slice;
                // Note that `self.1` is the sum of the lengths of slices in `self.2`, so
                // underflow does not occur.
                self.1 -= next_slice.len();
                self.2 = following;
            } else {
                // `self.0` is empty (since we used `mem::take` on it, and have not since
                // replaced its contents with anything) and so is `self.2`, since it has
                // no first element. Therefore, `prefix_len` is greater than the total length
                // of `self`'s slices, and we've processed all of `self`. We can just return here.
                return Ok(acc);
            }
        }
    }
}
