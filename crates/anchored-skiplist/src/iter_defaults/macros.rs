/// For a given <code>List: [SkiplistSeek]</code>, this macro creates a wrapper type
/// around [`SkiplistIter<'a, List>`] that implements
/// <code>[SeekableIterator]<\[u8\], List::Cmp></code> and [`Iterator`].
///
/// # Example
/// ```no_run,compile_fail
/// skiplistiter_wrapper! {
///     /// Documentation
///     pub struct Iter<'_, Cmp: _>(#[List = SkiplistImpl<Cmp>] _);
/// }
/// ```
/// expands to
/// ```no_run,compile_fail
/// /// Documentation
/// pub struct Iter<'a, Cmp: Comparator>(SkiplistIter<'a, SkiplistImpl<Cmp>>);
/// ```
/// in addition to implementations of [`CursorIterator`], <code>[Seekable]<\[u8\], Cmp></code>, and
/// `Iterator<Item = &'a [u8]>`.
///
/// # Hygiene / Environment
///
/// The [`Comparator`], [`CursorIterator`], [`Seekable`] and [`Iterator`] traits must be in scope,
/// and the [`SkiplistIter`], [`Option`], and [`u8`] types must be in scope. That is, this macro
/// does not fully qualify all its types, traits, and method calls.
///
/// [`Comparator`]: seekable_iterator::Comparator
/// [`CursorIterator`]: seekable_iterator::CursorIterator
/// [`Seekable`]: seekable_iterator::Seekable
/// [Seekable]: seekable_iterator::Seekable
/// [SeekableIterator]: seekable_iterator::SeekableIterator
/// [SkiplistSeek]: crate::iter_defaults::SkiplistSeek
/// [`SkiplistIter`]: crate::iter_defaults::SkiplistIter
/// [`SkiplistIter<'a, List>`]: crate::iter_defaults::SkiplistIter
#[macro_export]
macro_rules! skiplistiter_wrapper {
    {
        $(#[$meta:meta])*
        $vis:vis struct $iter:ident<'_, $cmp:ident: _>(#[List = $list:ty] _ $(,)?);
    } => {
        $(#[$meta])*
        $vis struct $iter<'a, $cmp: Comparator<[u8]>>(SkiplistIter<'a, $list>);

        impl<'a, $cmp: Comparator<[u8]>> Iterator for $iter<'a, $cmp> {
            type Item = &'a [u8];

            #[inline]
            fn next(&mut self) -> Option<Self::Item> {
                self.0.next()
            }

            #[inline]
            fn fold<B, F>(self, init: B, f: F) -> B
            where
                F: FnMut(B, Self::Item) -> B,
            {
                self.0.fold(init, f)
            }
        }

        impl<'a, $cmp: Comparator<[u8]>> CursorIterator for $iter<'a, $cmp> {
            #[inline]
            fn valid(&self) -> bool {
                self.0.valid()
            }

            #[inline]
            fn current(&self) -> Option<&'a [u8]> {
                self.0.current()
            }

            fn prev(&mut self) -> Option<&'a [u8]> {
                self.0.prev()
            }
        }

        impl<$cmp: Comparator<[u8]>> Seekable<[u8], Cmp> for $iter<'_, $cmp> {
            #[inline]
            fn reset(&mut self) {
                self.0.reset();
            }

            fn seek(&mut self, min_bound: &[u8]) {
                self.0.seek(min_bound);
            }

            fn seek_before(&mut self, strict_upper_bound: &[u8]) {
                self.0.seek_before(strict_upper_bound);
            }

            #[inline]
            fn seek_to_first(&mut self) {
                self.0.seek_to_first();
            }

            fn seek_to_last(&mut self) {
                self.0.seek_to_last();
            }
        }
    };
}

/// For a given <code>List: [SkiplistSeek]</code>, this macro creates a wrapper type
/// around [`SkiplistLendingIter<List>`] that implements
/// <code>[SeekableLendingIterator]<\[u8\], List::Cmp></code>.
///
/// # Example
/// ```no_run,compile_fail
/// skiplistlendingiter_wrapper! {
///     /// Documentation
///     pub struct LendingIter<Cmp: _>(#[List = SkiplistImpl<Cmp>] _);
/// }
/// ```
/// expands to
/// ```no_run,compile_fail
/// /// Documentation
/// pub struct LendingIter<Cmp: Comparator>(SkiplistLendingIter<SkiplistImpl<Cmp>>);
/// ```
/// in addition to implementations of [`CursorLendingIterator`], [`LendItem`], and
/// <code>[Seekable]<\[u8\], Cmp></code>.
///
/// # Hygiene / Environment
///
/// The [`Comparator`], [`CursorLendingIterator`], [`LendItem`], and [`Seekable`] traits must be in
/// scope, and the [`SkiplistLendingIter`], [`Option`], and [`u8`] types must be in scope. That is,
/// this macro does not fully qualify all its types, traits, and method calls.
///
/// [`Comparator`]: seekable_iterator::Comparator
/// [`CursorLendingIterator`]: seekable_iterator::CursorLendingIterator
/// [`LendItem`]: seekable_iterator::LendItem
/// [`Seekable`]: seekable_iterator::Seekable
/// [Seekable]: seekable_iterator::Seekable
/// [SeekableLendingIterator]: seekable_iterator::SeekableLendingIterator
/// [SkiplistSeek]: crate::iter_defaults::SkiplistSeek
/// [`SkiplistLendingIter`]: crate::iter_defaults::SkiplistLendingIter
/// [`SkiplistLendingIter<List>`]: crate::iter_defaults::SkiplistLendingIter
#[macro_export]
macro_rules! skiplistlendingiter_wrapper {
    {
        $(#[$meta:meta])*
        $vis:vis struct $lending_iter:ident<$cmp:ident: _>(#[List = $list:ty] _ $(,)?);
    } => {
        $(#[$meta])*
        $vis struct $lending_iter<$cmp: Comparator<[u8]>>(SkiplistLendingIter<$list>);

        impl<'lend, $cmp: Comparator<[u8]>> LendItem<'lend> for $lending_iter<$cmp> {
            type Item = &'lend [u8];
        }

        impl<$cmp: Comparator<[u8]>> CursorLendingIterator for $lending_iter<$cmp> {
            #[inline]
            fn valid(&self) -> bool {
                self.0.valid()
            }

            #[inline]
            fn next(&mut self) -> Option<&[u8]> {
                self.0.next()
            }

            #[inline]
            fn current(&self) -> Option<&[u8]> {
                self.0.current()
            }

            fn prev(&mut self) -> Option<&[u8]> {
                self.0.prev()
            }
        }

        impl<$cmp: Comparator<[u8]>> Seekable<[u8], Cmp> for $lending_iter<$cmp> {
            #[inline]
            fn reset(&mut self) {
                self.0.reset();
            }

            fn seek(&mut self, min_bound: &[u8]) {
                self.0.seek(min_bound);
            }

            fn seek_before(&mut self, strict_upper_bound: &[u8]) {
                self.0.seek_before(strict_upper_bound);
            }

            #[inline]
            fn seek_to_first(&mut self) {
                self.0.seek_to_first();
            }

            fn seek_to_last(&mut self) {
                self.0.seek_to_last();
            }
        }
    };
}
