/// For a given `List` which implements [`SkiplistSeek`], this macro creates a wrapper type
/// around [`SkiplistIter<'a, List>`] that implements [`SkiplistIterator<'a>`] and
/// [`Iterator`].
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
/// in addition to implementations of [`SkiplistIterator<'a>`] and `Iterator<Item = &'a [u8]>`.
///
/// # Hygiene / Environment
///
/// The [`Comparator`], [`SkiplistIterator`], and [`Iterator`] traits must be in scope,
/// and the [`SkiplistIter`], [`Option`], and [`u8`] types must be in scope. That is, this macro
/// does not fully qualify all its types, traits, and method calls.
///
/// [`Comparator`]: crate::interface::Comparator
/// [`SkiplistIterator`]: crate::interface::SkiplistIterator
/// [`SkiplistIterator<'a>`]: crate::interface::SkiplistIterator
/// [`SkiplistSeek`]: crate::iter_defaults::SkiplistSeek
/// [`SkiplistIter`]: crate::iter_defaults::SkiplistIter
/// [`SkiplistIter<'a, List>`]: crate::iter_defaults::SkiplistIter
#[macro_export]
macro_rules! skiplistiter_wrapper {
    {
        $(#[$meta:meta])*
        $vis:vis struct $iter:ident<'_, $cmp:ident: _>(#[List = $list:ty] _ $(,)?);
    } => {
        $(#[$meta])*
        $vis struct $iter<'a, $cmp: Comparator>(SkiplistIter<'a, $list>);

        impl<'a, $cmp: Comparator> Iterator for $iter<'a, $cmp> {
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

        impl<'a, $cmp: Comparator> SkiplistIterator<'a> for $iter<'a, $cmp> {
            #[inline]
            fn is_valid(&self) -> bool {
                self.0.is_valid()
            }

            #[inline]
            fn reset(&mut self) {
                self.0.reset();
            }

            #[inline]
            fn current(&self) -> Option<&'a [u8]> {
                self.0.current()
            }

            fn prev(&mut self) -> Option<&'a [u8]> {
                self.0.prev()
            }

            fn seek(&mut self, min_bound: &[u8]) {
                self.0.seek(min_bound);
            }

            #[inline]
            fn seek_to_first(&mut self) {
                self.0.seek_to_first();
            }

            fn seek_to_end(&mut self) {
                self.0.seek_to_end();
            }
        }
    };
}

/// For a given `List` which implements [`SkiplistSeek`], this macro creates a wrapper type
/// around [`SkiplistLendingIter<List>`] that implements [`SkiplistLendingIterator`].
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
/// in addition to an implementation of [`SkiplistLendingIterator`].
///
/// # Hygiene / Environment
///
/// The [`Comparator`] and [`SkiplistLendingIterator`] traits must be in scope, and the
/// [`SkiplistLendingIter`], [`Option`], and [`u8`] types must be in scope. That is, this macro
/// does not fully qualify all its types, traits, and method calls.
///
/// [`Comparator`]: crate::interface::Comparator
/// [`SkiplistLendingIterator`]: crate::interface::SkiplistLendingIterator
/// [`SkiplistSeek`]: crate::iter_defaults::SkiplistSeek
/// [`SkiplistLendingIter`]: crate::iter_defaults::SkiplistLendingIter
/// [`SkiplistLendingIter<List>`]: crate::iter_defaults::SkiplistLendingIter
#[macro_export]
macro_rules! skiplistlendingiter_wrapper {
    {
        $(#[$meta:meta])*
        $vis:vis struct $lending_iter:ident<$cmp:ident: _>(#[List = $list:ty] _ $(,)?);
    } => {
        $(#[$meta])*
        $vis struct $lending_iter<$cmp: Comparator>(SkiplistLendingIter<$list>);

        impl<$cmp: Comparator> SkiplistLendingIterator for $lending_iter<$cmp> {
            #[inline]
            fn is_valid(&self) -> bool {
                self.0.is_valid()
            }

            #[inline]
            fn reset(&mut self) {
                self.0.reset();
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

            fn seek(&mut self, min_bound: &[u8]) {
                self.0.seek(min_bound);
            }

            #[inline]
            fn seek_to_first(&mut self) {
                self.0.seek_to_first();
            }

            fn seek_to_end(&mut self) {
                self.0.seek_to_end();
            }
        }
    };
}
