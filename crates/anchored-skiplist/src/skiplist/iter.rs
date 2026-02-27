#![expect(unsafe_code, reason = "Associate raw iter state with its backing skiplist")]
// The `unsafe` code reduces code duplication. Additionally, since `SkiplistLendingIter`
// is self-referential, some quantity of `unsafe` is unavoidable.

use core::fmt::{Debug, Formatter, Result as FmtResult};

use variance_family::UpperBound;

use crate::{
    interface::{Entry, Key, SkiplistFormat},
    raw_skiplist::{RawSkiplist, RawSkiplistIterState, RawSkiplistIterView, RawSkiplistIterViewMut},
};
use super::structs::SkiplistReader;


pub struct SkiplistIter<'a, F, U, Cmp> {
    list:  &'a RawSkiplist<F, U>,
    cmp:   &'a Cmp,
    state: RawSkiplistIterState<F, U>,
}

impl<'a, F, U, Cmp> SkiplistIter<'a, F, U, Cmp> {
    #[inline]
    #[must_use]
    pub(super) const fn new(list: &'a RawSkiplist<F, U>, cmp: &'a Cmp) -> Self {
        Self {
            list,
            cmp,
            state: RawSkiplistIterState::new(),
        }
    }
}

impl<'a, F: SkiplistFormat<U>, U: UpperBound> Iterator for SkiplistIter<'a, F, U, F::Cmp> {
    type Item = Entry<'a, F, U>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.view_mut().next()
    }

    #[inline]
    fn fold<B, Func>(mut self, init: B, f: Func) -> B
    where
        Self: Sized,
        Func: FnMut(B, Self::Item) -> B,
    {
        self.view_mut().fold(init, f)
    }
}

impl<'a, F: SkiplistFormat<U>, U: UpperBound> SkiplistIter<'a, F, U, F::Cmp> {
    #[inline]
    #[must_use]
    const fn view(&self) -> RawSkiplistIterView<'a, '_, F, U, F::Cmp> {
        // SAFETY: we keep `self.state` and `self.list` associated with this iter. We do not
        // expose mutable access to them in a way that could let them be unexpectedly switched out.
        // In other words, we fulfill the requirement of always passing the same raw skiplist
        // to the `view` and `view_mut` methods of `self.state`.
        unsafe { self.state.view(self.list, self.cmp) }
    }

    #[inline]
    #[must_use]
    const fn view_mut(&mut self) -> RawSkiplistIterViewMut<'a, '_, F, U, F::Cmp> {
        // SAFETY: we keep `self.state` and `self.list` associated with this iter. We do not
        // expose mutable access to them in a way that could let them be unexpectedly switched out.
        // In other words, we fulfill the requirement of always passing the same raw skiplist
        // to the `view` and `view_mut` methods of `self.state`.
        unsafe { self.state.view_mut(self.list, self.cmp) }
    }

    #[inline]
    #[must_use]
    pub const fn skiplist_cmp(&self) -> &'a F::Cmp {
        self.cmp
    }

    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.view().valid()
    }

    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<Entry<'a, F, U>> {
        self.view().current()
    }

    #[must_use]
    pub fn prev_without_duplicates(&mut self) -> Option<Entry<'a, F, U>> {
        self.view_mut().prev_without_duplicates()
    }

    #[must_use]
    pub fn prev(&mut self) -> Option<Entry<'a, F, U>> {
        self.view_mut().prev()
    }

    pub const fn reset(&mut self) {
        self.view_mut().reset();
    }

    pub fn seek(&mut self, lower_bound: Key<'_, F, U>) {
        self.view_mut().seek(lower_bound);
    }

    pub fn seek_before(&mut self, strict_upper_bound: Key<'_, F, U>) {
        self.view_mut().seek_before(strict_upper_bound);
    }

    pub fn seek_to_first(&mut self) {
        self.view_mut().seek_to_first();
    }

    pub fn seek_to_last(&mut self) {
        self.view_mut().seek_to_last();
    }
}

impl<F: SkiplistFormat<U>, U: UpperBound> Clone for SkiplistIter<'_, F, U, F::Cmp> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            list:  self.list,
            cmp:   self.cmp,
            state: self.state.clone(),
        }
    }
}

impl<F, U> Debug for SkiplistIter<'_, F, U, F::Cmp>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    F::Cmp: Debug,
    for<'a> Entry<'a, F, U>: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        self.view().debug(f, "SkiplistIter")
    }
}

#[expect(missing_debug_implementations, reason = "not a priority. TODO: debug impls")]
pub struct SkiplistLendingIter<F, U, Cmp> {
    list:  SkiplistReader<F, U, Cmp>,
    state: RawSkiplistIterState<F, U>,
}

impl<F, U, Cmp> SkiplistLendingIter<F, U, Cmp> {
    #[inline]
    #[must_use]
    pub(super) const fn new(list: SkiplistReader<F, U, Cmp>) -> Self {
        Self {
            list,
            state: RawSkiplistIterState::new(),
        }
    }

    #[inline]
    #[must_use]
    pub fn into_skiplist(self) -> SkiplistReader<F, U, Cmp> {
        self.list
    }
}

impl<F: SkiplistFormat<U>, U: UpperBound> SkiplistLendingIter<F, U, F::Cmp> {
    #[inline]
    #[must_use]
    fn view(&self) -> RawSkiplistIterView<'_, '_, F, U, F::Cmp> {
        let (list, cmp) = self.list.raw_parts();
        // SAFETY: we keep `self.state` and `self.list` associated with this iter. We do not
        // expose mutable access to them in a way that could let them be unexpectedly switched out.
        // In other words, we fulfill the requirement of always passing the same raw skiplist
        // to the `view` and `view_mut` methods of `self.state`.
        unsafe { self.state.view(list, cmp) }
    }

    #[inline]
    #[must_use]
    fn view_mut(&mut self) -> RawSkiplistIterViewMut<'_, '_, F, U, F::Cmp> {
        let (list, cmp) = self.list.raw_parts();
        // SAFETY: we keep `self.state` and `self.list` associated with this iter. We do not
        // expose mutable access to them in a way that could let them be unexpectedly switched out.
        // In other words, we fulfill the requirement of always passing the same raw skiplist
        // to the `view` and `view_mut` methods of `self.state`.
        unsafe { self.state.view_mut(list, cmp) }
    }

    #[inline]
    #[must_use]
    pub fn skiplist_cmp(&self) -> &F::Cmp {
        self.list.cmp()
    }

    #[inline]
    #[must_use]
    pub fn valid(&self) -> bool {
        self.view().valid()
    }

    #[expect(clippy::should_implement_trait, reason = "this is a lending iter")]
    #[inline]
    #[must_use]
    pub fn next(&mut self) -> Option<Entry<'_, F, U>> {
        self.view_mut().next()
    }

    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<Entry<'_, F, U>> {
        self.view().current()
    }

    #[must_use]
    pub fn prev_without_duplicates(&mut self) -> Option<Entry<'_, F, U>> {
        self.view_mut().prev_without_duplicates()
    }

    #[must_use]
    pub fn prev(&mut self) -> Option<Entry<'_, F, U>> {
        self.view_mut().prev()
    }

    pub fn reset(&mut self) {
        self.view_mut().reset();
    }

    pub fn seek(&mut self, lower_bound: Key<'_, F, U>) {
        self.view_mut().seek(lower_bound);
    }

    pub fn seek_before(&mut self, strict_upper_bound: Key<'_, F, U>) {
        self.view_mut().seek_before(strict_upper_bound);
    }

    pub fn seek_to_first(&mut self) {
        self.view_mut().seek_to_first();
    }

    pub fn seek_to_last(&mut self) {
        self.view_mut().seek_to_last();
    }
}

impl<F: SkiplistFormat<U>, U: UpperBound> Clone for SkiplistLendingIter<F, U, F::Cmp> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            list:  self.list.clone(),
            state: self.state.clone(),
        }
    }
}
