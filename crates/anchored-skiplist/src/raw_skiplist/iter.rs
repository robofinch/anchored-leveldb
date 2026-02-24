#![expect(unsafe_code, reason = "Lifetime erasure with `ErasedNodeRef`")]
#![expect(clippy::undocumented_unsafe_blocks, reason = "temporary. TODO: fix this")]
// The `unsafe` code reduces code duplication. Additionally, since `SkiplistLendingIter`
// is self-referential, some quantity of `unsafe` is unavoidable.

use core::fmt::{Debug, Formatter, Result as FmtResult};

use variance_family::UpperBound;

use crate::interface::{Entry, Key, SkiplistFormat};
use super::list::RawSkiplist;
use super::node::{ErasedNodeRef, NodeRef};


pub struct RawSkiplistIterState<F, U> {
    cursor: Option<ErasedNodeRef<F, U>>,
}

impl<F, U> RawSkiplistIterState<F, U> {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cursor: None,
        }
    }
}

impl<F, U> Debug for RawSkiplistIterState<F, U>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("RawSkiplistIterState")
            .field("cursor", &self.cursor)
            .finish()
    }
}

impl<F, U> Default for RawSkiplistIterState<F, U> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<F, U> Clone for RawSkiplistIterState<F, U> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            cursor: self.cursor,
        }
    }
}

impl<F: SkiplistFormat<U>, U: UpperBound> RawSkiplistIterState<F, U> {
    /// # Safety
    /// If `view_mut` has previously been called on this `self` value, then `list` must be the same
    /// skiplist that was provided to that invocation of `view_mut`.
    ///
    /// For the purposes of this safety comment, calling `self.view_mut(_, _)` preserves the
    /// "identity" of `self`, whereas overwriting `self`
    /// (e.g. `*self = RawSkiplistIterState::new()`) does *not* preserve the identity of `self`.
    /// `self.clone()` yields an equal value that should have the same "identity" as `self`.
    ///
    /// Rust generally does not have a strong sense of object identity, making the concept
    /// of "this `self` value" somewhat fuzzy.
    ///
    /// Essentially, after calling `self.view_mut(list, _)`, `self` may contain a lifetime-erased
    /// reference to data of `list`. Dropping or resetting `list` and keeping around `self`
    /// does not cause a problem by itself (otherwise, the lifetime-erasure would be unsound),
    /// but dropping or resetting `list` *and then* calling `self.view(other_list, _)` or
    /// `self.view_mut(other_list, _)` may cause undefined behavior.
    #[inline]
    #[must_use]
    pub const unsafe fn view<'a, 'b>(
        &'b self,
        list: &'a RawSkiplist<F, U>,
        cmp:  &'b F::Cmp,
    ) -> RawSkiplistIterView<'a, 'b, F, U, F::Cmp> {
        RawSkiplistIterView {
            list,
            cmp,
            state: self,
        }
    }

    /// # Safety
    /// If `view_mut` has previously been called on this `self` value, then `list` must be the same
    /// skiplist that was provided to that invocation of `view_mut`.
    ///
    /// For the purposes of this safety comment, calling `self.view_mut(_, _)` preserves the
    /// "identity" of `self`, whereas overwriting `self`
    /// (e.g. `*self = RawSkiplistIterState::new()`) does *not* preserve the identity of `self`.
    /// `self.clone()` yields an equal value that should have the same "identity" as `self`.
    ///
    /// Rust generally does not have a strong sense of object identity, making the concept
    /// of "this `self` value" somewhat fuzzy.
    ///
    /// Essentially, after calling `self.view_mut(list, _)`, `self` may contain a lifetime-erased
    /// reference to data of `list`. Dropping or resetting `list` and keeping around `self`
    /// does not cause a problem by itself (otherwise, the lifetime-erasure would be unsound),
    /// but dropping or resetting `list` *and then* calling `self.view(other_list, _)` or
    /// `self.view_mut(other_list, _)` may cause undefined behavior.
    #[inline]
    #[must_use]
    pub const unsafe fn view_mut<'a, 'b>(
        &'b mut self,
        list: &'a RawSkiplist<F, U>,
        cmp:  &'b F::Cmp,
    ) -> RawSkiplistIterViewMut<'a, 'b, F, U, F::Cmp> {
        RawSkiplistIterViewMut {
            list,
            cmp,
            state: self,
        }
    }
}

pub struct RawSkiplistIterView<'a, 'b, F, U, Cmp> {
    list:   &'a RawSkiplist<F, U>,
    cmp:    &'b Cmp,
    state:  &'b RawSkiplistIterState<F, U>,
}

impl<'a, F: SkiplistFormat<U>, U: UpperBound> RawSkiplistIterView<'a, '_, F, U, F::Cmp> {
     #[inline]
    #[must_use]
    pub const fn list(&self) -> &'a RawSkiplist<F, U> {
        self.list
    }

    #[inline]
    #[must_use]
    pub const fn skiplist_cmp(&self) -> &F::Cmp {
        self.cmp
    }

    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.state.cursor.is_some()
    }

    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<Entry<'a, F, U>> {
        self.state.cursor.map(|current| {
            let current: NodeRef<'a, F, U> = unsafe { current.unerase() };
            current.entry()
        })
    }
}

impl<F, U, Cmp> Copy for RawSkiplistIterView<'_, '_, F, U, Cmp> {}

impl<F, U, Cmp> Clone for RawSkiplistIterView<'_, '_, F, U, Cmp> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, F, U> RawSkiplistIterView<'a, '_, F, U, F::Cmp>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    F::Cmp: Debug,
    for<'b> Entry<'b, F, U>: Debug,
{
    /// Helper utility for debugging a `Struct { list, cmp, state }` triple with the given
    /// `name`.
    pub fn debug(&self, f: &mut Formatter<'_>, name: &'static str) -> FmtResult {
        struct FromFn<F>(F);

        impl<F: Fn(&mut Formatter<'_>) -> FmtResult> Debug for FromFn<F> {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                self.0(f)
            }
        }

        f.debug_struct(name)
            .field("list",  &"<&RawSkiplist>")
            .field("cmp",   &self.cmp)
            .field("state", &FromFn(|inner_f: &mut Formatter<'_>| {
                let cursor: Option<NodeRef<'a, F, U>> = self.state.cursor
                    .map(|node| unsafe { node.unerase() });

                inner_f.debug_struct("RawSkiplistIterState")
                    .field("cursor", &cursor)
                    .finish()
            }))
            .finish()
    }
}

impl<F, U> Debug for RawSkiplistIterView<'_, '_, F, U, F::Cmp>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    F::Cmp: Debug,
    for<'a> Entry<'a, F, U>: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        self.debug(f, "RawSkiplistIterView")
    }
}

pub struct RawSkiplistIterViewMut<'a, 'b, F, U, Cmp> {
    list:   &'a RawSkiplist<F, U>,
    cmp:    &'b Cmp,
    state:  &'b mut RawSkiplistIterState<F, U>,
}

impl<'a, F: SkiplistFormat<U>, U: UpperBound> Iterator
for RawSkiplistIterViewMut<'a, '_, F, U, F::Cmp>
{
    type Item = Entry<'a, F, U>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let next_node = if let Some(current) = self.state.cursor {
            let current: NodeRef<'a, F, U> = unsafe { current.unerase() };
            current.next_node()
        } else {
            self.list.get_first()
        };
        self.state.cursor = next_node.map(NodeRef::erase);

        next_node.map(NodeRef::entry)
    }

    #[inline]
    fn fold<B, Func>(self, init: B, mut f: Func) -> B
    where
        Func: FnMut(B, Self::Item) -> B,
    {
        // Having this branch once instead of `n` times seems slightly better than the default
        // `fold` implementation.
        if self.state.cursor.is_none() {
            self.state.cursor = self.list.get_first().map(NodeRef::erase);
        }

        let mut accumulator = init;
        while let Some(current) = self.state.cursor {
            let current: NodeRef<'a, F, U> = unsafe { current.unerase() };
            self.state.cursor = current.next_node().map(NodeRef::erase);
            accumulator = f(accumulator, current.entry());
        }

        accumulator
    }
}

impl<'a, 'b, F: SkiplistFormat<U>, U: UpperBound> RawSkiplistIterViewMut<'a, 'b, F, U, F::Cmp> {
    #[inline]
    #[must_use]
    pub const fn reborrow(&mut self) -> RawSkiplistIterViewMut<'a, '_, F, U, F::Cmp> {
        RawSkiplistIterViewMut {
            list:  self.list,
            cmp:   self.cmp,
            state: self.state,
        }
    }

    #[inline]
    #[must_use]
    pub const fn downgrade_ref(&self) -> RawSkiplistIterView<'a, '_, F, U, F::Cmp> {
        RawSkiplistIterView {
            list:  self.list,
            cmp:   self.cmp,
            state: self.state,
        }
    }

    #[inline]
    #[must_use]
    pub const fn downgrade(self) -> RawSkiplistIterView<'a, 'b, F, U, F::Cmp> {
        RawSkiplistIterView {
            list:  self.list,
            cmp:   self.cmp,
            state: self.state,
        }
    }

    #[inline]
    #[must_use]
    pub const fn list(&self) -> &'a RawSkiplist<F, U> {
        self.list
    }

    #[inline]
    #[must_use]
    pub const fn skiplist_cmp(&self) -> &F::Cmp {
        self.cmp
    }

    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.state.cursor.is_some()
    }

    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<Entry<'a, F, U>> {
        self.state.cursor.map(|current| {
            let current: NodeRef<'a, F, U> = unsafe { current.unerase() };
            current.entry()
        })
    }

    #[must_use]
    pub fn prev_without_duplicates(&mut self) -> Option<Entry<'a, F, U>> {
        let prev_node = if let Some(current) = self.state.cursor {
            let current: NodeRef<'a, F, U> = unsafe { current.unerase() };
            self.list.find_strictly_less(self.cmp, current.key())
        } else {
            self.list.find_last()
        };
        self.state.cursor = prev_node.map(NodeRef::erase);

        prev_node.map(NodeRef::entry)
    }

    #[must_use]
    pub fn prev(&mut self) -> Option<Entry<'a, F, U>> {
        let prev_node = if let Some(current) = self.state.cursor {
            let current: NodeRef<'a, F, U> = unsafe { current.unerase() };
            self.prev_from_some(current)
        } else {
            self.list.find_last()
        };
        self.state.cursor = prev_node.map(NodeRef::erase);

        prev_node.map(NodeRef::entry)
    }

    #[must_use]
    fn prev_from_some(&self, current: NodeRef<'a, F, U>) -> Option<NodeRef<'a, F, U>> {
        let strictly_less = self.list.find_strictly_less(self.cmp, current.key());
        // `prev_candidate` should be strictly before `current`. We then need to check whether
        // `current` is its successor. (This is needed in order to handle the case where there
        // are several entries with key equal to `current.key()`, and `find_strictly_less`
        // may have gone too far back.)
        let mut prev_candidate = if let Some(strictly_less) = strictly_less {
            strictly_less
        } else {
            // If `self.list.get_first()` is `None`, then there is no prev node, since the skiplist
            // is empty.
            let maybe_prev = self.list.get_first()?;

            if maybe_prev.ptr_eq(current) {
                // Turns out that `current` is the first element of the skiplist. There is no prev
                // element.
                return None;
            } else {
                // It's strictly before `current` in the skiplist.
                maybe_prev
            }
        };

        let current_next = current.next_node();

        loop {
            let Some(prev_next) = prev_candidate.next_node() else {
                // This branch shouldn't be able to happen in a single-threaded program, since
                // `prev_candidate` is always strictly less than `current`, so `next_node()`
                // should, at worst, find `current`. However, consider the following chain
                // of events:
                //
                // - The node which will be `current` begins to be inserted into the skiplist.
                //   It happens to be the first node of height `10` and is the last node in the
                //   skiplist. There is a previously existing node... say, "`prev_candidate`",
                //   which was the former last node in the skiplist.
                // - `current` gets fully initialized.
                // - `current` begins to be inserted. The `Relaxed` load and store to
                //   the list's `current_height` field get reordered to *before* the `Release`
                //   stores publishing `current`, and are executed.
                // - The `Release` store to level 9 of the skiplist gets reordered to before
                //   the other publishes, and only it is executed.
                // - The thread publishing `current` gets put to sleep by the OS for a while.
                //
                // - Some iterator on a different thread calls `seek_to_last`. That thread
                //   sees that `current_height` is 10, reads the link to `current` on level 9,
                //   sees that `current.next_node()` is `None`, and sets the iterator's cursor
                //   to `Some(current)` since that's the last node.
                // - That thread calls `prev` on that iterator. Since the cursor is `Some`,
                //   we enter this function. `find_strictly_less` is called. It first tries
                //   level 9, sees that it needs to backtrack, goes down to lower levels, and
                //   eventually finds its way to `prev_candidate`. It doesn't even see `current`
                //   or any entry with `current.key()`, but it sees that `prev_candidate`'s next
                //   node is `None`, which compares as greater than `current.key()`, while
                //   `prev_candidate`'s key is less than `current.key()`. Therefore, that function
                //   returns `Some(prev_candidate)`, which is placed in `strictly_less`, and is then
                //   unwrapped into what is called `prev_candidate` in this function. (Surprise.)
                // - `prev_candidate.next_node()` is evaluated and is `None`.
                // - We reach this branch. (Realistically, this still *never* happens.)
                //
                // Since only one node can be in the process of being published at a time,
                // we can't somehow end up missing *two* nodes like this, only one. But this does
                // mean that we need to check both `ptr_eq` between `prev_candidate->next`
                // and `current` and between `prev_candidate->next` and `current->next`; before
                // `current` even starts to be published, `current->next` is set to
                // `current->prev->next`, and thus if `prev_candidate->next` is `current->next`,
                // it follows that `prev_candidate` is in fact the desired previous node.
                // This branch amounts to noticing that `prev_candidate->next` is `None`, which
                // could only possibly occur if `current->next` is `None` and it's in the process
                // of being published, implying that this is the desired previous node.
                break Some(prev_candidate);
            };

            if prev_next.ptr_eq(current)
                || current_next.is_some_and(|cur_next| prev_next.ptr_eq(cur_next))
            {
                // See long reasoning above.
                break Some(prev_candidate);
            }

            prev_candidate = prev_next;
        }
    }

    #[inline]
    pub const fn reset(&mut self) {
        self.state.cursor = None;
    }

    pub fn seek(&mut self, lower_bound: Key<'_, F, U>) {
        let seeked_node = self.list.find_greater_or_equal(self.cmp, lower_bound);
        self.state.cursor = seeked_node.map(NodeRef::erase);
    }

    pub fn seek_before(&mut self, strict_upper_bound: Key<'_, F, U>) {
        let seeked_node = self.list.find_strictly_less(self.cmp, strict_upper_bound);
        self.state.cursor = seeked_node.map(NodeRef::erase);
    }

    pub fn seek_to_first(&mut self) {
        let seeked_node = self.list.get_first();
        self.state.cursor = seeked_node.map(NodeRef::erase);
    }

    pub fn seek_to_last(&mut self) {
        let seeked_node = self.list.find_last();
        self.state.cursor = seeked_node.map(NodeRef::erase);
    }
}

impl<F, U> Debug for RawSkiplistIterViewMut<'_, '_, F, U, F::Cmp>
where
    F: SkiplistFormat<U>,
    U: UpperBound,
    F::Cmp: Debug,
    for<'a> Entry<'a, F, U>: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        self.downgrade_ref().debug(f, "RawSkiplistIterViewMut")
    }
}
