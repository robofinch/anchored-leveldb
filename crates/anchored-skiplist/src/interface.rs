use seekable_iterator::{Comparator, LendItem, SeekableIterator, SeekableLendingIterator};


/// A minimal [skiplist] interface which allows entries to be inserted but never removed.
///
/// Implementations may or may not be threadsafe. Even if an implementation is threadsafe,
/// newly-added entries may or may not be seen immediately by other threads.
///
/// If a thread panics while inserting into the skiplist, or panics while holding a [`WriteLocked`],
/// all other attempts to insert into the skiplist may or may not panic as well, depending on
/// whether an implementation can encounter [poison errors] and how they are handled.
///
/// [skiplist]: https://en.wikipedia.org/wiki/Skip_list
/// [`WriteLocked`]: Skiplist::WriteLocked
/// [poison errors]: std::sync::PoisonError
// TODO(feature): consider providing ways to gracefully error upon poisoned mutexes.
// As panics are not something most people have an interest in recovering from, this is
// not a priority.
pub trait Skiplist<Cmp: Comparator<[u8]>>: Sized {
    /// A version of the skiplist which holds any write locks needed for insertions until it is
    /// dropped or released with [`Self::write_unlocked`], instead of acquiring those locks only
    /// while performing insertions.
    ///
    /// If the skiplist implementation does not have any such locks to acquire (or is itself
    /// a `WriteLocked` type which already holds those locks), `WriteLocked` should be set to
    /// `Self`.
    ///
    /// If a thread panics while inserting into the skiplist, or panics while holding a
    /// `WriteLocked`, all other attempts to insert into the skiplist may or may not panic as well,
    /// depending on whether an implementation can encounter [poison errors] and how they are
    /// handled.
    ///
    /// [`Self::write_unlocked`]: Skiplist::write_unlocked
    /// [poison errors]: std::sync::PoisonError
    type WriteLocked: Skiplist<Cmp>;
    type Iter<'a>:    SeekableIterator<[u8], Cmp, Item = &'a [u8]> where Self: 'a;
    type LendingIter:
        SeekableLendingIterator<[u8], Cmp>
        + for<'lend> LendItem<'lend, Item = &'lend [u8]>;

    #[inline]
    #[must_use]
    fn new(cmp: Cmp) -> Self {
        // Figured I'd use the fun default seed at
        // https://github.com/google/leveldb/blob/ac691084fdc5546421a55b25e7653d450e5a25fb/db/skiplist.h#L322-L328
        Self::new_seeded(cmp, 0x_deadbeef)
    }

    #[must_use]
    fn new_seeded(cmp: Cmp, seed: u64) -> Self;

    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `init_entry`.
    ///
    /// If the resulting entry compares equal to an entry already in the skiplist, the entry
    /// is discarded, and `false` is returned. Otherwise, `true` is returned. Attempting to add
    /// duplicate entries should be avoided, as skiplist implementations might not reclaim the
    /// spent memory until the skiplist is dropped.
    ///
    /// # Panics or Deadlocks
    /// Implementatations may panic or deadlock if the `init_entry` callback attempts to call
    /// [`insert_with`], [`insert_copy`], or [`write_locked`] on the skiplist (including via
    /// reference-counted clones). Specific implementations may indicate otherwise.
    ///
    /// If a thread panics while inserting into the skiplist, or panics while holding a
    /// [`WriteLocked`], all other attempts to insert into the skiplist may or may not panic as
    /// well, depending on whether an implementation can encounter [poison errors] and how they are
    /// handled.
    ///
    /// [`insert_with`]: Skiplist::insert_with
    /// [`insert_copy`]: Skiplist::insert_copy
    /// [`write_locked`]: Skiplist::write_locked
    /// [`WriteLocked`]: Skiplist::WriteLocked
    /// [poison errors]: std::sync::PoisonError
    fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F) -> bool;

    /// Insert the provided data into the skiplist, incurring a copy to create an owned version of
    /// the entry.
    ///
    /// If the resulting entry compares equal to an entry already in the skiplist, the entry
    /// is discarded, and `false` is returned. Otherwise, `true` is returned. Attempting to add
    /// duplicate entries should be avoided, as skiplist implementations might not reclaim the
    /// spent memory until the skiplist is dropped.
    #[inline]
    fn insert_copy(&mut self, entry: &[u8]) -> bool {
        self.insert_with(
            entry.len(),
            |created_entry| created_entry.copy_from_slice(entry),
        )
    }

    /// Signal to the skiplist implementation that it should acquire and hold any write locks
    /// it needs for insertions, to improve the speed of following writes.
    ///
    /// If the skiplist implementation does not have any locks to acquire, or this skiplist is
    /// already a `WriteLocked` type which has acquired those locks, this function should be a
    /// no-op which returns `Self`.
    ///
    /// Dropping the returned `WriteLocked` or using [`Self::write_unlocked`] should release
    /// any write locks newly acquired by this function.
    ///
    /// # Panics or Deadlocks
    /// After the current thread obtains a `WriteLocked`, implementations may panic or deadlock
    /// if that same thread attempts to call [`insert_with`], [`insert_copy`], or [`write_locked`]
    /// on a reference-counted clone of the skiplist *other* than the returned `WriteLocked`. That
    /// is, the thread should attempt to mutate the skiplist only through the returned
    /// `WriteLocked`, while it exists.
    ///
    /// This function may block if a different thread holds write locks, perhaps for a long period
    /// of time if that thread has acquired a `WriteLocked`.
    ///
    /// Additionally, note that if the thread panics while holding the write locks, the related
    /// mutexes may become poisoned and lead to later panics on other threads.
    ///
    /// Specific implementations may indicate otherwise.
    ///
    /// [`insert_with`]: Skiplist::insert_with
    /// [`insert_copy`]: Skiplist::insert_copy
    /// [`write_locked`]: Skiplist::write_locked
    /// [`Self::write_unlocked`]: Skiplist::write_unlocked
    #[must_use]
    fn write_locked(self) -> Self::WriteLocked;

    /// Unless [`Self::write_locked`] was a no-op, release any locks required for insertions,
    /// and return to acquiring them only inside the insertion functions.
    ///
    /// If [`Self::write_locked`] was a no-op, then this function should be a no-op which returns
    /// the provided `list`.
    ///
    /// [`Self::write_locked`]: Skiplist::write_locked
    #[must_use]
    fn write_unlocked(list: Self::WriteLocked) -> Self;

    /// Check whether the entry, or something which compares as equal to the entry, is in
    /// the skiplist.
    #[must_use]
    fn contains(&self, entry: &[u8]) -> bool;

    /// Get an iterator that can seek through the skiplist to read entries.
    ///
    /// See [`SeekableIterator`] and [`CursorIterator`] for more.
    ///
    /// [`CursorIterator`]: seekable_iterator::CursorIterator
    #[must_use]
    fn iter(&self) -> Self::Iter<'_>;

    /// Move the skiplist into a lending iterator which can seek through the list to read entries.
    ///
    /// See [`SeekableLendingIterator`] and [`CursorLendingIterator`] for more.
    ///
    /// [`CursorLendingIterator`]: seekable_iterator::CursorLendingIterator
    #[must_use]
    fn lending_iter(self) -> Self::LendingIter;

    /// Reclaim the underlying skiplist from a lending iterator.
    #[must_use]
    fn from_lending_iter(lending_iter: Self::LendingIter) -> Self;
}
