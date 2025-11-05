macro_rules! tests_for_all_skiplists {
    ($skiplist:ident, $iter:ident, $lending_iter:ident $(,)?) => {
        // ================================
        //  Empty List
        // ================================

        #[cfg(any(not(miri), tests_with_leaks))]
        #[test]
        fn empty_list_leaky() {
            let list = $skiplist::new(OrdComparator);

            assert!(!list.contains(&[]));
            assert!(!list.contains(&[0]));
            assert!(!list.contains(&[255]));

            let list = Box::leak(Box::new(list));

            assert!(!list.contains(&[]));
            assert!(!list.contains(&[0]));
            assert!(!list.contains(&[255]));

            let _check_that_debug_works = format!("{list:?}");
        }

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn empty_list() {
            let list = $skiplist::new(OrdComparator);

            assert!(!list.contains(&[]));
            assert!(!list.contains(&[0]));
            assert!(!list.contains(&[255]));

            let list = Box::new(list);

            assert!(!list.contains(&[]));
            assert!(!list.contains(&[0]));
            assert!(!list.contains(&[255]));

            let _check_that_debug_works = format!("{list:?}");
        }

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn empty_list_iter() {
            let list = &$skiplist::new(OrdComparator);

            let mut iter: $iter<'_, OrdComparator> = list.into_iter();

            assert!(!iter.valid());
            assert!(iter.current().is_none());
            assert!(iter.next().is_none());
            assert!(iter.prev().is_none());
            assert!(!iter.valid());

            iter.reset();
            assert!(!iter.valid());

            iter.seek_to_first();
            assert!(iter.current().is_none());
            iter.seek_to_last();
            assert!(iter.current().is_none());

            // Move the iter to a different address
            let mut iter = iter;

            iter.seek(&[]);
            assert!(iter.current().is_none());
            iter.seek(&[0, 0, 0, 0, 0, 0]);
            assert!(iter.current().is_none());
            iter.seek(&[255]);
            assert!(iter.current().is_none());
            iter.seek_before(&[255]);
            assert!(iter.current().is_none());

            let _check_that_debug_works = format!("{iter:?}");
        }

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn empty_list_lending_iter() {
            let list = $skiplist::new(OrdComparator);

            let mut iter: $lending_iter<OrdComparator> = list.lending_iter();

            assert!(!iter.valid());
            assert!(iter.current().is_none());
            assert!(iter.next().is_none());
            assert!(iter.prev().is_none());
            assert!(!iter.valid());

            iter.reset();
            assert!(!iter.valid());

            let mut iter = Box::new(iter);

            iter.seek_to_first();
            assert!(iter.current().is_none());
            iter.seek_to_last();
            assert!(iter.current().is_none());

            let list = $skiplist::from_lending_iter(*iter);
            let mut iter = list.lending_iter();

            iter.seek(&[]);
            assert!(iter.current().is_none());
            iter.seek(&[0, 0, 0, 0, 0, 0]);
            assert!(iter.current().is_none());
            iter.seek(&[255]);
            assert!(iter.current().is_none());
            iter.seek_before(&[255]);
            assert!(iter.current().is_none());

            let _check_that_debug_works = format!("{iter:?}");
        }

        // ================================
        //  Iter lifetime extension
        // ================================

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn extend_iter_reference() {
            #![expect(unsafe_code, reason = "Confirm claims of safety of lifetime extension")]

            let mut list: $skiplist<OrdComparator> = Default::default();
            list.insert_copy(&[0]);

            let mut iter = list.iter();
            iter.seek_to_first();

            let data = iter.current().unwrap();
            assert_eq!(data, &[0]);

            drop(iter);

            // No unsafe needed yet. The list hasn't been moved.
            let data = data;

            // Now, asserting that the data remains valid even though the list has been moved?
            // That requires `unsafe`.
            let data: *const [u8] = data;
            // SAFETY: `data` is a non-null, properly-aligned, dereferenceable pointer to a valid
            // value of type `&'source [u8]` that satisfies aliasing for at least `'source`.
            // We're asserting that it lasts even longer. Since this came from
            // `anchored_skiplist::simple::Iter::current`
            // or `anchored_skiplist::concurrent::Iter::current`,
            // the safety guarantees made by `$iter` apply, and since the backing skiplist is not
            // invalidated until the end of this function (aside from being moved), extending the
            // lifetime to the end of this function is sound.
            let data: &[u8] = unsafe { &*data };
            let list = list;

            let same_data = list.iter().next().unwrap();

            assert_eq!(data, &[0]);
            assert_eq!(same_data, &[0]);
            assert_eq!(data.as_ptr(), same_data.as_ptr());
        }

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn extend_lending_iter_reference() {
            #![expect(unsafe_code, reason = "Confirm claims of safety of lifetime extension")]

            let mut list: $skiplist<OrdComparator> = Default::default();
            list.insert_copy(&[0]);

            let mut iter = list.lending_iter();
            iter.seek_to_first();

            let data = iter.current().unwrap();
            assert_eq!(data, &[0]);

            // Asserting that the data remains valid even though the list or iter has been moved
            // requires `unsafe`.
            let data: *const [u8] = data;
            // SAFETY: `data` is a non-null, properly-aligned, dereferenceable pointer to a valid
            // value of type `&'source [u8]` that satisfies aliasing for at least `'source`.
            // We're asserting that it lasts even longer. Since this came from
            // `anchored_skiplist::simple::LendingIter::current`
            // or `anchored_skiplist::concurrent::LendingIter::current`,
            // the safety guarantees made by `$lending_iter` apply, and since the backing iterator
            // (and the skiplist inside) is not invalidated until the end of this function (aside
            // from being moved), extending the lifetime to the end of this function is sound.
            let data: &[u8] = unsafe { &*data };
            let iter = iter;

            let same_data = iter.current().unwrap();

            assert_eq!(data, &[0]);
            assert_eq!(same_data, &[0]);
            assert_eq!(data.as_ptr(), same_data.as_ptr());
        }

        // ================================
        //  Two-element List
        // ================================

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn two_element_list() {
            let mut list = $skiplist::new_seeded(OrdComparator, 5);

            // Should be the same as `list.insert_copy(&[1]);`.
            list.insert_with(1, |data| data[0] = 1);
            // Inserting a duplicate element; should return `false` and discard it.
            assert!(!list.insert_copy(&[1]));

            // The `write_locked` and `write_unlocked` don't particularly matter on a single thread,
            // but might as well use them.
            let mut locked = list.write_locked();
            // Inserting a distinct element; should return `true`.
            assert!(locked.insert_copy(&[2, 2]));

            let list = $skiplist::write_unlocked(locked);

            assert!(list.contains(&[1]));
            let list = list;
            assert!(list.contains(&[2, 2]));

            let _check_that_debug_works = format!("{list:?}");
        }

        #[cfg(any(not(miri), tests_with_leaks))]
        #[test]
        fn two_element_list_iter_leaky() {
            let mut list = $skiplist::new_seeded(OrdComparator, 5);

            let one: &[u8] = &[1];
            let two: &[u8] = &[2, 2];

            assert!(list.insert_copy(one));
            // Inserting a duplicate element; should return `false` and discard it.
            assert!(!list.insert_copy(&[1]));
            assert!(list.insert_copy(two));

            let list = Box::leak(Box::new(list));

            let mut iter: $iter<'static, OrdComparator> = list.iter();

            assert!(!iter.valid());
            iter.seek(&[]);
            // Don't need to call `current` after `seek`, immediately skipping to the following
            // element should work.
            assert_eq!(iter.next(), Some(two));

            let static_ref: &'static [u8] = iter.current().unwrap();

            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), Some(one));
            assert_eq!(iter.prev(), None);
            assert_eq!(iter.prev(), Some(two));

            iter.seek_to_last();
            assert!(iter.valid());
            assert_eq!(iter.current(), Some(two));

            let iter_clone = iter.clone();

            iter.seek_to_first();
            assert!(iter.valid());
            assert_eq!(iter.current(), Some(one));

            // The clone is independent, the cursor is not reference-counted.
            assert_eq!(iter_clone.current(), Some(two));
            assert_eq!(iter.current(), Some(one));

            // Read the static ref, just so Miri can doubly-confirm
            // that the reference hasn't been invalidated
            assert_eq!(static_ref[0], 2);

            iter.seek(&[1]);
            assert_eq!(iter.current(), Some(one));
            // `&[1]` is smaller than `&[1, 2]`, so this should seek to `&[2, 2]`
            iter.seek(&[1, 2]);
            assert_eq!(iter.current(), Some(two));

            iter.seek(&[3]);
            assert!(!iter.valid());

            let _check_that_debug_works = format!("{iter:?}");
        }

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn two_element_list_iter() {
            let mut list = $skiplist::new_seeded(OrdComparator, 5);

            let one: &[u8] = &[1];
            let two: &[u8] = &[2, 2];

            assert!(list.insert_copy(one));
            // Inserting a duplicate element; should return `false` and discard it.
            assert!(!list.insert_copy(&[1]));
            assert!(list.insert_copy(two));

            let list = Box::new(list);

            let mut iter: $iter<'_, OrdComparator> = list.iter();

            assert!(!iter.valid());
            iter.seek(&[]);
            // Don't need to call `current` after `seek`, immediately skipping to the following
            // element should work.
            assert_eq!(iter.next(), Some(two));

            let longish_ref: &[u8] = iter.current().unwrap();

            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), Some(one));
            assert_eq!(iter.prev(), None);
            assert_eq!(iter.prev(), Some(two));

            iter.seek_to_last();
            assert!(iter.valid());
            assert_eq!(iter.current(), Some(two));

            let iter_clone = Clone::clone(&iter);

            iter.seek_to_first();
            assert!(iter.valid());
            assert_eq!(iter.current(), Some(one));

            // The clone is independent, the cursor is not reference-counted.
            assert_eq!(iter_clone.current(), Some(two));
            assert_eq!(iter.current(), Some(one));

            // Read the reference, just so Miri can doubly-confirm that it's still valid.
            assert_eq!(longish_ref[0], 2);

            iter.seek(&[1]);
            assert_eq!(iter.current(), Some(one));
            // `&[1]` is smaller than `&[1, 2]`, so this should seek to `&[2, 2]`
            iter.seek(&[1, 2]);
            assert_eq!(iter.current(), Some(two));

            iter.seek(&[3]);
            assert!(!iter.valid());

            let _check_that_debug_works = format!("{iter:?}");
        }

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn two_element_list_lending_iter() {
            let mut list = $skiplist::new_seeded(OrdComparator, 5);

            let one: &[u8] = &[1];
            let two: &[u8] = &[2, 2];

            list.insert_copy(one);
            list.insert_copy(two);

            let mut iter: $lending_iter<OrdComparator> = list.lending_iter();

            assert!(!iter.valid());
            iter.seek(&[]);
            // Don't need to call `current` after `seek`, immediately skipping to the following
            // element should work.
            assert_eq!(iter.next(), Some(two));

            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), Some(one));
            assert_eq!(iter.prev(), None);
            assert_eq!(iter.prev(), Some(two));

            iter.seek_to_last();
            assert!(iter.valid());
            assert_eq!(iter.current(), Some(two));

            let mut iter = $skiplist::from_lending_iter(iter).lending_iter();
            assert!(!iter.valid());

            iter.seek_to_first();
            assert!(iter.valid());
            assert_eq!(iter.current(), Some(one));

            iter.seek(&[1]);
            assert_eq!(iter.current(), Some(one));
            // `&[1]` is smaller than `&[1, 2]`, so this should seek to `&[2, 2]`
            iter.seek(&[1, 2]);
            assert_eq!(iter.current(), Some(two));

            iter.seek(&[3]);
            assert!(!iter.valid());

            let _check_that_debug_works = format!("{iter:?}");
        }

        // ===============================
        //  Independent clones
        // ===============================
        #[cfg(not(tests_with_leaks))]
        #[test]
        fn independent_list_clone() {
            let mut list = $skiplist::new_seeded(OrdComparator, 42);
            list.insert_copy(&[1]);

            let mut other_list = list.deep_clone();

            assert!(list.iter().eq([[1_u8].as_slice()].into_iter()));
            assert!(list.iter().eq(other_list.iter()));

            list.insert_copy(&[2]);
            other_list.insert_copy(&[3]);

            assert!(list.iter().eq([[1_u8].as_slice(), [2_u8].as_slice()].into_iter()));
            assert!(other_list.iter().eq([[1_u8].as_slice(), [3_u8].as_slice()].into_iter()));
        }

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn independent_iter_clone() {
            let mut list = $skiplist::new_seeded(OrdComparator, 42);
            list.insert_copy(&[1]);

            let lending_iter = list.lending_iter();
            let mut old_iter = lending_iter.deep_clone();
            let mut list = $skiplist::from_lending_iter(lending_iter);

            list.insert_copy(&[2]);

            assert!(list.iter().eq([[1_u8].as_slice(), [2_u8].as_slice()].into_iter()));
            assert_eq!(old_iter.next(), Some([1_u8].as_slice()));
            assert_eq!(old_iter.next(), None);

            let mut new_iter = old_iter.deep_clone();
            assert_eq!(new_iter.next(), Some([1_u8].as_slice()));
            assert_eq!(old_iter.next(), Some([1_u8].as_slice()));
        }

        // ===============================
        //  Misc
        // ===============================
        #[cfg(not(tests_with_leaks))]
        #[test]
        fn wrapped_comparators() {
            let mut list: $skiplist<GenericContainer<OrdComparator, Box<OrdComparator>>>
                = $skiplist::new(GenericContainer::new(Box::new(OrdComparator)));

            list.insert_copy(&[2]);
            list.insert_copy(&[3]);
            list.insert_copy(&[1]);

            let correct: [&[u8]; 3] = [&[1_u8], &[2], &[3]];

            assert!(list.iter().eq(correct.into_iter()));

            let mut list: $skiplist<Rc<dyn Comparator<[u8]>>>
                = $skiplist::new(Rc::new(OrdComparator));

            list.insert_copy(&[2]);
            list.insert_copy(&[3]);
            list.insert_copy(&[1]);

            let correct: [&[u8]; 3] = [&[1_u8], &[2], &[3]];

            assert!(list.iter().eq(correct.into_iter()));
        }

        // This test helps to ensure that a broken comparator doesn't cause memory unsafety
        // or otherwise unexpected results, like panics.
        // Logic errors are fine, and users of this crate shouldn't rely on panics to *not* happen,
        // but as the implementor I'm not aware of a reason this should panic.
        #[cfg(not(tests_with_leaks))]
        #[cfg_attr(miri, ignore)]
        #[test]
        fn broken_comparators() {
            // You know where this is going from the type signature.....
            #[derive(Debug)]
            struct BadComparator(RefCell<Rand32>);

            impl BadComparator {
                fn new() -> Self {
                    Self(RefCell::new(Rand32::new(666)))
                }
            }

            impl Comparator<[u8]> for BadComparator {
                fn cmp(&self, _lhs: &[u8], _rhs: &[u8]) -> Ordering {
                    match self.0.borrow_mut().rand_range(0..3) {
                        0 => Ordering::Less,
                        1 => Ordering::Equal,
                        _ => Ordering::Greater,
                    }
                }
            }

            let mut list: $skiplist<BadComparator> = $skiplist::new(BadComparator::new());

            for i in 0..1024_u32 {
                list.insert_copy(i.to_le_bytes().as_slice());
            }

            let num_entries = list.iter().fold(0, |acc, _ele| acc + 1);
            // This should always be true? Now, if seeks were involved, anything could
            // happen, but since just going to the next element via a skip is deterministic, we
            // can't accidentally go backwards.
            assert!(num_entries <= 1000);

            let mut lending_iter = list.lending_iter();
            let mut iteration_cap = 2000;

            // Now *this* could do almost anything. Therefore, I'm capping it.
            while let Some(_entry) = lending_iter.prev() {
                if iteration_cap == 0 {
                    break
                } else {
                    iteration_cap -= 1;
                }
            }
        }

        // ================================
        //  Large List
        // ================================

        // Is this ugly? Yes. But it's reasonably thorough.
        #[cfg(not(tests_with_leaks))]
        #[cfg_attr(miri, ignore)]
        #[test]
        fn many_insertions_and_reads() {
            // We will randomly choose 2048 (possibly-repeated) entries, out of 4096 possible
            // values. Those possible entries are slices of length 4, each of whose bytes are
            // strictly less than 8. (And 8^4 == 4096.)
            // Duplicates end up being discard, so fewer than 2048 end up in the skiplist.
            #[cfg(not(miri))]
            let num_insertions: usize = 2048_usize;
            #[cfg(not(miri))]
            let bits_per_entry_byte: u32 = 3;

            // Below, when checking on every possible entry value, we seek around,
            // and then check 4 of the following entries just to confirm the seek worked fine.
            #[cfg(not(miri))]
            let seek_check_len: usize = 4;

            // When miri is run.... don't do the above. It's too much for the poor thing.
            // Instead, we choose 64 of 256 possible values, which still takes some time.
            #[cfg(miri)]
            let num_insertions: usize = 64_usize;
            #[cfg(miri)]
            let bits_per_entry_byte: u32 = 2;
            #[cfg(miri)]
            let seek_check_len: usize = 2;

            let total_possible_entries: usize = 2_usize.pow(bits_per_entry_byte).pow(4);


            let mut prng = Rand32::new(0x_12345678);
            let mut get_random_entry = || -> [u8; 4] {
                prng.rand_u32()
                    .to_le_bytes()
                    .map(|byte| byte % (1 << bits_per_entry_byte))
            };

            let mut skiplist = $skiplist::new(OrdComparator);
            // `sorted_entries` will have the correct order of entries, because `OrdComparator`
            // uses `Ord`. Note that the `skiplist` and `BTreeMap` handle duplicate entries
            // identically... aside from `skiplist` not deallocating wasted memory immediately.
            let mut sorted_entries: BTreeSet<[u8; 4]> = BTreeSet::new();

            for _ in 0..num_insertions {
                let entry: [u8; 4] = get_random_entry();

                // `true` means it's a unique entry, `false` means it was previously added.
                assert_eq!(skiplist.insert_copy(entry.as_slice()), sorted_entries.insert(entry));
            }

            // Check every possibly entry value.
            for entry_num in 0..total_possible_entries {
                let entry_arr: [u8; 4] = array::from_fn(|idx| {
                        entry_num >> ((idx as u32) * bits_per_entry_byte)
                    })
                    .map(|num| (num % (1 << bits_per_entry_byte)) as u8);

                // The entry should be in the skiplist iff it's in the `BTreeSet`
                assert_eq!(
                    skiplist.contains(entry_arr.as_slice()),
                    sorted_entries.contains(&entry_arr),
                );


                // Next, check `$iter`
                let mut std_iter = sorted_entries
                    .iter()
                    .skip_while(|entry| **entry < entry_arr)
                    .peekable();
                let std_iter_clone = std_iter.clone();

                let mut iter = skiplist.iter();
                iter.seek(&entry_arr);

                // Make sure that `iter` is at the end iff `std_iter` is at the end.
                assert_eq!(iter.valid(), std_iter.peek().is_some());

                // Move `iter` back one element, since we want `next()` to return what `current()`
                // currently does, to match with `std_iter`.
                iter.prev();

                let iter_seek_check = iter.fuse().take(seek_check_len);
                let std_seek_check = std_iter.take(seek_check_len);
                assert!(iter_seek_check.eq(std_seek_check));


                // Next, check `$lending_iter`
                let mut std_iter = std_iter_clone;

                let mut lending_iter = skiplist.lending_iter();
                lending_iter.seek(&entry_arr);

                // Make sure that `lending_iter` is at the end iff `std_iter` is at the end.
                assert_eq!(lending_iter.valid(), std_iter.peek().is_some());

                // Move `iter` back one element, since we want `next()` to return what `current()`
                // currently does, to match with `std_iter`.
                lending_iter.prev();

                for _ in 0..seek_check_len {
                    if let Some(entry) = lending_iter.next() {
                        assert_eq!(Some(entry), std_iter.next().map(|arr| arr.as_slice()));
                    } else {
                        assert_eq!(None, std_iter.next());
                        break;
                    }
                }

                skiplist = $skiplist::from_lending_iter(lending_iter);
            }


            // Iterate forwards through the whole list.
            assert!(skiplist.iter().eq(sorted_entries.iter()));

            let mut lending_iter = skiplist.lending_iter();

            sorted_entries.iter().for_each(|entry| {
                assert_eq!(lending_iter.next(), Some(entry.as_slice()));
            });
            // Note that `next` was called one more time on `std_iter` than on `lending_iter`.
            lending_iter.next();
            assert!(!lending_iter.valid());

            let skiplist = $skiplist::from_lending_iter(lending_iter);

            // Iterate backwards through the whole list.
            let std_iter = sorted_entries.iter().rev();
            let mut iter = skiplist.iter();

            std_iter.clone().for_each(|entry| {
                assert_eq!(iter.prev(), Some(entry.as_slice()));
            });
            // Note that `next` was called one more time on `std_iter`
            // than `prev` was on `lending_iter`.
            iter.prev();
            assert!(!iter.valid());

            let mut lending_iter = skiplist.lending_iter();

            std_iter.for_each(|entry| {
                assert_eq!(lending_iter.prev(), Some(entry.as_slice()));
            });
            // Note that `next` was called one more time on `std_iter`
            // than `prev` was on `lending_iter`.
            lending_iter.prev();
            assert!(!lending_iter.valid());
        }

    };
}

pub(crate) use tests_for_all_skiplists as tests_for_all_skiplists;
