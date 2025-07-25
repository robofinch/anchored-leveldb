macro_rules! tests_for_refcounted_skiplists {
    ($skiplist:ident) => {
        #[cfg(not(tests_with_leaks))]
        #[test]
        fn basic_clone_functionality() {
            let mut list = $skiplist::new(DefaultComparator);
            let mut other_handle = list.clone();

            // Since there's only a single thread, unlike in multithreaded tests we know for sure
            // that the changes should be seen by all handles immediately.
            list.insert_copy(&[1]);
            assert_eq!(other_handle.iter().next(), Some([1].as_slice()));

            other_handle.insert_copy(&[0]);
            let mut lending_iter = list.lending_iter();
            assert_eq!(lending_iter.next(), Some([0].as_slice()));

            other_handle.insert_copy(&[0, 1]);

            // Despite not getting a new lending iterator, it should observe the results.
            assert_eq!(lending_iter.next(), Some([0, 1].as_slice()));

            let mut lending_iter_clone = lending_iter.clone();

            assert_eq!(lending_iter.next(), Some([1].as_slice()));
            assert!(lending_iter.next().is_none());
            assert!(!lending_iter.is_valid());

            let mut iter = other_handle.iter();
            let mut list = $skiplist::from_lending_iter(lending_iter);

            assert_eq!(iter.next(), Some([0].as_slice()));
            assert_eq!(iter.next(), Some([0, 1].as_slice()));

            let mut iter_clone = iter.clone();

            // An `Iter` should be able to immediately observe the results, too.
            list.insert_copy(&[0, 2]);
            assert_eq!(iter.next(), Some([0, 2].as_slice()));

            // The iterators' cursors should not be reference counted. Though their list should be.
            assert_eq!(iter_clone.next(), Some([0, 2].as_slice()));

            assert_eq!(iter.next(), Some([1].as_slice()));
            assert!(iter.next().is_none());
            assert!(!iter.is_valid());

            // That lending iterator's clone was at the `[0, 1]` element.
            assert_eq!(lending_iter_clone.next(), Some([0, 2].as_slice()));
            assert_eq!(lending_iter_clone.next(), Some([1].as_slice()));
            assert!(lending_iter_clone.next().is_none());
            assert!(!lending_iter_clone.is_valid());
        }

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn suspicious_init_entry() {
            #[derive(Debug, Clone, Copy)]
            struct TrivialComparator;

            impl Comparator for TrivialComparator {
                fn cmp(&self, _lhs: &[u8], _rhs: &[u8]) -> Ordering {
                    Ordering::Equal
                }
            }

            let mut list = $skiplist::new(TrivialComparator);
            let mut other_handle = list.clone();

            // The inner insert should succeed, and the outer insert should fail.
            assert!(!list.insert_with(1, |data| {
                data[0] = 1;
                assert!(other_handle.insert_copy(&[2]));
            }));

            assert!(list.iter().eq([[2].as_slice()].into_iter()));

            // In the next case, both should succeed.
            let mut list = $skiplist::new(DefaultComparator);
            let mut other_handle = list.clone();

            assert!(list.insert_with(1, |data| {
                data[0] = 1;
                assert!(other_handle.insert_copy(&[2]));
            }));

            assert!(list.iter().eq([[1].as_slice(), [2].as_slice()].into_iter()))
        }

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn lifetime_extension() {
            #![expect(unsafe_code, reason = "Confirm claims of safety of lifetime extension")]

            let mut list: $skiplist<DefaultComparator> = $skiplist::default();

            list.insert_copy(&[1, 2, 3]);

            let data = list.iter().next().unwrap();
            assert_eq!(data, [1, 2, 3].as_slice());

            let data: *const [u8] = data;
            // SAFETY: `data` is a non-null, properly-aligned, dereferenceable pointer to a valid
            // value of type `&'source [u8]` that satisfies aliasing for at least `'source`.
            // We're asserting that it lasts even longer. Since this came from
            // `anchored_skiplist::concurrent::Iter::next`,
            // the safety guarantees made by `Iter` apply, and since the at least one
            // reference-counted clone (either `list` or `other_handle`) remains valid (aside from
            // being moved) until the end of this function, extending the lifetime to the end of
            // this function is sound.
            let data: &[u8] = unsafe { &*data };

            let mut other_handle = list.clone();

            // Inserting this duplicate should fail, without harming the previous data.
            assert!(!other_handle.insert_copy(&[1, 2, 3]));

            let other_data_handle = other_handle.iter().next().unwrap();

            drop(list);

            assert_eq!(data, [1, 2, 3].as_slice());
            assert_eq!(other_data_handle, [1, 2, 3].as_slice());
            assert_eq!(data.as_ptr(), other_data_handle.as_ptr());
        }

        #[cfg(any(not(miri), tests_with_leaks))]
        #[test]
        fn static_lifetime_extension_leaky() {
            #![expect(unsafe_code, reason = "Confirm claims of safety of lifetime extension")]

            let mut list: $skiplist<DefaultComparator> = $skiplist::default();

            list.insert_copy(&[1, 2, 3]);

            let data = list.iter().next().unwrap();
            assert_eq!(data, [1, 2, 3].as_slice());

            let data: *const [u8] = data;
            // SAFETY: `data` is a non-null, properly-aligned, dereferenceable pointer to a valid
            // value of type `&'source [u8]` that satisfies aliasing for at least `'source`.
            // We're asserting that it lasts even longer. Since this came from
            // `anchored_skiplist::concurrent::Iter::next`,
            // the safety guarantees made by `Iter` apply, and since the at least one
            // reference-counted clone (either `list` or `other_handle` or
            // `other_handle.lending_iter()`) remains valid for `'static`, extending the lifetime
            // to the end of this function is sound.
            let data: &'static [u8] = unsafe { &*data };

            let mut other_handle = list.clone();

            // Inserting this duplicate should fail, without harming the previous data.
            assert!(!other_handle.insert_copy(&[1, 2, 3]));

            let lending_iter: &'static mut LendingIter<DefaultComparator> = Box::leak(Box::new(
                other_handle.lending_iter(),
            ));

            let other_data_handle: &'static [u8] = lending_iter.next().unwrap();

            drop(list);

            assert_eq!(data, [1, 2, 3].as_slice());
            assert_eq!(other_data_handle, [1, 2, 3].as_slice());
            assert_eq!(data.as_ptr(), other_data_handle.as_ptr());
        }
    };
}

pub(crate) use tests_for_refcounted_skiplists;
