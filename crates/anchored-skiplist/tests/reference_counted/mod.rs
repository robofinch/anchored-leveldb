macro_rules! tests_for_refcounted_skiplists {
    ($skiplist:ident, $iter:ident, $lending_iter:ident $(,)?) => {
        #[cfg(not(tests_with_leaks))]
        #[test]
        fn basic_clone_functionality() {
            let mut list = $skiplist::new(OrdComparator);
            let mut other_handle = list.refcounted_clone();

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

            let mut lending_iter_clone = Clone::clone(&lending_iter);

            assert_eq!(lending_iter.next(), Some([1].as_slice()));
            assert!(lending_iter.next().is_none());
            assert!(!lending_iter.valid());

            let mut iter = other_handle.iter();
            let mut list = $skiplist::from_lending_iter(lending_iter);

            assert_eq!(iter.next(), Some([0].as_slice()));
            assert_eq!(iter.next(), Some([0, 1].as_slice()));

            let mut iter_clone = iter.clone();

            // An `$iter` should be able to immediately observe the results, too.
            list.insert_copy(&[0, 2]);
            assert_eq!(iter.next(), Some([0, 2].as_slice()));

            // The iterators' cursors should not be reference counted. Though their list should be.
            assert_eq!(iter_clone.next(), Some([0, 2].as_slice()));

            assert_eq!(iter.next(), Some([1].as_slice()));
            assert!(iter.next().is_none());
            assert!(!iter.valid());

            // That lending iterator's clone was at the `[0, 1]` element.
            assert_eq!(lending_iter_clone.next(), Some([0, 2].as_slice()));
            assert_eq!(lending_iter_clone.next(), Some([1].as_slice()));
            assert!(lending_iter_clone.next().is_none());
            assert!(!lending_iter_clone.valid());
        }

        #[cfg(not(tests_with_leaks))]
        #[test]
        fn lifetime_extension() {
            #![expect(unsafe_code, reason = "Confirm claims of safety of lifetime extension")]

            let mut list: $skiplist<OrdComparator> = $skiplist::default();

            list.insert_copy(&[1, 2, 3]);

            let data = list.iter().next().unwrap();
            assert_eq!(data, [1, 2, 3].as_slice());

            let data: *const [u8] = data;
            // SAFETY: `data` is a non-null, properly-aligned, dereferenceable pointer to a valid
            // value of type `&'source [u8]` that satisfies aliasing for at least `'source`.
            // We're asserting that it lasts even longer. Since this came from
            // `anchored_skiplist::concurrent::Iter::next`,
            // the safety guarantees made by `$iter` apply, and since the at least one
            // reference-counted clone (either `list` or `other_handle`) remains valid (aside from
            // being moved) until the end of this function, extending the lifetime to the end of
            // this function is sound.
            let data: &[u8] = unsafe { &*data };

            let mut other_handle = list.refcounted_clone();

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

            let mut list: $skiplist<OrdComparator> = $skiplist::default();

            list.insert_copy(&[1, 2, 3]);

            let data = list.iter().next().unwrap();
            assert_eq!(data, [1, 2, 3].as_slice());

            let data: *const [u8] = data;
            // SAFETY: `data` is a non-null, properly-aligned, dereferenceable pointer to a valid
            // value of type `&'source [u8]` that satisfies aliasing for at least `'source`.
            // We're asserting that it lasts even longer. Since this came from
            // `anchored_skiplist::concurrent::Iter::next`,
            // the safety guarantees made by `$iter` apply, and since the at least one
            // reference-counted clone (either `list` or `other_handle` or
            // `other_handle.lending_iter()`) remains valid for `'static`, extending the lifetime
            // to the end of this function is sound.
            let data: &'static [u8] = unsafe { &*data };

            let mut other_handle = list.refcounted_clone();

            // Inserting this duplicate should fail, without harming the previous data.
            assert!(!other_handle.insert_copy(&[1, 2, 3]));

            let lending_iter: &'static mut $lending_iter<OrdComparator> = Box::leak(Box::new(
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
