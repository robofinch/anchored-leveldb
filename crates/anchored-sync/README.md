<div align="center" class="rustdoc-hidden">
<h1> Anchored Sync </h1>
</div>

[<img alt="github" src="https://img.shields.io/badge/github-anchored--sync-08f?logo=github" height="20">](https://github.com/robofinch/anchored-leveldb/tree/main/crates/anchored-sync)
[![Latest version](https://img.shields.io/crates/v/anchored-sync.svg)](https://crates.io/crates/anchored-sync)
[![Documentation](https://img.shields.io/docsrs/anchored-sync)](https://docs.rs/anchored-sync/0)
[![Apache 2.0 or MIT license.](https://img.shields.io/badge/license-Apache--2.0_OR_MIT-blue.svg)](#license)

Abstract over atomicity and sync-ness with a `const SYNC: bool` const generic.

- `MaybeSyncArc<SYNC, T>` for `Arc<T>` or `Rc<T>`
- `MaybeSyncWeak<SYNC, T>` for `sync::Weak<T>` or `rc::Weak<T>`
- `MaybeSyncMutex<SYNC, T>` to protect data with either `Mutex<()>` or `Cell<bool>`
- `MaybeSyncRwLock<SYNC, T>` to protect data with either `RwLock<()>` or `Cell<usize>`

TODO: `MaybeSyncStrongArc<SYNC, T>` for a version of `MaybeSyncArc` without weak pointers.
TODO: add links to these types

# Features

- `std`: Enable the `MaybeSyncMutex` and `MaybeSyncRwLock` types.
- `clone-behavior`: Implements [`clone-behavior`] traits for `MaybeSyncArc` and `MaybeSyncWeak`.
- `parking_lot`: Use [`parking_lot`]'s `Mutex` and `RwLock` types for `MaybeSyncMutex` and `MaybeSyncRwLock`.


## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE][])
* MIT license ([LICENSE-MIT][])

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without
any additional terms or conditions.

[LICENSE-APACHE]: ../../LICENSE-APACHE
[LICENSE-MIT]: ../../LICENSE-MIT

[`clone-behavior`]: https://docs.rs/clone-behavior/
[`parking_lot`]: https://docs.rs/parking_lot/
