# Anchored LevelDB

Reimplementation of Google's LevelDB (originally in C++) written in Rust, with effort to provide
a good API for convenient and performant usage.

Note that there are also [Rust bindings for LevelDB](https://crates.io/crates/leveldb)
and a previous Rust implementation, [rusty-leveldb](https://crates.io/crates/rusty-leveldb) (aka leveldb-rs).

## Motivation

While working on [Prismarine Anchor](https://github.com/robofinch/Prismarine-Anchor), I found that
rusty-leveldb didn't provide all the features I'd have liked, such as lending iterators over a
database's entries or keys.

I decided it would be easier to start from scratch rather than making all-encompassing pull
requests to rusty-leveldb, free from backwards compatibility.

## Testing and Build Dependencies

Currently, there are no strictly necesary dependencies that aren't part of normal Rust toolchains.
However, to use the `Justfile`, both `just` and `cargo-hack` are necessary.

### Testing / Linting

Before pushing a commit, run `just` or `just do-it-all`, which runs checks on supported combinations
of architectures.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
 * MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without
any additional terms or conditions.
