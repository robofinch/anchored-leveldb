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

Currently, there are no strictly necessary dependencies that aren't part of normal Rust toolchains.
However, to use the `Justfile`, both `just` and `cargo-hack` are necessary.
Additionally, some commands require `miri` and `cargo-llvm-cov`.

When `zstd-compressor` features are enabled, bindings to `zstd` are used, which may make
cross-compiling difficult. Compiling for Linux on Macos involves getting some form of
`x86_64-unknown-linux-gnu-gcc` compiler and selecting it with
`export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-gcc`.

### Testing / Linting

Before pushing a commit, run `just clippy-all --no-cache` and `just test-all --no-cache`, which run
checks on supported combinations of features and several architectures.

Initially, `just add-targets` may need to be run.

Occasionally, `just find-possible-missing-commas` should be run and looked through. `just miri-test`
should occasionally be run, especially when modifying `anchored-skiplist`. The coverage-related
commands should likewise be run occasionally, but are not critical.
`just skiplist-loom-test` and `just multithreaded-skiplist-test` should be run when modifying
`anchored-skiplist`.

Documentation should be built with `just doc`.


## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
 * MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without
any additional terms or conditions.
