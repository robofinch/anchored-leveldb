<div align="center" class="rustdoc-hidden">
<h1> Anchored Pool </h1>
</div>

[<img alt="github" src="https://img.shields.io/badge/github-anchored--pool-08f?logo=github" height="20">](https://github.com/robofinch/anchored-leveldb/tree/main/crates/anchored-pool)
[![Latest version](https://img.shields.io/crates/v/anchored-pool.svg)](https://crates.io/crates/anchored-pool)
[![Documentation](https://img.shields.io/docsrs/anchored-pool)](https://docs.rs/anchored-pool/0)
[![Apache 2.0 or MIT license.](https://img.shields.io/badge/license-Apache--2.0_OR_MIT-blue.svg)](#license)

Provides bounded and unbounded pools for any type of resource, as well as pools specific to
`Vec<u8>` buffers.

The resource pools can have a user-chosen `init_resource` function run to create a new resource,
and whenever a resource is returned to the pool, a `reset_resource` callback is first run.

The buffer pools use these features to create new empty `Vec<u8>` buffers as resources, and
whenever a buffer is returned to the pool, the buffer is either cleared (without changing its
capacity) if its capacity is at most a user-chosen `max_buffer_capacity`, and is otherwise replaced
with a new empty `Vec<u8>`.

The unbounded pools all have a `trim_unused` function that can discard an excessive number of
unused resources or buffers. Together with the `max_buffer_capacity` setting of the buffer pools,
the amount of unused memory in a pool can be limited.

# Features

- `clone-behavior`: Implements [`clone-behavior`] traits for relevant structs.

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

[`clone-behavior`]:  https://docs.rs/clone-behavior/
