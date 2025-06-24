#![expect(unreachable_pub, reason = "I know everything is private, no need for pub(crate)")]

//! Results are cached per-package and by whether or not `--on-save` was used.
//!
//! Arguments are additive; for instance, `--command` arguments and `--all-comands` add together.
//!
//! Parameters to command-line arguments:
//!
//! - Possible commands:
//!   `check`, `clippy`. Note that `clippy` runs a superset of the checks that `check` does.
//! - Possible channels: `stable`, `nightly`. (`beta` is not supported.)
//! - Possible targets:
//!   `native` (the platform the compiler is run on),
//!   `apple` or `apple-silicon`,
//!   `linux`,
//!   `windows`,
//!   `wasm` or `wasm32`,
//!   or a full target triple.
//! - Possible packages:
//!   `anchored-leveldb`, `anchored-skiplist`, `anchored-vfs`, `generic-container`.
//!   The `anchored-` and `generic-` prefixes are optional.
//!
//! Command-line arguments:
//!
//! - `--command {command}`: A command to run. (See above.)
//! - `--channel {channel}`: A channel to perform commands on. (See above.)
//! - `--target {target}`: A target to perform commands on. (See above.)
//! - `--package {package}`: A package which commands will be performed on. (See above.)
//!
//! - `--all-commands`: Run every command.
//! - `--all-channels`: Run each command on every channel.
//! - `--all-targets`: Run each command on every target.
//! - `--all-packages`: Run each command on eavery package.
//!
//! - `--all`:
//!   Run every command on every channel, target, and package.
//! - `--on-save`:
//!   Run commands with `--message-format=json` and limit `--feature-powerset` to a depth
//!   of 1 (making it equivalent to `--each-feature`), for use as an on-save check.
//! - `--no-cache`:
//!   Ignore previously cached outputs.
//! - `-- {trailing-arg}*`:
//!   Pass any following arguments to the inner command
//!   (`cargo hack check` or `cargo hack clippy`).

mod data;
mod commands;
mod package_cache;
mod parsing;


use anyhow::Context as _;

use crate::parsing::ParsedArgs;
use crate::package_cache::{packages_to_check, print_cached_checks};


fn main() -> anyhow::Result<()> {
    let args = ParsedArgs::try_parse()
        .context("error while parsing args to ldb-cargo-utils")?;

    let to_check = packages_to_check(
        &args.packages,
        args.on_save,
        args.no_cache,
    );

    // Check those
    for command in args.commands {
        command.run(
            &args.channels,
            &args.targets,
            &to_check,
            args.on_save,
            &args.trailing_args,
        );
    }

    // Print to stdour or stderr
    print_cached_checks(
        &args.packages,
        &to_check,
        args.on_save,
        args.no_cache,
    );

    Ok(())
}
