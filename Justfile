# TODO: (test-all) (build-all) (bench-all)

list:
    just --list

# ================================================================
#   Example `.vscode/settings.json` for `rust-analyzer`:
# ================================================================

# {
#     "rust-analyzer.check.overrideCommand": [
#         "just",
#         "on-save",
#     ],
#     "rust-analyzer.checkOnSave": true,
# }

# ================================================================
#   Smaller scripts
# ================================================================

# Run ripgrep, but don't return an error if nothing matched.
[group("ripgrep")]
rg-maybe-no-match *args:
    rg {{ args }} || [ $$? -eq 1 ]

# Find lines not ending in a comma, where the next line starts with `]`, `)`, or `>`.
[group("ripgrep")]
find-possible-missing-commas: \
    (rg-maybe-no-match '-U' '[^,]\n[ ]*\]') \
    (rg-maybe-no-match '-U' '[^,]\n[ ]*\)') \
    (rg-maybe-no-match '-U' '[^,]\n[ ]*\>')

# Find any `#[allow(...)]` attribute, or to be precise, find `[allow(`.
[group("ripgrep")]
find-allow-attributes: (rg-maybe-no-match '\[allow\(')

# Find any possible sites of unsafe code.
[group("ripgrep")]
find-unsafe-code: (rg-maybe-no-match 'unsafe_code|unsafe')

# ================================================================
#   Check util
# ================================================================

check-dir := justfile_directory() + "/check"
check-executable := "anchored-ldb-check"

[doc("""
    Run the util script in the `check` directory with the provided args as its command-line
    arguments.

    Results are cached per-package and by whether or not `--on-save` was used.

    Arguments are additive; for instance, `--command` arguments and `--all-comands` add together.
    If none are specified for a certain category, defaults are used for it.

    Parameters to command-line arguments:

    - Possible commands:
        `check`, `clippy`.
        Note that `clippy` runs a superset of the checks that `check` does.
    - Possible channels: `stable`, `nightly`. (`beta` is not supported.)
    - Possible targets:
        `native` (the platform the compiler is run on),
        `apple` or `apple-silicon`,
        `linux`,
        `windows`,
        `wasm` or `wasm32`,
        or a full target triple.
    - Possible packages:
       `anchored-leveldb`, `anchored-skiplist`, `anchored-vfs`, `generic-container`.
       The `anchored-` and `generic-` prefixes are optional.

    Command-line arguments:

    - `--command {command}`: A command to run. (See above.)
    - `--channel {channel}`: A channel to perform commands on. (See above.)
    - `--target {target}`: A target to perform commands on. (See above.)
    - `--package {package}`: A package which commands will be performed on. (See above.)

    - `--all-commands`: Run every command.
    - `--all-channels`: Run each command on every channel.
    - `--all-targets`: Run each command on every target.
    - `--all-packages`: Run each command on eavery package.

    - `--all`: Run every command on every channel, target, and package.
    - `--on-save`:
           Run commands with `--message-format=json` and limit `--feature-powerset` to a depth
           of 1 (making it equivalent to `--each-feature`), for use as an on-save check.
    - `--no-cache`: Ignore previously cached outputs.
    - `-- {trailing-arg}*`:
           Pass any following arguments to the inner command
           (which is `cargo hack check` or `cargo hack clippy`).
""")]
check-util *args:
    #!/usr/bin/env bash
    set -euxo pipefail
    cd {{check-dir}}
    cargo build --release
    cd {{justfile_directory()}}
    {{check-dir}}/target/release/{{check-executable}} {{args}}

# ================================================================
#   Shorthands for using that util
# ================================================================

all-channels := 'stable nightly'
default-targets  := 'native wasm'

[group("on-save")]
on-save: (check-util "--on-save")

# Check-all

[group("check")]
check-all *extra-args: \
    (check-util "--command check" "--all-channels" "--all-targets" "--all-packages" extra-args)

[group("check-package")]
check-leveldb-all *extra-args: \
    (check-util "--command check" "--all-channels" "--all-targets" "--package leveldb" extra-args)

[group("check-package")]
check-skiplist-all *extra-args: \
    (check-util "--command check" "--all-channels" "--all-targets" "--package skiplist" extra-args)

[group("check-package")]
check-vfs-all *extra-args: \
    (check-util "--command check" "--all-channels" "--all-targets" "--package vfs" extra-args)

[group("check-package")]
check-container-all *extra-args: \
    (check-util "--command check" "--all-channels" "--all-targets" "--package container" extra-args)

# Check

[group("check")]
check channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command check" prepend("--channel ", channels) \
     prepend("--target ", targets) "--all-packages" extra-args)

[group("check-package")]
check-leveldb channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command check" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package leveldb" extra-args)

[group("check-package")]
check-skiplist channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command check" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package skiplist" extra-args)

[group("check-package")]
check-vfs channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command check" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package vfs" extra-args)

[group("check-package")]
check-container channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command check" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package container" extra-args)

# Clippy-all

# Note that `cargo clippy` performs a superset of the checks done by `cargo check`
[group("clippy")]
clippy-all *extra-args: \
    (check-util "--command clippy" "--all-channels" "--all-targets" "--all-packages" extra-args)

[group("clippy-package")]
clippy-leveldb-all *extra-args: \
    (check-util "--command clippy" "--all-channels" "--all-targets" "--package leveldb" extra-args)

[group("clippy-package")]
clippy-skiplist-all *extra-args: \
    (check-util "--command clippy" "--all-channels" "--all-targets" "--package skiplist" extra-args)

[group("clippy-package")]
clippy-vfs-all *extra-args: \
    (check-util "--command clippy" "--all-channels" "--all-targets" "--package vfs" extra-args)

[group("clippy-package")]
clippy-container-all *extra-args: \
    (check-util "--command clippy" "--all-channels" "--all-targets" "--package container" extra-args)

# Clippy

# Note that `cargo clippy` performs a superset of the checks done by `cargo check`
[group("clippy")]
clippy channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command clippy" prepend("--channel ", channels) \
     prepend("--target ", targets) "--all-packages" extra-args)

[group("clippy-package")]
clippy-leveldb channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command clippy" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package leveldb" extra-args)

[group("clippy-package")]
clippy-skiplist channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command clippy" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package skiplist" extra-args)

[group("clippy-package")]
clippy-vfs channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command clippy" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package vfs" extra-args)

[group("clippy-package")]
clippy-container channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command clippy" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package container" extra-args)
