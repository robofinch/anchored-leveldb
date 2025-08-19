# TODO: (build-all) (bench-all)

list:
    just --list

# Add all the toolchain targets needed (four target architectures on three channels), and miri.
add-targets:
    rustup target add --toolchain stable aarch64-apple-darwin
    rustup target add --toolchain stable x86_64-unknown-linux-gnu
    rustup target add --toolchain stable x86_64-pc-windows-msvc
    rustup target add --toolchain stable wasm32-unknown-unknown
    rustup target add --toolchain nightly aarch64-apple-darwin
    rustup target add --toolchain nightly x86_64-unknown-linux-gnu
    rustup target add --toolchain nightly x86_64-pc-windows-msvc
    rustup target add --toolchain nightly wasm32-unknown-unknown
    rustup target add --toolchain 1.85 aarch64-apple-darwin
    rustup target add --toolchain 1.85 x86_64-unknown-linux-gnu
    rustup target add --toolchain 1.85 x86_64-pc-windows-msvc
    rustup target add --toolchain 1.85 wasm32-unknown-unknown
    rustup +nightly component add miri

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
    @rg {{ args }} || [ $? -eq 1 ]

# Find lines not ending in a comma, where the next line starts with `]`, `)`, or `>`.
[group("ripgrep")]
find-possible-missing-commas: \
    (rg-maybe-no-match ''' -U '[^,]\n[ ]*\]' ''') \
    (rg-maybe-no-match ''' -U '[^,]\n[ ]*\)' ''') \
    (rg-maybe-no-match ''' -U '[^,]\n[ ]*>' ''')

# Find any `#[allow(...)]` attribute, or to be precise, find `[allow(`.
[group("ripgrep")]
find-allow-attributes: (rg-maybe-no-match '"\[allow\("')

# Find any possible sites of unsafe code.
[group("ripgrep")]
find-unsafe-code: (rg-maybe-no-match '"unsafe_code|unsafe"')

# ================================================================
#   Miscellaneous tests
# ================================================================

[group("extra-tests")]
miri-test *extra-args:
    MIRIFLAGS="-Zmiri-many-seeds -Zmiri-strict-provenance -Zmiri-recursive-validation" \
    cargo +nightly miri test --target x86_64-unknown-linux-gnu {{extra-args}}
    MIRIFLAGS="-Zmiri-many-seeds -Zmiri-strict-provenance -Zmiri-recursive-validation -Zmiri-ignore-leaks" \
    RUSTFLAGS="--cfg tests_with_leaks" \
    cargo +nightly miri test --target x86_64-unknown-linux-gnu {{extra-args}}
    MIRIFLAGS="-Zmiri-many-seeds=0..4 -Zmiri-strict-provenance -Zmiri-recursive-validation" \
    cargo +nightly miri test --target x86_64-unknown-linux-gnu {{extra-args}} -- --ignored

[group("extra-tests")]
skiplist-loom-test:
    RUSTFLAGS="--cfg skiplist_loom" \
    cargo test --test multithreaded_test --release
    RUSTFLAGS="--cfg skiplist_loom --cfg skiplist_loom_hard" \
    cargo test --test multithreaded_test --release

[group("extra-tests")]
multithreaded-skiplist-test:
    cargo test --test multithreaded_test --release -- --nocapture --ignored

[group("coverage")]
generate-coverage-info *extra-args:
    cargo +stable llvm-cov --lcov --output-path coverage/lcov.info {{extra-args}}

[group("coverage")]
coverage-all *extra-args:
    cargo +stable llvm-cov {{extra-args}}

[group("coverage")]
coverage-leveldb *extra-args:
    cargo +stable llvm-cov --package anchored-leveldb {{extra-args}}

[group("coverage")]
coverage-skiplist *extra-args:
    cargo +stable llvm-cov --package anchored-skiplist {{extra-args}}

[group("coverage")]
coverage-vfs *extra-args:
    cargo +stable llvm-cov --package anchored-vfs {{extra-args}}

# ================================================================
#   Check util
# ================================================================

check-dir := justfile_directory() + "/check"
check-executable := "anchored-ldb-check"

[doc("""
    Run the util script in the `check` directory with the provided args as its command-line
    arguments.

    Results are cached per-package and by whether or not `--on-save` was used.

    Arguments are additive; for instance, `--command` arguments and `--all-commands` add together.
    If none are specified for a certain category, defaults are used for it.

    Parameters to command-line arguments:

    - Possible commands:
        `check`, `clippy`, `test`.
        Note that `clippy` runs a superset of the checks that `check` does.
    - Possible channels:
        `stable`, `nightly`, `msrv`. (`beta` is not supported.)
        `msrv` refers to the `stable` channel of the minimum-supported Rust version's compiler.
    - Possible targets:
        `native` (the platform the compiler is run on),
        `apple` or `apple-silicon`,
        `linux`,
        `windows`,
        `wasm` or `wasm32`,
        or a full target triple.
    - Possible packages:
        `anchored-leveldb`, `anchored-skiplist`, `anchored-sstable`, `anchored-vfs`.
        The `anchored-` prefix is optional.

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
    cargo +stable build --release
    cd {{justfile_directory()}}
    {{check-dir}}/target/release/{{check-executable}} {{args}}

# ================================================================
#   Shorthands for using that util
# ================================================================

all-channels := 'stable nightly msrv'
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
check-sstable-all *extra-args: \
    (check-util "--command check" "--all-channels" "--all-targets" "--package sstable" extra-args)

[group("check-package")]
check-vfs-all *extra-args: \
    (check-util "--command check" "--all-channels" "--all-targets" "--package vfs" extra-args)

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
check-sstable channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command check" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package sstable" extra-args)

[group("check-package")]
check-vfs channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command check" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package vfs" extra-args)

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
clippy-sstable-all *extra-args: \
    (check-util "--command clippy" "--all-channels" "--all-targets" "--package sstable" extra-args)

[group("clippy-package")]
clippy-vfs-all *extra-args: \
    (check-util "--command clippy" "--all-channels" "--all-targets" "--package vfs" extra-args)

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
clippy-sstable channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command clippy" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package sstable" extra-args)

[group("clippy-package")]
clippy-vfs channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command clippy" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package vfs" extra-args)

# Test-all

[group("test")]
test-all *extra-args: \
    (check-util "--command test" "--all-channels" "--all-targets" "--all-packages" extra-args)

[group("test-package")]
test-leveldb-all *extra-args: \
    (check-util "--command test" "--all-channels" "--all-targets" "--package leveldb" extra-args)

[group("test-package")]
test-skiplist-all *extra-args: \
    (check-util "--command test" "--all-channels" "--all-targets" "--package skiplist" extra-args)

[group("test-package")]
test-sstable-all *extra-args: \
    (check-util "--command test" "--all-channels" "--all-targets" "--package sstable" extra-args)

[group("test-package")]
test-vfs-all *extra-args: \
    (check-util "--command test" "--all-channels" "--all-targets" "--package vfs" extra-args)

# Test

[group("test")]
test channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command test" prepend("--channel ", channels) \
     prepend("--target ", targets) "--all-packages" extra-args)

[group("test-package")]
test-leveldb channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command test" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package leveldb" extra-args)

[group("test-package")]
test-skiplist channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command test" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package skiplist" extra-args)

[group("test-package")]
test-sstable channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command test" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package sstable" extra-args)

[group("test-package")]
test-vfs channels=all-channels targets=default-targets *extra-args: \
    (check-util "--command test" prepend("--channel ", channels) \
     prepend("--target ", targets) "--package vfs" extra-args)
