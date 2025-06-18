# TODO: (test-all) (build-all)
[doc]
do-it-all: (check-all) (clippy-all)

on-save: \
    (check default-channels default-targets "--message-format=json --depth 1") \
    (clippy default-channels default-targets "--message-format=json --depth 1")

# check-on-save: \
#     (check default-channels default-targets "--message-format=json --depth 1")

# clippy-on-save: \
#     (clippy default-channels default-targets "--message-format=json --depth 1")


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
#   Flags, and then `check` and `clippy`
# ================================================================

# Current packages:
#    `anchored-leveldb` (features: `serde` `lender` `lending-iterator` `threading` `std-fs` `js`)
#    `anchored-ldb-vfs` (features: `std-fs` `zip` `zip-time-js` `polonius`)
# The `polonius` and `js`-related features need special handling.

wasm-target := "wasm32-unknown-unknown"
leveldb-pkg := "anchored-leveldb"
ldb-vfs-pkg := "anchored-ldb-vfs"

default-channels := 'stable nightly'
default-targets := 'native wasm'
all-targets := 'aarch64-apple-darwin x86_64-unknown-linux-gnu wasm x86_64-pc-windows-msvc'


stable-rust-flags := ''
stable-ck := 'hack check'
stable-clippy := 'hack clippy'
nightly-rust-flags := '-Zpolonius'
nightly-ck := '+nightly hack check'
nightly-clippy := '+nightly hack clippy'

common-leveldb := "--feature-powerset --package " + leveldb-pkg
stable-leveldb := ''
stable-leveldb-wasm := '--exclude-features threading,std-fs --features js'
nightly-leveldb := ''
nightly-leveldb-wasm := '--exclude-features threading,std-fs --features js'

common-ldb-vfs := "--feature-powerset --package " + ldb-vfs-pkg
stable-ldb-vfs := '--exclude-features polonius'
stable-ldb-vfs-wasm := '--exclude-features polonius,std-fs --group-features zip,zip-time-js'
nightly-ldb-vfs := '--features polonius'
nightly-ldb-vfs-wasm := '--exclude-features std-fs --features polonius --group-features zip,zip-time-js'


stable-clippy-flags := ''
nightly-clippy-flags := \
    '-- -Zcrate-attr="feature( \
    strict_provenance_lints, \
    multiple_supertrait_upcastable, \
    must_not_suspend, \
    non_exhaustive_omitted_patterns_lint, \
    supertrait_item_shadowing, \
    unqualified_local_imports \
    )" \
    -Wfuzzy_provenance_casts \
    -Wlossy_provenance_casts \
    -Wmultiple_supertrait_upcastable \
    -Wmust_not_suspend \
    -Wnon_exhaustive_omitted_patterns \
    -Wsupertrait_item_shadowing_definition \
    -Wsupertrait_item_shadowing_usage \
    -Wsupertrait_item_shadowing_usage \
    -Wunqualified_local_imports'

# ================================================================
#   Frontends for `cargo hack check`
# ================================================================

[group("check")]
check-all channels=default-channels *cargo-args: \
    (check-leveldb-all channels cargo-args) \
    (check-ldb-vfs-all channels cargo-args) \

[group("check")]
check channels=default-channels targets=default-targets *cargo-args: \
    (check-leveldb channels targets cargo-args) \
    (check-ldb-vfs channels targets cargo-args) \

[group("check")]
check-leveldb-all channels=default-channels *cargo-args: \
    (check-leveldb channels all-targets cargo-args)

[group("check")]
check-leveldb channels=default-channels targets=default-targets *cargo-args:
    #!/usr/bin/env bash
    set -euxo pipefail
    for channel in {{channels}}; do
        for target in {{targets}}; do
            if [ $channel = "stable" ]; then
                if [ $target = "native" ]; then
                    flags="{{stable-leveldb}}"
                elif [ $target = "wasm" ]; then
                   flags="{{stable-leveldb-wasm}} --target {{wasm-target}}"
                else
                    flags="{{stable-leveldb-wasm}} --target $target"
                fi
                RUSTFLAGS='{{stable-rust-flags}}' cargo {{stable-ck}} {{common-leveldb}} $flags {{cargo-args}}
            else
                if [ $target = "native" ]; then
                    flags="{{nightly-leveldb}}"
                elif [ $target = "wasm" ]; then
                   flags="{{nightly-leveldb-wasm}} --target {{wasm-target}}"
                else
                    flags="{{nightly-leveldb-wasm}} --target $target"
                fi
                RUSTFLAGS='{{nightly-rust-flags}}' cargo {{nightly-ck}} {{common-leveldb}} $flags {{cargo-args}}
            fi
        done;
    done

[group("check")]
check-ldb-vfs-all channels=default-channels *cargo-args: \
    (check-ldb-vfs channels all-targets cargo-args)

[group("check")]
check-ldb-vfs channels=default-channels targets=default-targets *cargo-args:
    #!/usr/bin/env bash
    set -euxo pipefail
    for channel in {{channels}}; do
        for target in {{targets}}; do
            if [ $channel = "stable" ]; then
                if [ $target = "native" ]; then
                    flags="{{stable-ldb-vfs}}"
                elif [ $target = "wasm" ]; then
                   flags="{{stable-ldb-vfs-wasm}} --target {{wasm-target}}"
                else
                    flags="{{stable-ldb-vfs-wasm}} --target $target"
                fi
                RUSTFLAGS='{{stable-rust-flags}}' cargo {{stable-ck}} {{common-ldb-vfs}} $flags {{cargo-args}}
            else
                if [ $target = "native" ]; then
                    flags="{{nightly-ldb-vfs}}"
                elif [ $target = "wasm" ]; then
                   flags="{{nightly-ldb-vfs-wasm}} --target {{wasm-target}}"
                else
                    flags="{{nightly-ldb-vfs-wasm}} --target $target"
                fi
                RUSTFLAGS='{{nightly-rust-flags}}' cargo {{nightly-ck}} {{common-ldb-vfs}} $flags {{cargo-args}}
            fi
        done;
    done

# ================================================================
#   Frontends for `cargo clippy`
# ================================================================

[group("clippy")]
clippy-all channels=default-channels *cargo-args: \
    (clippy-leveldb-all channels cargo-args) \
    (clippy-ldb-vfs-all channels cargo-args) \

[group("clippy")]
clippy channels=default-channels targets=default-targets *cargo-args: \
    (clippy-leveldb channels targets cargo-args) \
    (clippy-ldb-vfs channels targets cargo-args) \

[group("clippy")]
clippy-leveldb-all channels=default-channels *cargo-args: \
    (clippy-leveldb channels all-targets cargo-args)

[group("clippy")]
clippy-ldb-vfs-all channels=default-channels *cargo-args: \
    (clippy-ldb-vfs channels all-targets cargo-args)

[group("clippy")]
clippy-leveldb channels=default-channels targets=default-targets *cargo-args:
    #!/usr/bin/env bash
    set -euxo pipefail
    for channel in {{channels}}; do
        for target in {{targets}}; do
            if [ $channel = "stable" ]; then
                if [ $target = "native" ]; then
                    flags="{{stable-leveldb}}"
                elif [ $target = "wasm" ]; then
                   flags="{{stable-leveldb-wasm}} --target {{wasm-target}}"
                else
                    flags="{{stable-leveldb-wasm}} --target $target"
                fi
                RUSTFLAGS='{{stable-rust-flags}}' cargo {{stable-clippy}} \
                    {{common-leveldb}} $flags {{cargo-args}} {{stable-clippy-flags}}
            else
                if [ $target = "native" ]; then
                    flags="{{nightly-leveldb}}"
                elif [ $target = "wasm" ]; then
                   flags="{{nightly-leveldb-wasm}} --target {{wasm-target}}"
                else
                    flags="{{nightly-leveldb-wasm}} --target $target"
                fi
                RUSTFLAGS='{{nightly-rust-flags}}' cargo {{nightly-clippy}} \
                    {{common-leveldb}} $flags {{cargo-args}} {{nightly-clippy-flags}}
            fi
        done;
    done

[group("clippy")]
clippy-ldb-vfs channels=default-channels targets=default-targets *cargo-args:
    #!/usr/bin/env bash
    set -euxo pipefail
    for channel in {{channels}}; do
        for target in {{targets}}; do
            if [ $channel = "stable" ]; then
                if [ $target = "native" ]; then
                    flags="{{stable-ldb-vfs}}"
                elif [ $target = "wasm" ]; then
                   flags="{{stable-ldb-vfs-wasm}} --target {{wasm-target}}"
                else
                    flags="{{stable-ldb-vfs-wasm}} --target $target"
                fi
                RUSTFLAGS='{{stable-rust-flags}}' cargo {{stable-clippy}} \
                    {{common-ldb-vfs}} $flags {{cargo-args}} {{stable-clippy-flags}}
            else
                if [ $target = "native" ]; then
                    flags="{{nightly-ldb-vfs}}"
                elif [ $target = "wasm" ]; then
                   flags="{{nightly-ldb-vfs-wasm}} --target {{wasm-target}}"
                else
                    flags="{{nightly-ldb-vfs-wasm}} --target $target"
                fi
                RUSTFLAGS='{{nightly-rust-flags}}' cargo {{nightly-clippy}} \
                    {{common-ldb-vfs}} $flags {{cargo-args}} {{nightly-clippy-flags}}
            fi
        done;
    done
