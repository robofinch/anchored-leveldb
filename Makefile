# TODO: use `just` and a `Justfile`, since I'm reeeeeeeally making this `Makefile` do more
# than it's meant to.

nightly-clippy-flags = -Zcrate-attr="feature(strict_provenance_lints, multiple_supertrait_upcastable, must_not_suspend, non_exhaustive_omitted_patterns_lint, supertrait_item_shadowing, unqualified_local_imports)" -Wfuzzy_provenance_casts -Wlossy_provenance_casts -Wmultiple_supertrait_upcastable -Wmust_not_suspend -Wnon_exhaustive_omitted_patterns -Wsupertrait_item_shadowing_definition -Wsupertrait_item_shadowing_usage -Wsupertrait_item_shadowing_usage -Wunqualified_local_imports

# Everything *could* be under .PHONY, but just doing the one-word targets should be enough.
.PHONY:	all test check clippy

all:	clippy nightly-clippy test check check_web

clippy:
	cargo clippy --no-default-features --package anchored-ldb-vfs
	cargo clippy --package anchored-ldb-vfs
	cargo clippy --package anchored-ldb-vfs --features std-fs,zip,zip-time-js
# Can't do `--all-features`, since that requires `-Zpolonius`.

nightly-clippy:
	cargo +nightly clippy --no-default-features  --package anchored-ldb-vfs -- $(nightly-clippy-flags)
	cargo +nightly clippy                        --package anchored-ldb-vfs -- $(nightly-clippy-flags)
	RUSTFLAGS="-Zpolonius" cargo +nightly clippy --package anchored-ldb-vfs --all-features -- $(nightly-clippy-flags)

test:
	cargo test

check_on_save:
	cargo hack check --feature-powerset --exclude-features polonius --message-format=json
	RUSTFLAGS="-Zpolonius" cargo +nightly hack check --package anchored-ldb-vfs --feature-powerset --include-features polonius --message-format=json

check:
	cargo hack check --feature-powerset --exclude-features polonius

check_web:
	cargo hack check --target wasm32-unknown-unknown --feature-powerset --features js

find_possible_missing_commas:
	rg -U '[^,]\n[ ]*\]' || [ $$? -eq 1 ]
	rg -U '[^,]\n[ ]*\)' || [ $$? -eq 1 ]
	rg -U '[^,]\n[ ]*>'  || [ $$? -eq 1 ]

find_allow_attributes:
	rg '\[allow\('

find_unsafe_code:
	rg 'unsafe_code|unsafe'
