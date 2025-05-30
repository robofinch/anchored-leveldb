all:	clippy test check check_web

clippy:
	cargo clippy --no-default-features
	cargo clippy
	cargo clippy --all-features

test:
	cargo test

check:
	cargo hack check --feature-powerset

check_web:
	cargo hack check --target wasm32-unknown-unknown --feature-powerset --features js

possible_missing_commas:
	rg -U '[^,]\n[ ]*\]'
	rg -U '[^,]\n[ ]*\)'
