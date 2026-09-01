#!/usr/bin/env bash
set -euo pipefail

python3 scripts/verify_phase40_13_14_static.py
cargo fmt --all --check
cargo check --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
helm lint charts/ngkg-platform -f charts/ngkg-platform/values.yaml
helm template ngkg charts/ngkg-platform -f charts/ngkg-platform/values.yaml >/dev/null
