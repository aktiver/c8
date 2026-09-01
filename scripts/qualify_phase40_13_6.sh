#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

python3 scripts/verify_phase40_13_6_static.py
cargo test --locked --offline -p ngkg-online-reasoning -p ngkg-direct-reasoner -p ngkg-direct-reasoner-worker
cargo check --locked --offline --workspace --all-targets --all-features
