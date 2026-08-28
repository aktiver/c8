#!/usr/bin/env bash
set -euo pipefail

python scripts/verify_phase40_13_12_static.py
python scripts/verify_phase40_13_11_static.py
cargo test -p ngkg-semantic-compiler
cargo check --workspace --all-targets --all-features
