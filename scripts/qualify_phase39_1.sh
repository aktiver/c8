#!/usr/bin/env bash
set -euo pipefail

# Generate the lock once with the exact pinned resolver when absent, then run
# the cumulative Phase 39 gate entirely in --locked mode.
if [[ ! -s Cargo.lock ]]; then
  scripts/generate_cargo_lock.sh
fi
test -s Cargo.lock
python3 scripts/verify_phase39_1_static.py
cargo metadata --locked --format-version 1 --no-deps >/dev/null
scripts/qualify_phase39.sh
