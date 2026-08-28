#!/usr/bin/env bash
set -euo pipefail

# Phase 39.1 reproducibility gate. Cargo.lock must be produced by the pinned
# Cargo resolver, never hand-edited or synthesized by release tooling.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

command -v rustc >/dev/null || { echo "rustc is required" >&2; exit 1; }
command -v cargo >/dev/null || { echo "cargo is required" >&2; exit 1; }

EXPECTED="$(python3 - <<'PY'
import tomllib
from pathlib import Path
v=tomllib.loads(Path('Cargo.toml').read_text())['workspace']['package']['rust-version']
print(v)
PY
)"
RUSTC_VERSION="$(rustc --version | awk '{print $2}')"
CARGO_VERSION="$(cargo --version | awk '{print $2}')"
if [[ "$RUSTC_VERSION" != "$EXPECTED" || "$CARGO_VERSION" != "$EXPECTED" ]]; then
  echo "pinned Rust/Cargo $EXPECTED required; observed rustc=$RUSTC_VERSION cargo=$CARGO_VERSION" >&2
  exit 1
fi

if [[ -f Cargo.lock ]]; then
  cp Cargo.lock "${TMPDIR:-/tmp}/ngkg-cargo-lock.before.$$"
fi
cargo generate-lockfile

test -s Cargo.lock
# A second locked metadata resolution proves the generated graph is usable and
# cannot silently update during qualification.
cargo metadata --locked --format-version 1 --no-deps >/dev/null
sha256sum Cargo.lock
