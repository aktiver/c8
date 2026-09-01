#!/usr/bin/env bash
set -euo pipefail
scripts/qualify_phase40_10.sh
python3 scripts/verify_phase40_11_static.py
python3 scripts/validate_phase40_helm_ceilings.py --root . --report qualification/phase40.11-helm-ceilings.json
if command -v cargo >/dev/null 2>&1 && [[ -f Cargo.lock ]]; then
  cargo test --locked -p ngkg-reference-worker phase40_11_tests
  cargo test --locked -p ngkg-direct-reasoner
else
  echo 'Phase 40.11 native Rust qualification not executed: cargo/Cargo.lock unavailable' >&2
fi
