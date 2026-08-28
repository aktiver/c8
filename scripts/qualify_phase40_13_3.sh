#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_W3C_SUITE_CACHE:?NGKG_W3C_SUITE_CACHE is required}"
PYTHON_BIN="${NGKG_CONFORMANCE_PYTHON:-python3}"

export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-1}"

"$PYTHON_BIN" scripts/verify_phase40_13_3_static.py
"$PYTHON_BIN" scripts/validate_sparql11_feature_matrix.py
"$PYTHON_BIN" scripts/test_w3c_conformance.py
cargo test --locked -p ngkg-reference --bin ngkg-w3c-case

"$PYTHON_BIN" scripts/run_w3c_conformance.py \
  --suite-root "$("$PYTHON_BIN" scripts/fetch_w3c_conformance.py --cache-root "$NGKG_W3C_SUITE_CACHE" --verify-only)" \
  --report qualification/w3c-phase40.13.3-inventory.json \
  --inventory-only

scripts/qualify_phase39_2.sh
