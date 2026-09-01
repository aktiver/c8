#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_W3C_SUITE_CACHE:?NGKG_W3C_SUITE_CACHE is required}"
PYTHON_BIN="${NGKG_CONFORMANCE_PYTHON:-python3}"

export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-1}"

"$PYTHON_BIN" scripts/verify_phase40_13_4_static.py
"$PYTHON_BIN" scripts/validate_sparql11_feature_matrix.py
"$PYTHON_BIN" scripts/test_w3c_conformance.py
cargo test --locked -p ngkg-sparql-compiler
cargo test --locked -p ngkg-reference
cargo build --locked -p ngkg-reference --bin ngkg-w3c-case

SUITE_ROOT="$("$PYTHON_BIN" scripts/fetch_w3c_conformance.py \
  --cache-root "$NGKG_W3C_SUITE_CACHE" --verify-only)"
set +e
"$PYTHON_BIN" scripts/run_w3c_conformance.py \
  --suite-root "$SUITE_ROOT" \
  --driver target/debug/ngkg-w3c-case \
  --report qualification/w3c-phase40.13.4-query-results.json \
  --manifest sparql/sparql11/manifest-sparql11-query.ttl \
  --manifest sparql/sparql11/manifest-sparql11-results.ttl
RUNNER_STATUS=$?
set -e
if (( RUNNER_STATUS > 1 )); then
  echo "W3C runner failed before producing a valid conformance result" >&2
  exit "$RUNNER_STATUS"
fi
"$PYTHON_BIN" scripts/verify_phase40_13_4_report.py
