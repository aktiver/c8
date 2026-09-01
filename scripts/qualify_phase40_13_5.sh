#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_W3C_SUITE_CACHE:?NGKG_W3C_SUITE_CACHE is required}"
PYTHON_BIN="${NGKG_CONFORMANCE_PYTHON:-python3}"

# Compilation is deliberately bounded independently of runtime parallelism.
# Runtime services derive their budgets from cgroup/cpuset limits and Kubernetes
# scales distinct query, fragment, hydration, and reasoning worker pools.
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export BLIS_NUM_THREADS="${BLIS_NUM_THREADS:-1}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-1}"
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-1}"

"$PYTHON_BIN" scripts/verify_phase40_13_5_static.py
"$PYTHON_BIN" scripts/validate_sparql11_feature_matrix.py
"$PYTHON_BIN" scripts/test_w3c_conformance.py
cargo test --locked --offline -p ngkg-sparql-compiler
cargo test --locked --offline -p ngkg-reference
cargo build --locked --offline -p ngkg-reference --bin ngkg-w3c-case

SUITE_ROOT="$("$PYTHON_BIN" scripts/fetch_w3c_conformance.py \
  --cache-root "$NGKG_W3C_SUITE_CACHE" --verify-only)"
"$PYTHON_BIN" scripts/run_w3c_conformance.py \
  --suite-root "$SUITE_ROOT" \
  --driver target/debug/ngkg-w3c-case \
  --report qualification/w3c-phase40.13.5-query-results.json \
  --manifest sparql/sparql11/manifest-sparql11-query.ttl \
  --manifest sparql/sparql11/manifest-sparql11-results.ttl \
  --jobs "${NGKG_CONFORMANCE_JOBS:-2}"
"$PYTHON_BIN" scripts/verify_phase40_13_5_report.py
