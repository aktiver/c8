#!/usr/bin/env bash
set -euo pipefail
: "${NGKG_W3C_SUITE_CACHE:?NGKG_W3C_SUITE_CACHE is required}"
PYTHON_BIN="${NGKG_CONFORMANCE_PYTHON:-python3}"

scripts/qualify_phase39_1.sh
"$PYTHON_BIN" scripts/verify_phase39_2_static.py
SUITE_ROOT="$("$PYTHON_BIN" scripts/fetch_w3c_conformance.py --cache-root "$NGKG_W3C_SUITE_CACHE" --verify-only)"
mkdir -p qualification
cargo build --locked -p ngkg-reference --bin ngkg-w3c-case
"$PYTHON_BIN" scripts/run_w3c_conformance.py \
  --suite-root "$SUITE_ROOT" \
  --driver target/debug/ngkg-w3c-case \
  --report qualification/w3c-phase39.2.json \
  --manifest rdf/rdf11/rdf-trig/manifest.ttl \
  --manifest sparql/sparql11/manifest-sparql11-query.ttl \
  --manifest sparql/sparql11/manifest-sparql11-results.ttl \
  --jobs "${NGKG_W3C_JOBS:-2}" \
  --case-timeout-seconds "${NGKG_W3C_CASE_TIMEOUT_SECONDS:-120}" \
  --max-driver-output-bytes "${NGKG_W3C_MAX_DRIVER_OUTPUT_BYTES:-1048576}" \
  --fail-on-unsupported
