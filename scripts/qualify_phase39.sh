#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_APPROVED_PLATFORM_VALUES:?NGKG_APPROVED_PLATFORM_VALUES is required}"
: "${NGKG_APPROVED_WORKLOAD_VALUES:?NGKG_APPROVED_WORKLOAD_VALUES is required}"
: "${NGKG_NAMESPACE:?NGKG_NAMESPACE is required}"

scripts/qualify_phase38.sh
python3 scripts/verify_phase39_static.py
cargo test --locked -p ngkg-sparql-compiler -p ngkg-reference -p ngkg-online-serving --all-features
python3 scripts/validate_helm_values.py "$NGKG_APPROVED_WORKLOAD_VALUES"
scripts/rke2_preflight.sh
