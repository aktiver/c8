#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_APPROVED_PLATFORM_VALUES:?NGKG_APPROVED_PLATFORM_VALUES is required}"
: "${NGKG_APPROVED_WORKLOAD_VALUES:?NGKG_APPROVED_WORKLOAD_VALUES is required}"
: "${NGKG_NAMESPACE:?NGKG_NAMESPACE is required}"

scripts/qualify_phase37.sh
python3 scripts/verify_phase38_static.py
cargo test --locked -p ngkg-sparql-compiler -p ngkg-reference -p ngkg-online-serving --all-features
scripts/rke2_preflight.sh
