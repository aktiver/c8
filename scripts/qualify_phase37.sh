#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_APPROVED_PLATFORM_VALUES:?NGKG_APPROVED_PLATFORM_VALUES is required}"
: "${NGKG_APPROVED_WORKLOAD_VALUES:?NGKG_APPROVED_WORKLOAD_VALUES is required}"
: "${NGKG_NAMESPACE:?NGKG_NAMESPACE is required}"

scripts/qualify_phase36.sh
python3 scripts/verify_phase37_static.py
python3 scripts/validate_helm_values.py "$NGKG_APPROVED_WORKLOAD_VALUES"
cargo test --locked -p ngkg-dataset -p ngkg-reference -p ngkg-hydration -p ngkg-distributed-artifacts -p ngkg-distributed-build -p ngkg-api -p ngkg-online-serving --all-features
scripts/rke2_preflight.sh
