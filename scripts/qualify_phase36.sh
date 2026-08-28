#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_APPROVED_PLATFORM_VALUES:?NGKG_APPROVED_PLATFORM_VALUES is required}"
: "${NGKG_APPROVED_WORKLOAD_VALUES:?NGKG_APPROVED_WORKLOAD_VALUES is required}"
: "${NGKG_NAMESPACE:?NGKG_NAMESPACE is required}"
: "${NGKG_W3C_SUITE_CACHE:?NGKG_W3C_SUITE_CACHE is required}"

for tool in cargo mvn helm kubectl python3; do
  command -v "$tool" >/dev/null
 done

test -f Cargo.lock
python3 scripts/structural_validate.py --root . >/dev/null
python3 scripts/verify_phase36_static.py
python3 scripts/fetch_w3c_conformance.py --cache-root "$NGKG_W3C_SUITE_CACHE"
python3 scripts/validate_platform_values.py "$NGKG_APPROVED_PLATFORM_VALUES"
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked -p ngkg-dataset -p ngkg-reference -p ngkg-query-cache -p ngkg-api -p ngkg-online-serving --all-features
mvn -B -ntp -f adapters/hermit-reasoner/pom.xml verify
helm lint charts/ngkg-platform --values "$NGKG_APPROVED_PLATFORM_VALUES"
helm lint charts/ngkg-workloads --values "$NGKG_APPROVED_WORKLOAD_VALUES"
helm template ngkg-platform charts/ngkg-platform --namespace "$NGKG_NAMESPACE" --values "$NGKG_APPROVED_PLATFORM_VALUES" | kubectl apply --dry-run=server -f -
helm template ngkg-workloads charts/ngkg-workloads --namespace "$NGKG_NAMESPACE" --values "$NGKG_APPROVED_WORKLOAD_VALUES" | kubectl apply --dry-run=server -f -
