#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repository_root}"

python3 scripts/verify_phase40_13_16_static.py
python3 scripts/verify_api_openapi_parity.py
python3 scripts/validate_helm_values.py charts/ngkg-workloads/values.yaml
python3 scripts/validate_helm_values.py charts/ngkg-workloads/values.yaml \
  --overlay charts/ngkg-workloads/profiles/production-workload-autoscaling.yaml

if command -v cargo >/dev/null 2>&1; then
  cargo fmt --all --check
  cargo check --locked --workspace --all-targets --all-features
  cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
  cargo test --locked --workspace --all-features
else
  echo "cargo is unavailable; native Phase 40.13.16 qualification is blocked" >&2
  exit 2
fi
