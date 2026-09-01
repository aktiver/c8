#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_APPROVED_PLATFORM_VALUES:?NGKG_APPROVED_PLATFORM_VALUES is required}"
: "${NGKG_APPROVED_WORKLOAD_VALUES:?NGKG_APPROVED_WORKLOAD_VALUES is required}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export BLIS_NUM_THREADS=1
export RAYON_NUM_THREADS=1

for tool in cargo helm; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done

python3 scripts/verify_phase40_13_9_static.py
python3 scripts/verify_phase40_13_10_static.py
python3 scripts/verify_api_openapi_parity.py
python3 scripts/validate_helm_values.py charts/ngkg-workloads/values.yaml \
  --overlay "$NGKG_APPROVED_WORKLOAD_VALUES"

cargo fmt --all --check
cargo check --locked --offline --workspace --all-targets --all-features
cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings
cargo test --locked --offline -p ngkg-kube -p ngkg-catalog -p ngkg-reference-worker -p ngkg-api -p ngkg-operator
cargo test --locked --offline --workspace --all-features

helm lint charts/ngkg-crds
helm lint charts/ngkg-platform --values "$NGKG_APPROVED_PLATFORM_VALUES"
helm lint charts/ngkg-workloads --values "$NGKG_APPROVED_WORKLOAD_VALUES"
helm template ngkg-crds charts/ngkg-crds --namespace "${NGKG_NAMESPACE:-ngkg}" >/tmp/ngkg-phase40.13.10-crds.yaml
helm template ngkg-platform charts/ngkg-platform \
  --namespace "${NGKG_NAMESPACE:-ngkg}" \
  --values "$NGKG_APPROVED_PLATFORM_VALUES" >/tmp/ngkg-phase40.13.10-platform.yaml
helm template ngkg-workloads charts/ngkg-workloads \
  --namespace "${NGKG_NAMESPACE:-ngkg}" \
  --values "$NGKG_APPROVED_WORKLOAD_VALUES" >/tmp/ngkg-phase40.13.10-workloads.yaml

echo "Phase 40.13.10 native and Helm qualification passed"
