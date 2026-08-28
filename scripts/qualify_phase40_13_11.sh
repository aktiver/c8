#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_APPROVED_PLATFORM_VALUES:?NGKG_APPROVED_PLATFORM_VALUES is required}"
: "${NGKG_APPROVED_WORKLOAD_VALUES:?NGKG_APPROVED_WORKLOAD_VALUES is required}"
: "${NGKG_PHASE40_13_11_LIVE_CLUSTER:?set true only for the designated multinode qualification cluster}"

if [[ "${NGKG_PHASE40_13_11_LIVE_CLUSTER}" != "true" ]]; then
  echo "NGKG_PHASE40_13_11_LIVE_CLUSTER must be true" >&2
  exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export BLIS_NUM_THREADS=1
export RAYON_NUM_THREADS=1

for tool in cargo helm kubectl; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done

python3 scripts/verify_phase40_13_10_static.py
python3 scripts/verify_phase40_13_11_static.py
python3 scripts/verify_api_openapi_parity.py
python3 scripts/validate_helm_values.py charts/ngkg-platform/values.yaml \
  --overlay "$NGKG_APPROVED_PLATFORM_VALUES"
python3 scripts/validate_helm_values.py charts/ngkg-workloads/values.yaml \
  --overlay "$NGKG_APPROVED_WORKLOAD_VALUES"

cargo fmt --all --check
cargo check --locked --offline --workspace --all-targets --all-features
cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings
cargo test --locked --offline -p ngkg-source-planner -p ngkg-artifact-store -p ngkg-kube \
  -p ngkg-reference-worker -p ngkg-operator -p ngkg-api
cargo test --locked --offline --workspace --all-features

helm lint charts/ngkg-crds
helm lint charts/ngkg-platform --values "$NGKG_APPROVED_PLATFORM_VALUES"
helm lint charts/ngkg-workloads --values "$NGKG_APPROVED_WORKLOAD_VALUES"
helm template ngkg-crds charts/ngkg-crds --namespace "${NGKG_NAMESPACE:-ngkg}" >/tmp/ngkg-phase40.13.11-crds.yaml
helm template ngkg-platform charts/ngkg-platform --namespace "${NGKG_NAMESPACE:-ngkg}" \
  --values "$NGKG_APPROVED_PLATFORM_VALUES" >/tmp/ngkg-phase40.13.11-platform.yaml
helm template ngkg-workloads charts/ngkg-workloads --namespace "${NGKG_NAMESPACE:-ngkg}" \
  --values "$NGKG_APPROVED_WORKLOAD_VALUES" >/tmp/ngkg-phase40.13.11-workloads.yaml

# The external harness must create a real cloud import before this gate and expose its resource name.
: "${NGKG_PHASE40_13_11_IMPORT_NAME:?live import resource name is required}"
: "${NGKG_NAMESPACE:?namespace is required}"
kubectl -n "$NGKG_NAMESPACE" wait --for=jsonpath='{.status.condition}'=CompilerHandoffPublished \
  "ngkgsourceimport/${NGKG_PHASE40_13_11_IMPORT_NAME}" --timeout=6h
operation_id="$(kubectl -n "$NGKG_NAMESPACE" get \
  "ngkgsourceimport/${NGKG_PHASE40_13_11_IMPORT_NAME}" -o jsonpath='{.spec.operationId}')"
kubectl -n "$NGKG_NAMESPACE" get jobs \
  -l "app.kubernetes.io/component=source-decode,ngkg.io/operation-id=${operation_id}" -o json \
  | python3 scripts/verify_phase40_13_11_live_jobs.py

echo "Phase 40.13.11 native, Helm, and live multinode gate passed"
