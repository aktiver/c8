#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_APPROVED_WORKLOAD_VALUES:?NGKG_APPROVED_WORKLOAD_VALUES is required}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# Compilation stays deliberately bounded. Runtime partition fan-out is controlled separately by
# cgroup-aware service budgets and Kubernetes HPA signals.
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export BLIS_NUM_THREADS=1
export RAYON_NUM_THREADS=1

for tool in cargo mvn helm; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done

for verifier in $(find scripts -maxdepth 1 -type f -name 'verify_phase40_13_*_static.py' | sort -V); do
  python3 "$verifier"
done
python3 scripts/verify_api_openapi_parity.py

cargo fmt --all --check
cargo check --locked --offline --workspace --all-targets --all-features
cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings
cargo test --locked --offline -p ngkg-query-planner -p ngkg-sparql-compiler -p ngkg-query-executor
cargo test --locked --offline --workspace --all-features

mvn -o -f adapters/hermit-reasoner/pom.xml test package
helm lint charts/ngkg-workloads --values "$NGKG_APPROVED_WORKLOAD_VALUES"
helm template ngkg-workloads charts/ngkg-workloads \
  --namespace "${NGKG_NAMESPACE:-ngkg}" \
  --values "$NGKG_APPROVED_WORKLOAD_VALUES" >/tmp/ngkg-phase40.13.8-workloads.yaml

echo "Phase 40.13.8 native, Maven, and Helm qualification passed"
