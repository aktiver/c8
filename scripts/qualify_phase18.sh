#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

python3 scripts/structural_validate.py --root .
python3 scripts/verify_phase18_static.py
python3 scripts/validate_helm_values.py charts/ngkg-workloads/values.yaml
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features

if command -v helm >/dev/null 2>&1; then
  helm template ngkg-workloads charts/ngkg-workloads \
    --set images.query.repository=registry.invalid/ngkg/query \
    --set images.query.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
    --set images.locator.repository=registry.invalid/ngkg/locator \
    --set images.locator.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
    --set images.hydration.repository=registry.invalid/ngkg/hydration \
    --set images.hydration.digest=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc \
    --set tls.existingSecret=ngkg-data-plane-tls >/dev/null
fi

echo "Phase 18 local qualification passed. RKE2, autoscaling, object-store, corruption, and node-loss gates remain external."
