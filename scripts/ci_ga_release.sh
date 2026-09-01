#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_GA_QUALIFICATION_LEDGER:?required}"
: "${NGKG_GA_DEFECT_LEDGER:?required}"
: "${NGKG_GA_RUNTIME_AUDIT:?required}"
: "${NGKG_GA_ARTIFACT_MANIFEST:?required}"
: "${NGKG_GA_SUPPLY_CHAIN_EVIDENCE:?required}"
: "${NGKG_GA_REPRODUCIBLE_BUILD_EVIDENCE:?required}"
: "${NGKG_GA_SUPPORT_MATRIX:?required}"
: "${NGKG_GA_CERTIFICATE_OUTPUT:?required}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

command -v cargo >/dev/null
command -v helm >/dev/null
command -v kubectl >/dev/null
command -v cosign >/dev/null

acceptance/ga.sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
helm lint charts/ngkg-crds
helm lint charts/ngkg-platform
helm lint charts/ngkg-workloads
python3 scripts/assess_ga_readiness.py --ledger "$NGKG_GA_QUALIFICATION_LEDGER" --require-publishable
python3 scripts/certify_ga_release.py \
  --qualifications "$NGKG_GA_QUALIFICATION_LEDGER" \
  --defects "$NGKG_GA_DEFECT_LEDGER" \
  --runtime-audit "$NGKG_GA_RUNTIME_AUDIT" \
  --freeze release/1.0.0/freeze-manifest.json \
  --artifacts "$NGKG_GA_ARTIFACT_MANIFEST" \
  --supply-chain "$NGKG_GA_SUPPLY_CHAIN_EVIDENCE" \
  --reproducible-build "$NGKG_GA_REPRODUCIBLE_BUILD_EVIDENCE" \
  --support-matrix "$NGKG_GA_SUPPORT_MATRIX" \
  --known-issues release/1.0.0/KNOWN_ISSUES.md \
  --acceptance-plan release/1.0.0/ACCEPTANCE_TEST_PLAN.md \
  --output "$NGKG_GA_CERTIFICATE_OUTPUT"
