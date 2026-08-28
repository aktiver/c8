#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_APPROVED_PLATFORM_VALUES:?NGKG_APPROVED_PLATFORM_VALUES is required}"
: "${NGKG_APPROVED_WORKLOAD_VALUES:?NGKG_APPROVED_WORKLOAD_VALUES is required}"
: "${NGKG_NAMESPACE:?NGKG_NAMESPACE is required}"
: "${NGKG_W3C_SUITE_CACHE:?NGKG_W3C_SUITE_CACHE is required}"

command -v cargo >/dev/null
command -v mvn >/dev/null
command -v helm >/dev/null
command -v kubectl >/dev/null
test -f Cargo.lock

python3 scripts/structural_validate.py --root .
python3 scripts/run_cumulative_static_gates.py --from-phase 15 --through-phase 40.6 --report qualification/cumulative-static-phase15-40.6.json
SUITE_ROOT="$(python3 scripts/fetch_w3c_conformance.py --cache-root "$NGKG_W3C_SUITE_CACHE")"
python3 scripts/run_w3c_conformance.py \
  --suite-root "$SUITE_ROOT" \
  --report qualification/w3c-phase39.2.json \
  --manifest rdf/rdf11/rdf-trig/manifest.ttl \
  --manifest sparql/sparql11/manifest-sparql11-query.ttl \
  --manifest sparql/sparql11/manifest-sparql11-results.ttl \
  --fail-on-unsupported
python3 scripts/verify_phase39_5_static.py
python3 scripts/verify_phase40_static.py
python3 scripts/verify_phase40_1_static.py
python3 scripts/verify_phase40_2_static.py
python3 scripts/verify_phase40_3_static.py
python3 scripts/verify_phase40_4_static.py
python3 scripts/verify_phase40_5_static.py
python3 scripts/verify_phase40_6_static.py
python3 scripts/verify_phase40_7_static.py
python3 scripts/verify_phase40_8_static.py
python3 scripts/verify_phase40_9_static.py
python3 scripts/verify_phase40_10_static.py
python3 scripts/verify_phase40_11_static.py
python3 scripts/verify_phase40_12_static.py
python3 scripts/verify_phase40_13_static.py
python3 scripts/validate_owl_consistency_qualification.py test-corpus/phase40_6/owl-consistency-qualification-valid-consistent.json
python3 scripts/validate_owl_profile_qualification.py test-corpus/phase40_5/owl-profile-qualification-valid.json
python3 scripts/validate_direct_certificate.py test-corpus/phase40_4/direct-certificate-valid.json --result test-corpus/phase40_3/direct-bgp-result-valid-complete.json
python3 scripts/validate_datatype_policy.py policies/owl-direct-datatype-policy.json
python3 scripts/verify_api_openapi_parity.py --report qualification/phase40-api-openapi-parity.json
python3 scripts/validate_platform_values.py "$NGKG_APPROVED_PLATFORM_VALUES"
python3 scripts/verify_phase_inheritance.py
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
mvn -B -ntp -f adapters/hermit-reasoner/pom.xml verify
helm lint charts/ngkg-crds
helm lint charts/ngkg-platform --values "$NGKG_APPROVED_PLATFORM_VALUES"
helm lint charts/ngkg-workloads --values "$NGKG_APPROVED_WORKLOAD_VALUES"
helm template ngkg-crds charts/ngkg-crds | kubectl apply --dry-run=server -f -
helm template ngkg-platform charts/ngkg-platform --namespace "$NGKG_NAMESPACE" --values "$NGKG_APPROVED_PLATFORM_VALUES" | kubectl apply --dry-run=server -f -
helm template ngkg-workloads charts/ngkg-workloads --namespace "$NGKG_NAMESPACE" --values "$NGKG_APPROVED_WORKLOAD_VALUES" | kubectl apply --dry-run=server -f -
scripts/rke2_preflight.sh
