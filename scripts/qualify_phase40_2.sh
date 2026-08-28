#!/usr/bin/env bash
set -euo pipefail
scripts/qualify_phase40_1.sh
python3 scripts/verify_phase40_2_static.py
python3 scripts/validate_datatype_policy.py policies/owl-direct-datatype-policy.json
python3 scripts/run_cumulative_static_gates.py --from-phase 15 --through-phase 40.2 --report qualification/cumulative-static-phase15-40.2.json
command -v cargo >/dev/null || { echo "cargo is required for Phase 40.2 native qualification" >&2; exit 1; }
command -v mvn >/dev/null || { echo "maven is required for Phase 40.2 native qualification" >&2; exit 1; }
cargo test --locked -p ngkg-reference phase40_2_tests
mvn -f adapters/hermit-reasoner/pom.xml test
