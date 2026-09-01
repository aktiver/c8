#!/usr/bin/env bash
set -euo pipefail
scripts/qualify_phase40_5.sh
python3 scripts/verify_phase40_6_static.py
python3 scripts/validate_owl_consistency_qualification.py test-corpus/phase40_6/owl-consistency-qualification-valid-consistent.json
python3 scripts/validate_owl_consistency_qualification.py test-corpus/phase40_6/owl-consistency-qualification-valid-inconsistent.json
python3 scripts/run_cumulative_static_gates.py --from-phase 15 --through-phase 40.6 --report qualification/cumulative-static-phase15-40.6.json
command -v cargo >/dev/null || { echo "cargo is required for Phase 40.6 native qualification" >&2; exit 1; }
command -v mvn >/dev/null || { echo "maven is required for Phase 40.6 native qualification" >&2; exit 1; }
cargo test --locked -p ngkg-reference phase40_6_tests
mvn -B -ntp -f adapters/hermit-reasoner/pom.xml -Dtest=MainTest verify
