#!/usr/bin/env bash
set -euo pipefail

# Inherited gates remain authoritative; this script does not convert missing
# native toolchains into successful qualification.
scripts/qualify_phase40.sh
python3 scripts/verify_phase40_1_static.py
python3 scripts/validate_owl_signature.py test-corpus/phase40_1/owl-signature-valid.json
python3 scripts/run_cumulative_static_gates.py \
  --from-phase 15 --through-phase 40.1 \
  --report qualification/cumulative-static-phase15-40.1.json

command -v cargo >/dev/null || { echo "cargo is required for Phase 40.1 native qualification" >&2; exit 1; }
command -v mvn >/dev/null || { echo "maven is required for Phase 40.1 native qualification" >&2; exit 1; }
cargo test --locked -p ngkg-reference phase40_1_tests
mvn -f adapters/hermit-reasoner/pom.xml test
