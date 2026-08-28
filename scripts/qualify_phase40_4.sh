#!/usr/bin/env bash
set -euo pipefail
scripts/qualify_phase40_3.sh
python3 scripts/verify_phase40_4_static.py
python3 scripts/validate_direct_certificate.py test-corpus/phase40_4/direct-certificate-valid.json --result test-corpus/phase40_3/direct-bgp-result-valid-complete.json
python3 scripts/run_cumulative_static_gates.py --from-phase 15 --through-phase 40.4 --report qualification/cumulative-static-phase15-40.4.json
command -v cargo >/dev/null || { echo "cargo is required for Phase 40.4 native qualification" >&2; exit 1; }
cargo test --locked -p ngkg-types phase40_4_tests
