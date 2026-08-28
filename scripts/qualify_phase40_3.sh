#!/usr/bin/env bash
set -euo pipefail
scripts/qualify_phase40_2.sh
python3 scripts/verify_phase40_3_static.py
python3 scripts/validate_direct_bgp_result.py test-corpus/phase40_3/direct-bgp-result-valid-complete.json
python3 scripts/validate_direct_bgp_result.py test-corpus/phase40_3/direct-bgp-result-valid-failed.json
python3 scripts/run_cumulative_static_gates.py --from-phase 15 --through-phase 40.3 --report qualification/cumulative-static-phase15-40.3.json
command -v cargo >/dev/null || { echo "cargo is required for Phase 40.3 native qualification" >&2; exit 1; }
cargo test --locked -p ngkg-types phase40_3_tests
