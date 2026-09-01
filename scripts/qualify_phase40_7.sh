#!/usr/bin/env bash
set -euo pipefail
scripts/qualify_phase40_6.sh
python3 scripts/verify_phase40_7_static.py
python3 scripts/validate_direct_bgp_legality.py test-corpus/phase40_7/direct-bgp-legality-valid-legal.json
python3 scripts/validate_direct_bgp_legality.py test-corpus/phase40_7/direct-bgp-legality-valid-illegal.json
python3 scripts/verify_api_openapi_parity.py --report qualification/phase40_7-api-openapi-parity.json
python3 scripts/run_cumulative_static_gates.py --from-phase 15 --through-phase 40.7 --report qualification/cumulative-static-phase15-40.7.json
command -v cargo >/dev/null || { echo "cargo is required for Phase 40.7 native qualification" >&2; exit 1; }
cargo test --locked -p ngkg-types direct_legality
cargo test --locked -p ngkg-owl-direct
cargo test --locked -p ngkg-reasoner-client
cargo test --locked -p ngkg-online-serving
