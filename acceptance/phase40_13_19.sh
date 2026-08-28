#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
python3 scripts/verify_phase40_13_19_static.py
python3 scripts/verify_api_openapi_parity.py --report qualification/phase40.13.19-api-openapi-parity.json
