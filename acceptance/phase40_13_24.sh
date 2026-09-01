#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python3 "$root/scripts/verify_phase40_13_23_static.py"
python3 "$root/scripts/verify_phase40_13_24_static.py"
python3 "$root/scripts/verify_api_openapi_parity.py"
