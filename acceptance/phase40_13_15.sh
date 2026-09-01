#!/usr/bin/env bash
set -euo pipefail
python3 scripts/verify_phase40_13_15_static.py
python3 scripts/verify_phase40_13_14_static.py
python3 scripts/verify_api_openapi_parity.py
