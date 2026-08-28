#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/verify_phase40_13_static.py
python3 scripts/structural_validate.py
