#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python3 "${ROOT}/qualification/validate_context_slice_phase10.py"
if [[ "${NGKG_RUN_LIVE_CONTEXT_SLICE:-false}" == "true" ]]; then
  "${ROOT}/qualification/run_phase10_context_slice_e2e.sh"
fi
