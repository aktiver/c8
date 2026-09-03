#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python3 "${ROOT}/qualification/validate_vllm_gpu_phase9.py"
"${ROOT}/qualification/run_phase9_gpu_e2e.sh"
