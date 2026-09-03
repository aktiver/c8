#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 /absolute/config-root /absolute/evidence-root" >&2
  exit 64
fi

config_root="$1"
evidence_root="$2"
[[ "$config_root" = /* && "$evidence_root" = /* ]] || {
  echo "Phase 6 requires absolute config and evidence paths" >&2
  exit 64
}
[[ "${NGKG_PHASE6_EXECUTE_LIVE:-}" == "YES" ]] || {
  echo "Phase 6 live capacity and chaos execution is not approved" >&2
  exit 77
}

script_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
python3 "$script_root/scripts/run_controlled.py" \
  --config-root "$config_root" \
  --evidence-root "$evidence_root" \
  --run-id "${NGKG_PHASE6_RUN_ID:?set a stable qualification run ID}"
