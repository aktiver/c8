#!/usr/bin/env bash
set -euo pipefail

# The implementation/static chain is always run. Historical live phases are
# explicit because they require real datasets, tokens, pods, and failure probes.
mkdir -p qualification
python3 scripts/run_cumulative_static_gates.py \
  --from-phase 15 --through-phase 39.5 \
  --report qualification/cumulative-static-phase15-39.5.json
python3 scripts/verify_phase39_5_static.py
scripts/qualify_phase39_4.sh
python3 scripts/verify_phase_inheritance.py

if [[ "${NGKG_RUN_HISTORICAL_LIVE_GATES:-false}" == "true" ]]; then
  python3 scripts/run_acceptance_gates.py --from-phase 17 --through-phase 35
else
  echo "historical Phase 17-35 live gates not executed; set NGKG_RUN_HISTORICAL_LIVE_GATES=true with the required environment to qualify them" >&2
fi
