#!/usr/bin/env bash
set -euo pipefail

# Phase 40 is a governance/control baseline. Native Phase 39.5 qualification
# remains authoritative for inherited runtime behavior and fails closed if the
# required Cargo/Maven/Helm/Kubernetes environment is unavailable.
scripts/qualify_phase39_5.sh
python3 scripts/verify_phase40_static.py
python3 scripts/verify_api_openapi_parity.py --report qualification/phase40-api-openapi-parity.json
python3 scripts/run_cumulative_static_gates.py \
  --from-phase 15 --through-phase 40 \
  --report qualification/cumulative-static-phase15-40.json
python3 scripts/verify_phase_inheritance.py
