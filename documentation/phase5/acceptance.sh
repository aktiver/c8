#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
evidence_root="${1:?usage: phase5/acceptance.sh EVIDENCE_ROOT}"

python3 "${root}/NGKG_1_0_0_GA/scripts/verify_enterprise_stabilization_phase4.py"
python3 "${root}/NGKG_1_0_0_GA/scripts/verify_enterprise_stabilization_phase5.py"
python3 -m py_compile \
  "${root}/phase5/verify_live_prerequisites.py" \
  "${root}/NGKG_1_0_0_GA/scripts/verify_enterprise_stabilization_phase5.py"
python3 "${root}/phase5/verify_live_prerequisites.py" --evidence-root "${evidence_root}"

echo "Enterprise Stabilization Phase 5 production acceptance: PASS"
