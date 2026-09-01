#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python3 -m py_compile \
  "${root}/phase3/scripts/write_provenance.py" \
  "${root}/phase3/scripts/verify_and_issue.py" \
  "${root}/phase3/scripts/qualify_cluster.py" \
  "${root}/phase3/scripts/verify_toolchain.py" \
  "${root}/phase3/scripts/verify_phase3_static.py"
bash -n \
  "${root}/phase3/scripts/common.sh" \
  "${root}/phase3/scripts/build_supply_chain.sh" \
  "${root}/phase3/scripts/qualify_postgres.sh" \
  "${root}/phase3/scripts/deploy_cluster.sh" \
  "${root}/phase3/scripts/sign_evidence.sh"
for file in "${root}"/phase3/config/*.json "${root}"/phase3/schemas/*.json "${root}"/phase3/fixtures/*.json; do
  jq -e . "${file}" >/dev/null
done
python3 "${root}/phase3/scripts/verify_phase3_static.py"
python3 -m unittest "${root}/phase3/tests/test_verifier.py"
echo "Enterprise Stabilization Phase 3 source acceptance: PASS"
