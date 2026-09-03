#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'
root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)"
cd "${root}"
python3 phase8/verify_phase8.py
python3 docker_repos/validate_image_parity.py
bash -n docker_repos/build_all_local.sh phase8/acceptance.sh
python3 -m compileall -q phase8 docker_repos
if command -v gcc >/dev/null 2>&1; then
  python3 phase8/test_openmp_kernel.py
fi
