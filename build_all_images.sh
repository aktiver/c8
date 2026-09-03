#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "${NGKG_LOCAL_REGISTRY:-}" && -n "${NGKG_IMAGE_REGISTRY:-}" ]]; then
  export NGKG_LOCAL_REGISTRY="${NGKG_IMAGE_REGISTRY}"
fi

exec "${root_dir}/docker_repos/build_all_local.sh" "$@"
