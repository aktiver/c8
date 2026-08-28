#!/usr/bin/env bash
set -euo pipefail

command -v cargo >/dev/null || { echo "cargo is required" >&2; exit 2; }
command -v sha256sum >/dev/null || { echo "sha256sum is required" >&2; exit 2; }
test -f Cargo.lock || {
  echo "Cargo.lock is required; generate and review it with the pinned toolchain before running a locked build" >&2
  exit 2
}

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repository_root}"
source_path="${repository_root}/test-corpus/datasets/cross-domain.trig"
policy_path="${repository_root}/test-corpus/reference/projection-policy.json"
source_sha256="$(sha256sum "${source_path}" | awk '{print $1}')"
policy_sha256="$(sha256sum "${policy_path}" | awk '{print $1}')"
run_root="${NGKG_DISTRIBUTED_OUTPUT_ROOT:-$(mktemp -d)}"
mkdir -p "${run_root}"

dataset_id="4d2e1a82-c2bc-536a-a809-fda7643ef1f7"
snapshot_id="91054ecb-2f68-5a63-b31a-137333c64a7c"
dataset_namespace="7b8e1c18-9c22-5b58-a2b4-7cbf21cc9b2b"
source_guid="d2cfd1d5-5f83-5bb3-9888-f3bc3229a760"
source_snapshot="reference-corpus-v1"

cargo build --quiet --locked --package ngkg-distributed-worker
worker=("${repository_root}/target/debug/ngkg-distributed-worker")

build_layout() {
  local name="$1"
  local partition_count="$2"
  local reducer_count="$3"
  local build_root="${run_root}/${name}"
  local plan_root="${build_root}/plan"

  "${worker[@]}" safe-scan \
    --source "${source_path}" \
    --source-sha256 "${source_sha256}" \
    --projection-policy "${policy_path}" \
    --projection-policy-sha256 "${policy_sha256}" \
    --output-root "${plan_root}" \
    --dataset-id "${dataset_id}" \
    --snapshot-id "${snapshot_id}" \
    --dataset-namespace "${dataset_namespace}" \
    --source-guid "${source_guid}" \
    --source-snapshot "${source_snapshot}" \
    --logical-partitions "${partition_count}" \
    --max-quads 100000 >/dev/null

  local plan_path="${plan_root}/source-plan.json"
  local plan_sha256
  plan_sha256="$(sha256sum "${plan_path}" | awk '{print $1}')"
  local projection_list="${build_root}/projection-manifests.json"
  python3 - "${projection_list}" <<'PY'
import json
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text("[]\n", encoding="utf-8")
PY

  local partition_index
  for ((partition_index=0; partition_index<partition_count; partition_index++)); do
    local projection_root="${build_root}/projection-$(printf '%05d' "${partition_index}")"
    "${worker[@]}" project-partition \
      --source-plan "${plan_path}" \
      --source-plan-sha256 "${plan_sha256}" \
      --partition-index "${partition_index}" \
      --dataset-namespace "${dataset_namespace}" \
      --source-guid "${source_guid}" \
      --source-snapshot "${source_snapshot}" \
      --projection-policy "${policy_path}" \
      --projection-policy-sha256 "${policy_sha256}" \
      --output-root "${projection_root}" \
      --max-quads 100000 >/dev/null
    python3 - "${projection_list}" "${projection_root}/projection-run.json" <<'PY'
import json
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
values = json.loads(path.read_text(encoding="utf-8"))
values.append(sys.argv[2])
path.write_text(json.dumps(values, indent=2) + "\n", encoding="utf-8")
PY
  done

  local reducer_list="${build_root}/reducer-manifests.json"
  python3 - "${reducer_list}" <<'PY'
import json
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text("[]\n", encoding="utf-8")
PY
  local reducer_index
  for ((reducer_index=0; reducer_index<reducer_count; reducer_index++)); do
    local reducer_root="${build_root}/reducer-$(printf '%05d' "${reducer_index}")"
    "${worker[@]}" reduce-range \
      --source-plan "${plan_path}" \
      --source-plan-sha256 "${plan_sha256}" \
      --projection-manifest-list "${projection_list}" \
      --reducer-index "${reducer_index}" \
      --reducer-count "${reducer_count}" \
      --output-root "${reducer_root}" >/dev/null
    python3 - "${reducer_list}" "${reducer_root}/reducer-run.json" <<'PY'
import json
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
values = json.loads(path.read_text(encoding="utf-8"))
values.append(sys.argv[2])
path.write_text(json.dumps(values, indent=2) + "\n", encoding="utf-8")
PY
  done

  "${worker[@]}" finalize-reducers \
    --source-plan "${plan_path}" \
    --source-plan-sha256 "${plan_sha256}" \
    --reducer-manifest-list "${reducer_list}" \
    --output-root "${build_root}/root" >/dev/null
}

build_layout baseline 1 1
build_layout distributed 8 3
"${worker[@]}" compare-builds \
  --baseline-root "${run_root}/baseline/root/distributed-root.json" \
  --candidate-root "${run_root}/distributed/root/distributed-root.json" \
  --report "${run_root}/build-equivalence-report.json"

echo "Phase 15 local equivalence evidence: ${run_root}"
