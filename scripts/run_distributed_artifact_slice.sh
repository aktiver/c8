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
run_root="${NGKG_DISTRIBUTED_ARTIFACT_OUTPUT_ROOT:-$(mktemp -d)}"
phase15_root="${run_root}/phase15"
mkdir -p "${run_root}"

NGKG_DISTRIBUTED_OUTPUT_ROOT="${phase15_root}" scripts/run_distributed_reference_slice.sh >/dev/null

source_plan="${phase15_root}/distributed/plan/source-plan.json"
dictionary="${phase15_root}/distributed/root/dictionary.tsv"
policy="${repository_root}/test-corpus/reference/projection-policy.json"
source_plan_sha256="$(sha256sum "${source_plan}" | awk '{print $1}')"
dictionary_sha256="$(sha256sum "${dictionary}" | awk '{print $1}')"
policy_sha256="$(sha256sum "${policy}" | awk '{print $1}')"
source_sha256="$(sha256sum "${repository_root}/test-corpus/datasets/cross-domain.trig" | awk '{print $1}')"
worker=("${repository_root}/target/debug/ngkg-distributed-worker")
matrix="${repository_root}/test-corpus/distributed/artifact-equivalence-v1.json"
partition_count="$(python3 - "${matrix}" <<'PY'
import json
import pathlib
import sys
print(json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))["logicalPartitionCount"])
PY
)"

build_artifacts() {
  local execution_name="$1"
  local order="$2"
  local execution_root="${run_root}/${execution_name}"
  local manifest_list="${execution_root}/artifact-manifests.json"
  mkdir -p "${execution_root}"
  python3 - "${manifest_list}" <<'PY'
import json
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text(json.dumps([], indent=2) + "\n", encoding="utf-8")
PY

  local partition_index
  for partition_index in ${order}; do
    local output="${execution_root}/partition-$(printf '%05d' "${partition_index}")"
    "${worker[@]}" materialize-artifact-partition \
      --source-plan "${source_plan}" \
      --source-plan-sha256 "${source_plan_sha256}" \
      --dictionary "${dictionary}" \
      --dictionary-sha256 "${dictionary_sha256}" \
      --partition-index "${partition_index}" \
      --dataset-namespace 7b8e1c18-9c22-5b58-a2b4-7cbf21cc9b2b \
      --source-guid d2cfd1d5-5f83-5bb3-9888-f3bc3229a760 \
      --source-snapshot reference-corpus-v1 \
      --source-sha256 "${source_sha256}" \
      --projection-policy "${policy}" \
      --projection-policy-sha256 "${policy_sha256}" \
      --output-root "${output}" \
      --max-quads 100000 \
      --row-group-rows 65536 >/dev/null
    python3 - "${manifest_list}" "${output}/artifact-partition.json" <<'PY'
import json
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
values = json.loads(path.read_text(encoding="utf-8"))
values.append(sys.argv[2])
path.write_text(json.dumps(values, indent=2) + "\n", encoding="utf-8")
PY
  done

  "${worker[@]}" finalize-artifact-partitions \
    --source-plan "${source_plan}" \
    --source-plan-sha256 "${source_plan_sha256}" \
    --dictionary "${dictionary}" \
    --dictionary-sha256 "${dictionary_sha256}" \
    --artifact-manifest-list "${manifest_list}" \
    --output-root "${execution_root}/root" >/dev/null
}

forward_order="$(python3 - "${matrix}" <<'PY'
import json
import pathlib
import sys
value = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
print(" ".join(str(index) for index in value["executions"][0]["partitionOrder"]))
PY
)"
reverse_order="$(python3 - "${matrix}" <<'PY'
import json
import pathlib
import sys
value = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
print(" ".join(str(index) for index in value["executions"][1]["partitionOrder"]))
PY
)"
test "$(wc -w <<<"${forward_order}")" -eq "${partition_count}"
test "$(wc -w <<<"${reverse_order}")" -eq "${partition_count}"
build_artifacts forward "${forward_order}"
build_artifacts reverse "${reverse_order}"

"${worker[@]}" compare-artifact-roots \
  --baseline-root "${run_root}/forward/root/distributed-artifact-root.json" \
  --candidate-root "${run_root}/reverse/root/distributed-artifact-root.json" \
  --report "${run_root}/artifact-equivalence-report.json"

echo "Phase 16 local artifact equivalence evidence: ${run_root}"
