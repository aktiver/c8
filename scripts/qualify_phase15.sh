#!/usr/bin/env bash
set -euo pipefail

required_variables=(
  NGKG_API_URL
  NGKG_API_TOKEN
  NGKG_DATASET_ID
  NGKG_DATASET_NAMESPACE
  NGKG_POLICY_VERSION
  NGKG_BUNDLE_OBJECT_KEY
  NGKG_BUNDLE_SHA256
  NGKG_TARGET_SNAPSHOT_ID
  NGKG_IDEMPOTENCY_KEY
  NGKG_KUBERNETES_NAMESPACE
  NGKG_PHASE15_TIMEOUT_SECONDS
  NGKG_PHASE15_LOGICAL_PARTITIONS
  NGKG_PHASE15_REDUCER_COUNT
)
for variable in "${required_variables[@]}"; do
  if [[ -z "${!variable:-}" ]]; then
    echo "${variable} is required" >&2
    exit 2
  fi
done
for command in curl jq kubectl; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 2; }
done

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"${repository_root}/scripts/run_distributed_reference_slice.sh"

run_dir="$(mktemp -d)"
dataset_body="${run_dir}/dataset.json"
ingestion_body="${run_dir}/ingestion.json"
job_body="${run_dir}/job.json"

jq -n \
  --arg identityNamespace "${NGKG_DATASET_NAMESPACE}" \
  --arg policyVersion "${NGKG_POLICY_VERSION}" \
  '{identityNamespace: $identityNamespace, policyVersion: $policyVersion}' > "${dataset_body}"

parent_json="null"
if [[ -n "${NGKG_PARENT_SNAPSHOT_ID:-}" ]]; then
  parent_json="\"${NGKG_PARENT_SNAPSHOT_ID}\""
fi
jq -n \
  --arg bundleObjectKey "${NGKG_BUNDLE_OBJECT_KEY}" \
  --arg bundleSha256 "${NGKG_BUNDLE_SHA256}" \
  --arg targetSnapshotId "${NGKG_TARGET_SNAPSHOT_ID}" \
  --argjson parentSnapshotId "${parent_json}" \
  '{bundleObjectKey: $bundleObjectKey, bundleSha256: $bundleSha256,
    parentSnapshotId: $parentSnapshotId, targetSnapshotId: $targetSnapshotId,
    publicationPolicy: "manual-after-certification", resourceProfile: "distributed-hpc-v1"}' \
  > "${ingestion_body}"

auth_header="Authorization: Bearer ${NGKG_API_TOKEN}"
curl --fail-with-body --silent --show-error \
  -X PUT -H "${auth_header}" -H 'Content-Type: application/json' \
  --data-binary "@${dataset_body}" \
  "${NGKG_API_URL}/v1/datasets/${NGKG_DATASET_ID}" >/dev/null

accepted_one="${run_dir}/accepted-one.json"
accepted_two="${run_dir}/accepted-two.json"
for output in "${accepted_one}" "${accepted_two}"; do
  curl --fail-with-body --silent --show-error \
    -X POST -H "${auth_header}" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: ${NGKG_IDEMPOTENCY_KEY}" \
    --data-binary "@${ingestion_body}" \
    "${NGKG_API_URL}/v1/datasets/${NGKG_DATASET_ID}/ingestions" > "${output}"
done
operation_id="$(jq -er '.operationId' "${accepted_one}")"
test "${operation_id}" = "$(jq -er '.operationId' "${accepted_two}")"

deadline="$((SECONDS + NGKG_PHASE15_TIMEOUT_SECONDS))"
state="REGISTERED"
while (( SECONDS < deadline )); do
  curl --fail-with-body --silent --show-error \
    -H "${auth_header}" "${NGKG_API_URL}/v1/jobs/${operation_id}" > "${job_body}"
  state="$(jq -er '.operation.state' "${job_body}")"
  case "${state}" in
    CERTIFIED|PUBLISHED) break ;;
    FAILED|CANCELLED)
      jq . "${job_body}" >&2
      exit 1
      ;;
  esac
  sleep 5
done
test "${state}" = "CERTIFIED" -o "${state}" = "PUBLISHED"
jq -e --argjson partitions "${NGKG_PHASE15_LOGICAL_PARTITIONS}" \
  --argjson reducers "${NGKG_PHASE15_REDUCER_COUNT}" '
  .distributedBuild.logicalPartitionCount == $partitions and
  .distributedBuild.reducerCount == $reducers and
  .distributedBuild.succeededProjections == $partitions and
  .distributedBuild.succeededReducers == $reducers and
  .distributedBuild.failedWork == 0' "${job_body}" >/dev/null

resource_name="ngkg-${operation_id//-/}"
kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get ngkgcompilation "${resource_name}" -o json \
  > "${run_dir}/compilation-resource.json"
test "distributed-hpc-v1" = "$(jq -er '.spec.resourceProfile' "${run_dir}/compilation-resource.json")"

check_job() {
  local suffix="$1"
  local responsibility="$2"
  local completions="$3"
  local completion_mode="$4"
  local output="${run_dir}/job-${suffix}.json"
  kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get job "${resource_name}-${suffix}" -o json > "${output}"
  test "${responsibility}" = "$(jq -er '.spec.template.spec.nodeSelector["ngkg.io/workload"]' "${output}")"
  test "${completions}" = "$(jq -er '.spec.completions' "${output}")"
  test "${completion_mode}" = "$(jq -er '.spec.completionMode // "NonIndexed"' "${output}")"
  if [[ "${completion_mode}" = "Indexed" ]]; then
    test "3" = "$(jq -er '.spec.backoffLimitPerIndex' "${output}")"
    test "0" = "$(jq -er '.spec.maxFailedIndexes' "${output}")"
  fi
  jq -e '.spec.template.spec.containers[0].image | test("@sha256:[0-9a-f]{64}$")' "${output}" >/dev/null
  jq -e '.spec.template.spec.containers[0].resources as $r |
    $r.requests.cpu == $r.limits.cpu and
    $r.requests.memory == $r.limits.memory and
    $r.requests["ephemeral-storage"] == $r.limits["ephemeral-storage"]' "${output}" >/dev/null
  jq -e '.spec.template.spec.containers[0].env |
    map(select(.name == "OMP_NUM_THREADS" or .name == "OPENBLAS_NUM_THREADS" or .name == "MKL_NUM_THREADS")) |
    length == 3 and all(.value == "1")' "${output}" >/dev/null
}

check_job plan semantic-projection 1 NonIndexed
check_job project semantic-projection "${NGKG_PHASE15_LOGICAL_PARTITIONS}" Indexed
check_job reduce index-build "${NGKG_PHASE15_REDUCER_COUNT}" Indexed
check_job finalize index-build 1 NonIndexed
check_job reason reasoning 1 NonIndexed

jq -n \
  --arg operationId "${operation_id}" \
  --arg snapshotId "${NGKG_TARGET_SNAPSHOT_ID}" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase15-smoke-passed", operationId: $operationId,
    snapshotId: $snapshotId, evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "The full fault-injection and RKE2 autoscaling matrix remains a separate required release gate."}'
