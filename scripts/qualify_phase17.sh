#!/usr/bin/env bash
set -euo pipefail

required_variables=(
  NGKG_API_URL
  NGKG_API_TOKEN
  NGKG_OPERATION_ID
  NGKG_KUBERNETES_NAMESPACE
  NGKG_PHASE17_LOGICAL_PARTITIONS
  NGKG_PHASE17_ROW_GROUP_ROWS
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

run_dir="$(mktemp -d)"
job_body="${run_dir}/job.json"
auth_header="Authorization: Bearer ${NGKG_API_TOKEN}"
curl --fail-with-body --silent --show-error \
  -H "${auth_header}" \
  "${NGKG_API_URL}/v1/jobs/${NGKG_OPERATION_ID}" > "${job_body}"

jq -e \
  --argjson partitions "${NGKG_PHASE17_LOGICAL_PARTITIONS}" \
  --argjson rowGroupRows "${NGKG_PHASE17_ROW_GROUP_ROWS}" '
    (.operation.state == "CERTIFIED" or .operation.state == "PUBLISHED") and
    .distributedArtifacts.partitionCount == $partitions and
    .distributedArtifacts.rowGroupRows == $rowGroupRows and
    .distributedArtifacts.succeededArtifacts == $partitions and
    .distributedArtifacts.failedArtifacts == 0 and
    (.distributedArtifactRoot.rootManifestSha256 | test("^[0-9a-f]{64}$")) and
    (.distributedArtifactRoot.locatorSha256 | test("^[0-9a-f]{64}$")) and
    .distributedArtifactRoot.payloadRowCount ==
      .distributedArtifactRoot.locatorRecordCount and
    (.distributedArtifactRoot.semanticRowCount +
      .distributedArtifactRoot.payloadRowCount) ==
      .distributedArtifactRoot.factCount
  ' "${job_body}" >/dev/null

resource_name="ngkg-${NGKG_OPERATION_ID//-/}"

check_job() {
  local suffix="$1"
  local responsibility="$2"
  local completions="$3"
  local completion_mode="$4"
  local output="${run_dir}/job-${suffix}.json"
  kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" \
    get job "${resource_name}-${suffix}" -o json > "${output}"
  test "${responsibility}" = "$(jq -er '.spec.template.spec.nodeSelector["ngkg.io/workload"]' "${output}")"
  test "${completions}" = "$(jq -er '.spec.completions' "${output}")"
  test "${completion_mode}" = "$(jq -er '.spec.completionMode // "NonIndexed"' "${output}")"
  jq -e '.spec.template.spec.containers[0].image |
    test("@sha256:[0-9a-f]{64}$")' "${output}" >/dev/null
  jq -e '.spec.template.spec.containers[0].resources as $r |
    $r.requests.cpu == $r.limits.cpu and
    $r.requests.memory == $r.limits.memory and
    $r.requests["ephemeral-storage"] == $r.limits["ephemeral-storage"]' \
    "${output}" >/dev/null
  jq -e '.spec.template.spec.containers[0].env |
    map(select(.name == "OMP_NUM_THREADS" or
      .name == "OPENBLAS_NUM_THREADS" or .name == "MKL_NUM_THREADS")) |
    length == 3 and all(.value == "1")' "${output}" >/dev/null
}

check_job artifact-plan semantic-artifact-build 1 NonIndexed
check_job artifact semantic-artifact-build "${NGKG_PHASE17_LOGICAL_PARTITIONS}" Indexed
check_job artifact-finalize index-build 1 NonIndexed

reasoner="${run_dir}/job-reason.json"
kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" \
  get job "${resource_name}-reason" -o json > "${reasoner}"
jq -e '.spec.template.spec.containers[0].args as $args |
  ($args | index("--distributed-root-object-key")) != null and
  ($args | index("--distributed-artifact-root-object-key")) != null' \
  "${reasoner}" >/dev/null

jq -n \
  --arg operationId "${NGKG_OPERATION_ID}" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase17-smoke-passed", operationId: $operationId,
    evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Run fault injection, one-node/N-node equivalence, RLS, S3 immutability, HermiT, query, hydration, and RKE2 scale probes before release."}'
