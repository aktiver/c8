#!/usr/bin/env bash
set -euo pipefail

required_variables=(
  NGKG_API_URL
  NGKG_API_TOKEN
  NGKG_OPERATION_ID
  NGKG_KUBERNETES_NAMESPACE
  NGKG_PHASE19_CERTIFIED_QUERY_COUNT
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
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer ${NGKG_API_TOKEN}" \
  "${NGKG_API_URL}/v1/jobs/${NGKG_OPERATION_ID}" > "${job_body}"

jq -e --argjson queryCount "${NGKG_PHASE19_CERTIFIED_QUERY_COUNT}" '
  (.operation.state == "CERTIFIED" or .operation.state == "PUBLISHED") and
  (.distributedServingRoot.servingRootSha256 | test("^[0-9a-f]{64}$")) and
  (.distributedServingRoot.binaryLocatorSha256 | test("^[0-9a-f]{64}$")) and
  .distributedServingRoot.locatorRecordCount ==
    .distributedArtifactRoot.locatorRecordCount and
  .distributedServingRoot.partitionCount ==
    .distributedArtifacts.partitionCount and
  .distributedServingRoot.rowGroupRows ==
    .distributedArtifacts.rowGroupRows and
  (.distributedServingCertification.reportSha256 | test("^[0-9a-f]{64}$")) and
  .distributedServingCertification.servingRootSha256 ==
    .distributedServingRoot.servingRootSha256 and
  .distributedServingCertification.binaryLocatorSha256 ==
    .distributedServingRoot.binaryLocatorSha256 and
  .distributedServingCertification.certifiedQueryCount == $queryCount and
  .distributedServingCertification.referenceManifestSha256 != null
' "${job_body}" >/dev/null

resource_name="ngkg-${NGKG_OPERATION_ID//-/}"
serving="${run_dir}/job-serving-root.json"
reasoner="${run_dir}/job-reason.json"
kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" \
  get job "${resource_name}-serving-root" -o json > "${serving}"
kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" \
  get job "${resource_name}-reason" -o json > "${reasoner}"

jq -e '
  .spec.completions == 1 and .spec.parallelism == 1 and
  .spec.template.spec.nodeSelector["ngkg.io/workload"] == "index-build" and
  (.spec.template.spec.containers[0].args | index("prepare-serving-root-object-store")) != null
' "${serving}" >/dev/null
jq -e --arg threads "$(jq -r '.spec.template.spec.containers[0].args as $a | $a[($a | index("--hydration-worker-threads")) + 1]' "${reasoner}")" '
  .spec.template.spec.nodeSelector["ngkg.io/workload"] == "reasoning" and
  (.spec.template.spec.containers[0].args | index("--distributed-serving-root-object-key")) != null and
  ($threads | tonumber) > 0 and
  (.spec.template.spec.containers[0].env |
    map(select(.name == "OMP_NUM_THREADS" or .name == "OPENBLAS_NUM_THREADS" or .name == "MKL_NUM_THREADS")) |
    length == 3 and all(.value == "1"))
' "${reasoner}" >/dev/null

jq -n \
  --arg operationId "${NGKG_OPERATION_ID}" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase19-serving-root-smoke-passed", operationId: $operationId,
    evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Run PostgreSQL/S3 immutability, one-thread/N-thread, one-node/N-node, corruption, node-loss, RKE2 autoscaling and independent reference equivalence gates before release."}'
