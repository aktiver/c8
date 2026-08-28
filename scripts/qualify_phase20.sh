#!/usr/bin/env bash
set -euo pipefail

required_variables=(
  NGKG_ONLINE_QUERY_URL
  NGKG_API_TOKEN
  NGKG_DATASET_ID
  NGKG_CERTIFIED_QUERY_FILE
  NGKG_KUBERNETES_NAMESPACE
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
[[ -f "${NGKG_CERTIFIED_QUERY_FILE}" ]] || { echo "certified query file is missing" >&2; exit 2; }

run_dir="$(mktemp -d)"
request="${run_dir}/query-request.json"
response="${run_dir}/query-response.json"
jq -n --rawfile query "${NGKG_CERTIFIED_QUERY_FILE}" \
  '{query: $query, hydrate: true}' > "${request}"
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer ${NGKG_API_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data-binary @"${request}" \
  "${NGKG_ONLINE_QUERY_URL}/v1/datasets/${NGKG_DATASET_ID}/query" > "${response}"
jq -e '
  .datasetId != null and .snapshotId != null and .complete == true and
  (.servingRootSha256 | test("^[0-9a-f]{64}$")) and
  (.querySha256 | test("^[0-9a-f]{64}$")) and
  (.bindings | type == "array") and
  (.qualifiedEntities | type == "array") and
  (.hydratedPayload | type == "array")
' "${response}" >/dev/null

for workload in statefulset/ngkg-query-shard statefulset/ngkg-locator deployment/ngkg-hydration; do
  kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" rollout status "${workload}" --timeout=10m
done
for hpa in ngkg-query-shard ngkg-hydration; do
  kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get hpa "${hpa}" -o json | jq -e '
    [.spec.metrics[] | select(.type == "Resource") | .resource.target.averageUtilization] |
    length == 2 and all(. <= 80)
  ' >/dev/null
done
kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get statefulset ngkg-query-shard -o json | jq -e '
  .spec.template.spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution != null and
  .spec.template.spec.nodeSelector["ngkg.io/workload"] == "sparql-query-processing" and
  (.spec.template.spec.containers[0].env |
    map(select(.name == "OMP_NUM_THREADS" or .name == "OPENBLAS_NUM_THREADS" or .name == "MKL_NUM_THREADS")) |
    length == 3 and all(.value == "1"))
' >/dev/null

jq -n \
  --arg snapshotId "$(jq -r .snapshotId "${response}")" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase20-certified-online-read-passed", snapshotId: $snapshotId,
    evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Run pinned Rust, PostgreSQL/S3 corruption, HermiT equivalence, Helm render, sustained 79/80 percent HPA, Rancher node-growth and node-loss gates before release."}'
