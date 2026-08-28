#!/usr/bin/env bash
set -euo pipefail

required_variables=(
  NGKG_ONLINE_QUERY_URL
  NGKG_API_TOKEN
  NGKG_DATASET_ID
  NGKG_CERTIFIED_QUERY_FILE
  NGKG_EXPECTED_RESULTS_FILE
  NGKG_EXPECTED_ROUTING_FILE
  NGKG_KUBERNETES_NAMESPACE
)
for variable in "${required_variables[@]}"; do
  if [[ -z "${!variable:-}" ]]; then
    echo "${variable} is required" >&2
    exit 2
  fi
done
for command in curl jq kubectl sha256sum cmp sort; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 2; }
done
for file in \
  "${NGKG_CERTIFIED_QUERY_FILE}" \
  "${NGKG_EXPECTED_RESULTS_FILE}" \
  "${NGKG_EXPECTED_ROUTING_FILE}"; do
  [[ -f "${file}" ]] || { echo "required qualification input is missing: ${file}" >&2; exit 2; }
done

run_dir="$(mktemp -d)"
request="${run_dir}/query-request.json"
response="${run_dir}/query-response.json"
expected_rows="${run_dir}/expected-bindings.jsonl"
observed_rows="${run_dir}/observed-bindings.jsonl"
query_sha256="$(sha256sum "${NGKG_CERTIFIED_QUERY_FILE}" | cut -d ' ' -f 1)"

jq -n --rawfile query "${NGKG_CERTIFIED_QUERY_FILE}" \
  '{query: $query, hydrate: true}' > "${request}"
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer ${NGKG_API_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data-binary @"${request}" \
  "${NGKG_ONLINE_QUERY_URL}/v1/datasets/${NGKG_DATASET_ID}/query" > "${response}"

jq -e \
  --arg querySha256 "${query_sha256}" \
  --slurpfile expected "${NGKG_EXPECTED_RESULTS_FILE}" \
  --slurpfile routing "${NGKG_EXPECTED_ROUTING_FILE}" '
  .datasetId != null and .snapshotId != null and .complete == true and
  .querySha256 == $querySha256 and
  .querySha256 == $routing[0].querySha256 and
  .head == $expected[0].head.vars and
  .routing.selectionMode == $routing[0].selectionMode and
  .routing.selectedGraphIris == $routing[0].selectedGraphIris and
  .routing.selectedGraphCount == $routing[0].selectedGraphCount and
  .routing.totalGraphCount == $routing[0].totalGraphCount and
  .routing.selectedGraphCount < .routing.totalGraphCount and
  (.routing.capabilityIndexSha256 | test("^[0-9a-f]{64}$")) and
  (.routing.routedDatasetSha256 | test("^[0-9a-f]{64}$")) and
  (.servingRootSha256 | test("^[0-9a-f]{64}$")) and
  (.bindings | type == "array") and
  (.qualifiedEntities | length > 0) and
  (.hydratedPayload | length > 0)
' "${response}" >/dev/null

jq -S -c '.results.bindings[]' "${NGKG_EXPECTED_RESULTS_FILE}" | sort > "${expected_rows}"
jq -S -c '.bindings[]' "${response}" | sort > "${observed_rows}"
cmp "${expected_rows}" "${observed_rows}"

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
  (.spec.template.spec.containers[0].env | map(select(.name == "NGKG_MAX_RESIDENT_QUERY_ROUTES")) | length == 1) and
  (.spec.template.spec.containers[0].env |
    map(select(.name == "OMP_NUM_THREADS" or .name == "OPENBLAS_NUM_THREADS" or .name == "MKL_NUM_THREADS")) |
    length == 3 and all(.value == "1"))
' >/dev/null

jq -n \
  --arg snapshotId "$(jq -r .snapshotId "${response}")" \
  --arg querySha256 "${query_sha256}" \
  --arg routedDatasetSha256 "$(jq -r .routing.routedDatasetSha256 "${response}")" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase21-certified-relevant-graph-routing-passed", snapshotId: $snapshotId,
    querySha256: $querySha256, routedDatasetSha256: $routedDatasetSha256,
    evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Also run pinned Rust tests, real HermiT corpus compilation, object corruption, Helm server dry-run, sustained 79/80 percent HPA, Rancher node growth and node-loss recovery before release."}'
