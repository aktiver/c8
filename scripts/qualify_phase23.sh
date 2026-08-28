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
  .complete == true and .querySha256 == $querySha256 and
  .querySha256 == $routing[0].querySha256 and
  .head == $expected[0].head.vars and
  .routing.selectionMode == $routing[0].selectionMode and
  .routing.selectedGraphIris == $routing[0].selectedGraphIris and
  .execution.mode == "certified_distributed_fragments" and
  .execution.exchangeFormat == "arrow_ipc_stream_v1" and
  .execution.fragmentCount >= 2 and .execution.workerCount >= 2 and
  (.execution.planSha256 | test("^[0-9a-f]{64}$")) and
  (.bindings | type == "array") and
  (.qualifiedEntities | length > 0) and
  (.hydratedPayload | length > 0)
' "${response}" >/dev/null

jq -S -c '.results.bindings[]' "${NGKG_EXPECTED_RESULTS_FILE}" | sort > "${expected_rows}"
jq -S -c '.bindings[]' "${response}" | sort > "${observed_rows}"
cmp "${expected_rows}" "${observed_rows}"

for workload in statefulset/ngkg-query-shard statefulset/ngkg-fragment-worker statefulset/ngkg-locator deployment/ngkg-hydration; do
  kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" rollout status "${workload}" --timeout=10m
done
for hpa in ngkg-query-shard ngkg-fragment-worker ngkg-hydration; do
  kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get hpa "${hpa}" -o json | jq -e '
    [.spec.metrics[] | select(.type == "Resource") | .resource.target.averageUtilization] |
    length == 2 and all(. <= 80)
  ' >/dev/null
done
kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get statefulset ngkg-fragment-worker -o json | jq -e '
  .spec.template.spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution != null and
  .spec.template.spec.nodeSelector["ngkg.io/workload"] == "sparql-fragment-processing" and
  (.spec.template.spec.containers[0].resources.requests == .spec.template.spec.containers[0].resources.limits) and
  (.spec.template.spec.containers[0].env |
    map(select(.name == "NGKG_FRAGMENT_ARROW_BATCH_ROWS" or
               .name == "NGKG_FRAGMENT_ARROW_HTTP_CHUNK_BYTES" or
               .name == "NGKG_FRAGMENT_ARROW_CHANNEL_CAPACITY" or
               .name == "NGKG_MAX_DISTRIBUTED_EXCHANGE_BYTES" or
               .name == "NGKG_MAX_FRAGMENT_RESPONSE_BYTES")) | length == 5) and
  (.spec.template.spec.containers[0].env |
    map(select(.name == "OMP_NUM_THREADS" or .name == "OPENBLAS_NUM_THREADS" or .name == "MKL_NUM_THREADS")) |
    length == 3 and all(.value == "1"))
' >/dev/null

ready_workers="$(kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get pods \
  -l app.kubernetes.io/component=fragment-worker \
  -o json | jq '[.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length')"
if (( ready_workers < 2 )); then
  echo "at least two ready fragment workers are required" >&2
  exit 1
fi

jq -n \
  --arg snapshotId "$(jq -r .snapshotId "${response}")" \
  --arg querySha256 "${query_sha256}" \
  --arg planSha256 "$(jq -r .execution.planSha256 "${response}")" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase23-certified-arrow-ipc-passed", snapshotId: $snapshotId,
    querySha256: $querySha256, planSha256: $planSha256,
    exchangeFormat: "arrow_ipc_stream_v1", evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Also run pinned Rust tests, Arrow corruption and truncation tests, enterprise wire/CPU/RSS benchmarks, Helm server dry-run, sustained 79/80 percent HPA, Rancher node growth, and worker-loss fault injection before release."}'
