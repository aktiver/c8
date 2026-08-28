#!/usr/bin/env bash
set -euo pipefail

# Real-cluster qualification for out-of-core fragment ingress. The supplied
# query must be certified for a distributed route and its expected SPARQL JSON
# result must be generated independently of the online execution under test.

required_variables=(
  NGKG_ONLINE_QUERY_POD_URL NGKG_API_TOKEN NGKG_DATASET_ID
  NGKG_CERTIFIED_SKEW_QUERY_FILE NGKG_EXPECTED_RESULTS_FILE
  NGKG_KUBERNETES_NAMESPACE
)
for variable in "${required_variables[@]}"; do
  [[ -n "${!variable:-}" ]] || { echo "${variable} is required" >&2; exit 2; }
done
for command in curl jq kubectl sort cmp awk grep; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 2; }
done
for file in "${NGKG_CERTIFIED_SKEW_QUERY_FILE}" "${NGKG_EXPECTED_RESULTS_FILE}"; do
  [[ -f "${file}" ]] || { echo "qualification input is missing: ${file}" >&2; exit 2; }
done

namespace="${NGKG_KUBERNETES_NAMESPACE}"
run_dir="$(mktemp -d)"
base_url="${NGKG_ONLINE_QUERY_POD_URL%/}"
jq -n --rawfile query "${NGKG_CERTIFIED_SKEW_QUERY_FILE}" \
  '{query: $query, hydrate: false}' > "${run_dir}/request.json"
jq -S -c '.results.bindings[]' "${NGKG_EXPECTED_RESULTS_FILE}" | sort > "${run_dir}/expected.jsonl"

mapfile -t query_pods < <(kubectl --namespace "${namespace}" get pods \
  -l app.kubernetes.io/component=query-shard -o json |
  jq -r '.items[] | select(.status.phase == "Running") | .metadata.name' | sort)
mapfile -t fragment_pods < <(kubectl --namespace "${namespace}" get pods \
  -l app.kubernetes.io/component=fragment-worker -o json |
  jq -r '.items[] | select(.status.phase == "Running") | .metadata.name' | sort)
(( ${#query_pods[@]} >= 1 )) || { echo "at least one running query pod is required" >&2; exit 2; }
(( ${#fragment_pods[@]} >= 2 )) || { echo "at least two running fragment pods are required" >&2; exit 2; }

collect_metrics() {
  local port="$1" destination="$2"
  shift 2
  : > "${destination}"
  local pod
  for pod in "$@"; do
    kubectl --namespace "${namespace}" get --raw \
      "/api/v1/namespaces/${namespace}/pods/${pod}:${port}/proxy/metrics" >> "${destination}"
  done
}
metric_sum() {
  local metric="$1" file="$2"
  awk -v metric="${metric}" 'index($0, metric) == 1 {sum += $2} END {printf "%.0f\n", sum+0}' "${file}"
}

collect_metrics 32010 "${run_dir}/query-before.prom" "${query_pods[@]}"
collect_metrics 32040 "${run_dir}/fragment-before.prom" "${fragment_pods[@]}"
(( $(metric_sum ngkg_fragment_response_spool_active_bytes "${run_dir}/query-before.prom") == 0 )) || {
  echo "fragment response spools must be idle before qualification" >&2; exit 2;
}
(( $(metric_sum ngkg_streaming_request_spool_active_bytes "${run_dir}/fragment-before.prom") == 0 )) || {
  echo "request spools must be idle before qualification" >&2; exit 2;
}
(( $(metric_sum ngkg_worker_join_active_spill_bytes "${run_dir}/fragment-before.prom") == 0 )) || {
  echo "Grace spools must be idle before qualification" >&2; exit 2;
}

curl --fail-with-body --silent --show-error \
  --output "${run_dir}/response.json" --dump-header "${run_dir}/response.headers" \
  -H "Authorization: Bearer ${NGKG_API_TOKEN}" -H 'Content-Type: application/json' \
  --data-binary @"${run_dir}/request.json" \
  "${base_url}/v1/datasets/${NGKG_DATASET_ID}/query"
grep -Eiq '^x-ngkg-query-cache:[[:space:]]*miss[[:space:]]*$' "${run_dir}/response.headers"
jq -e '
  .complete == true and
  (.execution.mode == "certified_distributed_fragments" or
   .execution.mode == "certified_partitioned_shuffle") and
  .execution.exchangeFormat == "arrow_ipc_stream_v1" and
  .execution.fragmentIngressMode == "streamed_nvme_spool_v1" and
  .execution.fragmentIngressBytes > 0 and
  (if .execution.mode == "certified_partitioned_shuffle" then
     .execution.coordinatorRequestMode == "streamed_from_spill_v1" and
     .execution.coordinatorRequestBytes == .execution.workerInputBytes
   else true end)
' "${run_dir}/response.json" >/dev/null
jq -S -c '.bindings[]' "${run_dir}/response.json" | sort > "${run_dir}/actual.jsonl"
cmp "${run_dir}/expected.jsonl" "${run_dir}/actual.jsonl"

collect_metrics 32010 "${run_dir}/query-after.prom" "${query_pods[@]}"
collect_metrics 32040 "${run_dir}/fragment-after.prom" "${fragment_pods[@]}"
(( $(metric_sum ngkg_fragment_response_spool_active_bytes "${run_dir}/query-after.prom") == 0 )) || {
  echo "fragment response spool allocation did not drain to zero" >&2; exit 1;
}
(( $(metric_sum ngkg_streaming_request_spool_active_bytes "${run_dir}/fragment-after.prom") == 0 )) || {
  echo "request spool allocation did not drain to zero" >&2; exit 1;
}
(( $(metric_sum ngkg_worker_join_active_spill_bytes "${run_dir}/fragment-after.prom") == 0 )) || {
  echo "Grace spill allocation did not drain to zero" >&2; exit 1;
}

kubectl --namespace "${namespace}" get statefulset ngkg-query-shard -o json | jq -e '
  (.spec.template.spec.containers[0].resources.requests == .spec.template.spec.containers[0].resources.limits) and
  (.spec.template.spec.containers[0].volumeMounts |
    any(.name == "fragment-response-spool" and .mountPath == "/var/lib/ngkg/fragment-responses")) and
  (.spec.template.spec.volumes |
    any(.name == "fragment-response-spool" and (.emptyDir.sizeLimit | length > 0))) and
  (.spec.template.spec.containers[0].env |
    any(.name == "NGKG_FRAGMENT_RESPONSE_SPOOL_ROOT" and .value == "/var/lib/ngkg/fragment-responses")) and
  (.spec.template.spec.containers[0].env |
    any(.name == "NGKG_MAX_FRAGMENT_RESPONSE_SPOOL_BYTES")) and
  (.spec.template.spec.containers[0].env |
    map(select(.name == "OMP_NUM_THREADS" or .name == "OPENBLAS_NUM_THREADS" or .name == "MKL_NUM_THREADS")) |
    length == 3 and all(.value == "1")) and
  (.spec.template.spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution | length > 0) and
  (.spec.template.spec.nodeSelector["ngkg.io/workload"] == "sparql-query-processing")
' >/dev/null
for hpa in ngkg-query-shard ngkg-fragment-worker; do
  kubectl --namespace "${namespace}" get hpa "${hpa}" -o json | jq -e '
    all(.spec.metrics[] | select(.type == "Resource"); .resource.target.averageUtilization <= 80)
  ' >/dev/null
done

jq -n \
  --argjson fragmentIngressBytes "$(jq -r '.execution.fragmentIngressBytes' "${run_dir}/response.json")" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase33-fragment-ingress-passed", exactIndependentBagEqual: true,
    fragmentIngressMode: "streamed_nvme_spool_v1",
    fragmentIngressBytes: $fragmentIngressBytes,
    fragmentResponseSpoolActiveBytesAfter: 0,
    evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Also run pinned Rust, truncation, append, corruption, disk-full, cancellation, sustained concurrency/RSS, pod-loss, Helm dry-run, service-mesh and RKE2 79/80-percent scaling gates before release."}'
