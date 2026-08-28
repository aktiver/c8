#!/usr/bin/env bash
set -euo pipefail

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

mapfile -t fragment_pods < <(kubectl --namespace "${namespace}" get pods \
  -l app.kubernetes.io/component=fragment-worker -o json |
  jq -r '.items[] | select(.status.phase == "Running") | .metadata.name' | sort)
(( ${#fragment_pods[@]} >= 2 )) || { echo "at least two running fragment pods are required" >&2; exit 2; }

collect_metrics() {
  local destination="$1"
  : > "${destination}"
  for pod in "${fragment_pods[@]}"; do
    kubectl --namespace "${namespace}" get --raw \
      "/api/v1/namespaces/${namespace}/pods/${pod}:32040/proxy/metrics" >> "${destination}"
  done
}
metric_sum() {
  local metric="$1" file="$2"
  awk -v metric="${metric}" 'index($0, metric) == 1 {sum += $2} END {printf "%.0f\n", sum+0}' "${file}"
}

collect_metrics "${run_dir}/metrics-before.prom"
(( $(metric_sum ngkg_streaming_request_spool_active_bytes "${run_dir}/metrics-before.prom") == 0 )) || {
  echo "request spools must be idle before qualification" >&2; exit 2;
}
(( $(metric_sum ngkg_worker_join_active_spill_bytes "${run_dir}/metrics-before.prom") == 0 )) || {
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
  .execution.mode == "certified_partitioned_shuffle" and
  .execution.workerInputMode == "streamed_spool_v1" and
  .execution.workerInputBytes > 0 and
  .execution.workerJoinMode == "grace_hash_nvme_v1" and
  .execution.workerJoinSpillBytes > 0 and
  .execution.workerJoinGracePartitions > 0
' "${run_dir}/response.json" >/dev/null
jq -S -c '.bindings[]' "${run_dir}/response.json" | sort > "${run_dir}/actual.jsonl"
cmp "${run_dir}/expected.jsonl" "${run_dir}/actual.jsonl"

collect_metrics "${run_dir}/metrics-after.prom"
(( $(metric_sum ngkg_streaming_request_spool_active_bytes "${run_dir}/metrics-after.prom") == 0 )) || {
  echo "request spool allocation did not drain to zero" >&2; exit 1;
}
(( $(metric_sum ngkg_worker_join_active_spill_bytes "${run_dir}/metrics-after.prom") == 0 )) || {
  echo "Grace spill allocation did not drain to zero" >&2; exit 1;
}

kubectl --namespace "${namespace}" get statefulset ngkg-fragment-worker -o json | jq -e '
  (.spec.template.spec.containers[0].resources.requests == .spec.template.spec.containers[0].resources.limits) and
  (.spec.template.spec.containers[0].volumeMounts |
    any(.name == "streaming-request-spool" and .mountPath == "/var/lib/ngkg/request-spool")) and
  (.spec.template.spec.volumes |
    any(.name == "streaming-request-spool" and (.emptyDir.sizeLimit | length > 0))) and
  (.spec.template.spec.containers[0].env |
    any(.name == "NGKG_STREAMING_REQUEST_SPOOL_ROOT" and .value == "/var/lib/ngkg/request-spool")) and
  (.spec.template.spec.containers[0].env |
    any(.name == "NGKG_MAX_STREAMING_REQUEST_SPOOL_BYTES")) and
  (.spec.template.spec.containers[0].env |
    map(select(.name == "OMP_NUM_THREADS" or .name == "OPENBLAS_NUM_THREADS" or .name == "MKL_NUM_THREADS")) |
    length == 3 and all(.value == "1")) and
  (.spec.template.spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution | length > 0) and
  (.spec.template.spec.nodeSelector["ngkg.io/workload"] == "sparql-fragment-processing")
' >/dev/null
for hpa in ngkg-query-shard ngkg-fragment-worker; do
  kubectl --namespace "${namespace}" get hpa "${hpa}" -o json | jq -e '
    all(.spec.metrics[] | select(.type == "Resource"); .resource.target.averageUtilization <= 80)
  ' >/dev/null
done

jq -n \
  --argjson streamedInputBytes "$(jq -r '.execution.workerInputBytes' "${run_dir}/response.json")" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase31-streamed-worker-shuffle-passed", exactIndependentBagEqual: true,
    workerInputMode: "streamed_spool_v1", streamedInputBytes: $streamedInputBytes,
    requestSpoolActiveBytesAfter: 0, graceSpillActiveBytesAfter: 0,
    evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Also run pinned Rust, chunk fragmentation, false-length, EOS corruption, cancellation, disk-full, sustained concurrency/RSS, pod-loss, Helm dry-run, service-mesh and RKE2 79/80-percent scaling gates before release."}'
