#!/usr/bin/env bash
set -euo pipefail

required_variables=(
  NGKG_ONLINE_QUERY_POD_URL
  NGKG_API_TOKEN
  NGKG_DATASET_ID
  NGKG_CERTIFIED_SKEW_QUERY_FILE
  NGKG_EXPECTED_RESULTS_FILE
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
  -l app.kubernetes.io/component=fragment-worker \
  -o json | jq -r '.items[] | select(.status.phase == "Running") | .metadata.name' | sort)
(( ${#fragment_pods[@]} >= 2 )) || { echo "at least two running fragment pods are required" >&2; exit 2; }

collect_fragment_metrics() {
  local destination="$1"
  : > "${destination}"
  for pod in "${fragment_pods[@]}"; do
    kubectl --namespace "${namespace}" get --raw \
      "/api/v1/namespaces/${namespace}/pods/${pod}:32040/proxy/metrics" >> "${destination}"
  done
}

metric_sum() {
  local metric="$1"
  local selector="$2"
  local file="$3"
  awk -v metric="${metric}" -v selector="${selector}" '
    index($0, metric "{") == 1 && (selector == "" || index($0, selector) > 0) {sum += $2}
    END {printf "%.0f\n", sum+0}
  ' "${file}"
}

collect_fragment_metrics "${run_dir}/metrics-before.prom"
active_before="$(metric_sum ngkg_worker_join_active_spill_bytes '' "${run_dir}/metrics-before.prom")"
(( active_before == 0 )) || { echo "worker spill must be idle before qualification" >&2; exit 2; }
shuffle_entries_before="$(metric_sum ngkg_shuffle_cache_entries '' "${run_dir}/metrics-before.prom")"
(( shuffle_entries_before == 0 )) || {
  echo "qualification requires freshly started fragment pods with empty shuffle caches" >&2
  exit 2
}

curl --fail-with-body --silent --show-error \
  --output "${run_dir}/response.json" \
  --dump-header "${run_dir}/response.headers" \
  -H "Authorization: Bearer ${NGKG_API_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data-binary @"${run_dir}/request.json" \
  "${base_url}/v1/datasets/${NGKG_DATASET_ID}/query"
grep -Eiq '^x-ngkg-query-cache:[[:space:]]*miss[[:space:]]*$' "${run_dir}/response.headers"

maximum_build_rows="$(kubectl --namespace "${namespace}" get statefulset ngkg-fragment-worker -o json | jq -er '
  .spec.template.spec.containers[0].env[] |
  select(.name == "NGKG_MAX_WORKER_JOIN_BUILD_ROWS") | .value | tonumber
')"
jq -e --argjson maximumBuildRows "${maximum_build_rows}" '
  .complete == true and
  .execution.mode == "certified_partitioned_shuffle" and
  .execution.workerJoinMode == "grace_hash_nvme_v1" and
  .execution.workerJoinSpillBytes > 0 and
  .execution.workerJoinGracePartitions > 0 and
  .execution.workerJoinMaxBuildRows > 0 and
  .execution.workerJoinMaxBuildRows <= $maximumBuildRows
' "${run_dir}/response.json" >/dev/null
jq -S -c '.bindings[]' "${run_dir}/response.json" | sort > "${run_dir}/actual.jsonl"
cmp "${run_dir}/expected.jsonl" "${run_dir}/actual.jsonl"

collect_fragment_metrics "${run_dir}/metrics-after.prom"
grace_before="$(metric_sum ngkg_worker_join_executions_total 'mode="grace_hash_nvme_v1"' "${run_dir}/metrics-before.prom")"
grace_after="$(metric_sum ngkg_worker_join_executions_total 'mode="grace_hash_nvme_v1"' "${run_dir}/metrics-after.prom")"
spill_before="$(metric_sum ngkg_worker_join_spill_bytes_total '' "${run_dir}/metrics-before.prom")"
spill_after="$(metric_sum ngkg_worker_join_spill_bytes_total '' "${run_dir}/metrics-after.prom")"
active_after="$(metric_sum ngkg_worker_join_active_spill_bytes '' "${run_dir}/metrics-after.prom")"
(( grace_after > grace_before )) || { echo "no fragment worker computed a Grace join" >&2; exit 1; }
(( spill_after > spill_before )) || { echo "worker spill-byte counter did not increase" >&2; exit 1; }
(( active_after == 0 )) || { echo "worker spill allocation did not drain to zero" >&2; exit 1; }

kubectl --namespace "${namespace}" get statefulset ngkg-fragment-worker -o json | jq -e '
  (.spec.template.spec.containers[0].resources.requests == .spec.template.spec.containers[0].resources.limits) and
  (.spec.template.spec.containers[0].volumeMounts |
    any(.name == "worker-join-spill" and .mountPath == "/var/lib/ngkg/worker-join")) and
  (.spec.template.spec.volumes |
    any(.name == "worker-join-spill" and (.emptyDir.sizeLimit | length > 0))) and
  (.spec.template.spec.containers[0].env |
    map(select(.name == "NGKG_WORKER_JOIN_SPILL_ROOT" or
               .name == "NGKG_MAX_WORKER_JOIN_SPILL_BYTES" or
               .name == "NGKG_MAX_WORKER_JOIN_SPILL_BYTES_PER_REQUEST" or
               .name == "NGKG_WORKER_JOIN_BUCKETS" or
               .name == "NGKG_MAX_WORKER_JOIN_OPEN_FILES" or
               .name == "NGKG_MAX_WORKER_JOIN_BUILD_ROWS" or
               .name == "NGKG_MAX_WORKER_JOIN_PROBE_ROWS" or
               .name == "NGKG_MAX_WORKER_JOIN_ROW_BYTES" or
               .name == "NGKG_IN_MEMORY_JOIN_BUILD_ROWS")) | length == 9) and
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
  --argjson graceExecutions "$((grace_after - grace_before))" \
  --argjson spillBytes "$((spill_after - spill_before))" \
  --argjson gracePartitions "$(jq -r '.execution.workerJoinGracePartitions' "${run_dir}/response.json")" \
  --argjson maxBuildRows "$(jq -r '.execution.workerJoinMaxBuildRows' "${run_dir}/response.json")" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase30-bounded-worker-grace-join-passed", exactIndependentBagEqual: true,
    workerJoinMode: "grace_hash_nvme_v1", graceExecutions: $graceExecutions,
    spillBytes: $spillBytes, gracePartitions: $gracePartitions,
    maxBuildRows: $maxBuildRows, activeSpillBytesAfter: 0,
    evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Also run pinned Rust, corruption, cancellation, disk-full, sustained hot-key skew, RSS/ephemeral plateau, pod loss, node replacement, service-mesh and RKE2 79/80-percent scaling gates before release."}'
