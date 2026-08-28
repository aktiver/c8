#!/usr/bin/env bash
set -euo pipefail

required_variables=(
  NGKG_ONLINE_QUERY_URL
  NGKG_API_TOKEN
  NGKG_DATASET_ID
  NGKG_CERTIFIED_QUERY_FILE
  NGKG_EXPECTED_RESULTS_FILE
  NGKG_KUBERNETES_NAMESPACE
)
for variable in "${required_variables[@]}"; do
  [[ -n "${!variable:-}" ]] || { echo "${variable} is required" >&2; exit 2; }
done
for command in curl jq kubectl sort cmp seq; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 2; }
done
for file in "${NGKG_CERTIFIED_QUERY_FILE}" "${NGKG_EXPECTED_RESULTS_FILE}"; do
  [[ -f "${file}" ]] || { echo "required qualification input is missing: ${file}" >&2; exit 2; }
done

concurrency="${NGKG_ADMISSION_CONCURRENCY:-64}"
[[ "${concurrency}" =~ ^[1-9][0-9]*$ ]] || { echo "NGKG_ADMISSION_CONCURRENCY must be positive" >&2; exit 2; }
run_dir="$(mktemp -d)"
request="${run_dir}/request.json"
jq -n --rawfile query "${NGKG_CERTIFIED_QUERY_FILE}" '{query: $query, hydrate: true}' > "${request}"

curl --fail --silent --show-error "${NGKG_ONLINE_QUERY_URL}/metrics" > "${run_dir}/metrics-before.prom"
for ordinal in $(seq 1 "${concurrency}"); do
  (
    curl --silent --show-error \
      --output "${run_dir}/response-${ordinal}.json" \
      --dump-header "${run_dir}/headers-${ordinal}.txt" \
      --write-out '%{http_code}' \
      -H "Authorization: Bearer ${NGKG_API_TOKEN}" \
      -H 'Content-Type: application/json' \
      --data-binary @"${request}" \
      "${NGKG_ONLINE_QUERY_URL}/v1/datasets/${NGKG_DATASET_ID}/query" \
      > "${run_dir}/status-${ordinal}"
  ) &
done
wait

successes=0
rejections=0
expected_rows="${run_dir}/expected.jsonl"
jq -S -c '.results.bindings[]' "${NGKG_EXPECTED_RESULTS_FILE}" | sort > "${expected_rows}"
for ordinal in $(seq 1 "${concurrency}"); do
  status="$(<"${run_dir}/status-${ordinal}")"
  case "${status}" in
    200)
      successes=$((successes + 1))
      jq -e '.complete == true and (.bindings | type == "array")' \
        "${run_dir}/response-${ordinal}.json" >/dev/null
      jq -S -c '.bindings[]' "${run_dir}/response-${ordinal}.json" | sort \
        > "${run_dir}/actual-${ordinal}.jsonl"
      cmp "${expected_rows}" "${run_dir}/actual-${ordinal}.jsonl"
      ;;
    429)
      rejections=$((rejections + 1))
      jq -e '.code == "ADMISSION_CAPACITY_EXHAUSTED"' \
        "${run_dir}/response-${ordinal}.json" >/dev/null
      grep -Eiq '^retry-after:[[:space:]]*1[[:space:]]*$' \
        "${run_dir}/headers-${ordinal}.txt"
      ;;
    *)
      echo "unexpected HTTP ${status} for request ${ordinal}" >&2
      exit 1
      ;;
  esac
done
(( successes >= 1 )) || { echo "no request completed successfully" >&2; exit 1; }
(( rejections >= 1 )) || {
  echo "qualification did not saturate admission; lower maxQueryInFlight or increase workload" >&2
  exit 1
}

curl --fail --silent --show-error "${NGKG_ONLINE_QUERY_URL}/metrics" > "${run_dir}/metrics-after.prom"
grep -q '^ngkg_admission_rejected_total{' "${run_dir}/metrics-after.prom"
grep -q '^ngkg_admission_service_seconds_total{' "${run_dir}/metrics-after.prom"
query_in_flight="$(awk -F' ' '/ngkg_admission_in_flight\{.*class="query"/ {value=$2} END {print value+0}' "${run_dir}/metrics-after.prom")"
[[ "${query_in_flight}" == "0" ]] || { echo "query permits did not drain" >&2; exit 1; }

kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get hpa ngkg-query-shard -o json | jq -e '
  all(.spec.metrics[] | select(.type == "Resource"); .resource.target.averageUtilization <= 80)
' >/dev/null
kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get statefulset ngkg-query-shard -o json | jq -e '
  (.spec.template.spec.containers[0].env |
    map(select(.name == "NGKG_MAX_QUERY_IN_FLIGHT" or
               .name == "NGKG_ADMISSION_WAIT_MILLISECONDS")) | length == 2)
' >/dev/null

jq -n \
  --argjson successes "${successes}" \
  --argjson rejections "${rejections}" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase27-bounded-admission-passed", successes: $successes,
    rejections: $rejections, evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Also run cancellation, slow-reader Arrow, body-limit, disconnect, multi-role saturation, RSS plateau, Prometheus scrape, NetworkPolicy, service-mesh, HPA 79/80-percent and RKE2 node-growth gates."}'
