#!/usr/bin/env bash
set -euo pipefail

required_variables=(
  NGKG_ONLINE_QUERY_POD_URL
  NGKG_API_TOKEN
  NGKG_DATASET_ID
  NGKG_CERTIFIED_QUERY_FILE
  NGKG_EXPECTED_RESULTS_FILE
  NGKG_KUBERNETES_NAMESPACE
)
for variable in "${required_variables[@]}"; do
  [[ -n "${!variable:-}" ]] || { echo "${variable} is required" >&2; exit 2; }
done
for command in curl jq kubectl sort cmp awk grep; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 2; }
done
for file in "${NGKG_CERTIFIED_QUERY_FILE}" "${NGKG_EXPECTED_RESULTS_FILE}"; do
  [[ -f "${file}" ]] || { echo "required qualification input is missing: ${file}" >&2; exit 2; }
done

# This URL must address one freshly started query pod (normally through
# kubectl port-forward), not the load-balanced Service. That makes the cold
# miss and subsequent local-NVMe hit deterministic.
base_url="${NGKG_ONLINE_QUERY_POD_URL%/}"
run_dir="$(mktemp -d)"
for mode in semantic hydrated; do
  if [[ "${mode}" == "hydrated" ]]; then hydrate=true; else hydrate=false; fi
  jq -n \
    --rawfile query "${NGKG_CERTIFIED_QUERY_FILE}" \
    --argjson hydrate "${hydrate}" \
    '{query: $query, hydrate: $hydrate}' > "${run_dir}/request-${mode}.json"
done
jq -S -c '.results.bindings[]' "${NGKG_EXPECTED_RESULTS_FILE}" | sort > "${run_dir}/expected.jsonl"

curl --fail --silent --show-error "${base_url}/metrics" > "${run_dir}/metrics-before.prom"
entries_before="$(awk '/^ngkg_query_cache_entries\{/ {value=$2} END {print value+0}' "${run_dir}/metrics-before.prom")"
[[ "${entries_before}" == "0" ]] || {
  echo "qualification requires a freshly started query pod with an empty result cache" >&2
  exit 2
}

query_url="${base_url}/v1/datasets/${NGKG_DATASET_ID}/query"
for mode in semantic hydrated; do
  for attempt in first second; do
    curl --fail --silent --show-error \
      --output "${run_dir}/${mode}-${attempt}.json" \
      --dump-header "${run_dir}/${mode}-${attempt}.headers" \
      -H "Authorization: Bearer ${NGKG_API_TOKEN}" \
      -H 'Content-Type: application/json' \
      --data-binary @"${run_dir}/request-${mode}.json" \
      "${query_url}"
    jq -e '.complete == true' "${run_dir}/${mode}-${attempt}.json" >/dev/null
    jq -S -c '.bindings[]' "${run_dir}/${mode}-${attempt}.json" | sort > "${run_dir}/${mode}-${attempt}.jsonl"
    cmp "${run_dir}/expected.jsonl" "${run_dir}/${mode}-${attempt}.jsonl"
  done
  grep -Eiq '^x-ngkg-query-cache:[[:space:]]*miss[[:space:]]*$' "${run_dir}/${mode}-first.headers"
  grep -Eiq '^x-ngkg-query-cache:[[:space:]]*hit[[:space:]]*$' "${run_dir}/${mode}-second.headers"
  cmp "${run_dir}/${mode}-first.json" "${run_dir}/${mode}-second.json"
done
jq -e '.hydratedPayload | length == 0' "${run_dir}/semantic-first.json" >/dev/null

curl --fail --silent --show-error "${base_url}/metrics" > "${run_dir}/metrics-after.prom"
metric_value() {
  local outcome="$1"
  local file="$2"
  awk -v outcome="${outcome}" '
    $0 ~ "^ngkg_query_cache_events_total\\{.*outcome=\"" outcome "\".*\\}" {value=$2}
    END {print value+0}
  ' "${file}"
}
hits_before="$(metric_value hit "${run_dir}/metrics-before.prom")"
hits_after="$(metric_value hit "${run_dir}/metrics-after.prom")"
misses_before="$(metric_value miss "${run_dir}/metrics-before.prom")"
misses_after="$(metric_value miss "${run_dir}/metrics-after.prom")"
(( hits_after - hits_before == 2 )) || { echo "query-cache hit metric delta is not two" >&2; exit 1; }
(( misses_after - misses_before == 2 )) || { echo "query-cache miss metric delta is not two" >&2; exit 1; }
entries_after="$(awk '/^ngkg_query_cache_entries\{/ {value=$2} END {print value+0}' "${run_dir}/metrics-after.prom")"
bytes_after="$(awk '/^ngkg_query_cache_bytes\{/ {value=$2} END {print value+0}' "${run_dir}/metrics-after.prom")"
(( entries_after == 2 && bytes_after > 160 )) || { echo "query-cache bounded usage evidence is invalid" >&2; exit 1; }

kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get statefulset ngkg-query-shard -o json | jq -e '
  (.spec.template.spec.containers[0].env |
    map(select(.name == "NGKG_QUERY_RESULT_CACHE_ROOT" or
               .name == "NGKG_MAX_QUERY_RESULT_CACHE_BYTES" or
               .name == "NGKG_MAX_QUERY_RESULT_CACHE_ENTRIES" or
               .name == "NGKG_MAX_QUERY_RESULT_CACHE_ENTRY_BYTES")) | length == 4) and
  (.spec.template.spec.containers[0].volumeMounts |
    any(.name == "query-result-cache" and .mountPath == "/var/lib/ngkg/query-result-cache")) and
  (.spec.template.spec.volumes |
    any(.name == "query-result-cache" and (.emptyDir.sizeLimit | length > 0))) and
  (.spec.template.spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution | length > 0) and
  (.spec.template.spec.nodeSelector["ngkg.io/workload"] == "sparql-query-processing")
' >/dev/null
kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get hpa ngkg-query-shard -o json | jq -e '
  all(.spec.metrics[] | select(.type == "Resource"); .resource.target.averageUtilization <= 80)
' >/dev/null

jq -n \
  --argjson cacheEntries "${entries_after}" \
  --argjson cacheBytes "${bytes_after}" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase29-certified-query-cache-passed", semanticSequence: ["miss", "hit"],
    hydratedSequence: ["miss", "hit"], completeResponseBytesIdenticalWithinMode: true,
    hydrationModesUseDistinctEntries: true, independentExpectedBagEqual: true,
    cacheEntries: $cacheEntries, cacheBytes: $cacheBytes, evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Also run corruption, key-separation, high-churn LRU, identical-miss single-flight, RSS/ephemeral plateau, pod/node replacement, authorization, service-mesh and RKE2 79/80-percent scaling gates."}'
