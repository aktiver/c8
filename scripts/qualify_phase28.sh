#!/usr/bin/env bash
set -euo pipefail

required_variables=(
  NGKG_ONLINE_QUERY_URL
  NGKG_TENANT_A_API_TOKEN
  NGKG_TENANT_A_DATASET_ID
  NGKG_TENANT_A_CERTIFIED_QUERY_FILE
  NGKG_TENANT_A_EXPECTED_RESULTS_FILE
  NGKG_TENANT_B_API_TOKEN
  NGKG_TENANT_B_DATASET_ID
  NGKG_TENANT_B_CERTIFIED_QUERY_FILE
  NGKG_TENANT_B_EXPECTED_RESULTS_FILE
  NGKG_KUBERNETES_NAMESPACE
)
for variable in "${required_variables[@]}"; do
  [[ -n "${!variable:-}" ]] || { echo "${variable} is required" >&2; exit 2; }
done
for command in curl jq kubectl sort cmp seq awk; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 2; }
done
for file in \
  "${NGKG_TENANT_A_CERTIFIED_QUERY_FILE}" \
  "${NGKG_TENANT_A_EXPECTED_RESULTS_FILE}" \
  "${NGKG_TENANT_B_CERTIFIED_QUERY_FILE}" \
  "${NGKG_TENANT_B_EXPECTED_RESULTS_FILE}"; do
  [[ -f "${file}" ]] || { echo "required qualification input is missing: ${file}" >&2; exit 2; }
done

tenant_a_concurrency="${NGKG_TENANT_A_ADMISSION_CONCURRENCY:-64}"
tenant_b_requests="${NGKG_TENANT_B_REQUESTS:-8}"
[[ "${tenant_a_concurrency}" =~ ^[1-9][0-9]*$ ]] || { echo "tenant A concurrency must be positive" >&2; exit 2; }
[[ "${tenant_b_requests}" =~ ^[1-9][0-9]*$ ]] || { echo "tenant B request count must be positive" >&2; exit 2; }

run_dir="$(mktemp -d)"
request_a="${run_dir}/request-a.json"
request_b="${run_dir}/request-b.json"
jq -n --rawfile query "${NGKG_TENANT_A_CERTIFIED_QUERY_FILE}" '{query: $query, hydrate: true}' > "${request_a}"
jq -n --rawfile query "${NGKG_TENANT_B_CERTIFIED_QUERY_FILE}" '{query: $query, hydrate: true}' > "${request_b}"
jq -S -c '.results.bindings[]' "${NGKG_TENANT_A_EXPECTED_RESULTS_FILE}" | sort > "${run_dir}/expected-a.jsonl"
jq -S -c '.results.bindings[]' "${NGKG_TENANT_B_EXPECTED_RESULTS_FILE}" | sort > "${run_dir}/expected-b.jsonl"

curl --fail --silent --show-error "${NGKG_ONLINE_QUERY_URL}/metrics" > "${run_dir}/metrics-before.prom"

for ordinal in $(seq 1 "${tenant_a_concurrency}"); do
  (
    curl --silent --show-error \
      --output "${run_dir}/a-response-${ordinal}.json" \
      --dump-header "${run_dir}/a-headers-${ordinal}.txt" \
      --write-out '%{http_code}' \
      -H "Authorization: Bearer ${NGKG_TENANT_A_API_TOKEN}" \
      -H 'Content-Type: application/json' \
      --data-binary @"${request_a}" \
      "${NGKG_ONLINE_QUERY_URL}/v1/datasets/${NGKG_TENANT_A_DATASET_ID}/query" \
      > "${run_dir}/a-status-${ordinal}"
  ) &
done

for ordinal in $(seq 1 "${tenant_b_requests}"); do
  curl --silent --show-error \
    --output "${run_dir}/b-response-${ordinal}.json" \
    --dump-header "${run_dir}/b-headers-${ordinal}.txt" \
    --write-out '%{http_code}' \
    -H "Authorization: Bearer ${NGKG_TENANT_B_API_TOKEN}" \
    -H 'Content-Type: application/json' \
    --data-binary @"${request_b}" \
    "${NGKG_ONLINE_QUERY_URL}/v1/datasets/${NGKG_TENANT_B_DATASET_ID}/query" \
    > "${run_dir}/b-status-${ordinal}"
done
wait

tenant_a_successes=0
tenant_a_rejections=0
tenant_a_tenant_rejections=0
for ordinal in $(seq 1 "${tenant_a_concurrency}"); do
  status="$(<"${run_dir}/a-status-${ordinal}")"
  case "${status}" in
    200)
      tenant_a_successes=$((tenant_a_successes + 1))
      jq -S -c '.bindings[]' "${run_dir}/a-response-${ordinal}.json" | sort > "${run_dir}/a-actual-${ordinal}.jsonl"
      cmp "${run_dir}/expected-a.jsonl" "${run_dir}/a-actual-${ordinal}.jsonl"
      ;;
    429)
      tenant_a_rejections=$((tenant_a_rejections + 1))
      code="$(jq -r '.code' "${run_dir}/a-response-${ordinal}.json")"
      [[ "${code}" == "TENANT_ADMISSION_CAPACITY_EXHAUSTED" || "${code}" == "ADMISSION_CAPACITY_EXHAUSTED" ]] || {
        echo "unexpected tenant A rejection code ${code}" >&2; exit 1;
      }
      if [[ "${code}" == "TENANT_ADMISSION_CAPACITY_EXHAUSTED" ]]; then
        tenant_a_tenant_rejections=$((tenant_a_tenant_rejections + 1))
      fi
      grep -Eiq '^retry-after:[[:space:]]*1[[:space:]]*$' "${run_dir}/a-headers-${ordinal}.txt"
      ;;
    *) echo "unexpected tenant A HTTP ${status}" >&2; exit 1 ;;
  esac
done
(( tenant_a_successes >= 1 )) || { echo "tenant A produced no exact success" >&2; exit 1; }
(( tenant_a_rejections >= 1 )) || { echo "tenant A did not saturate its tenant lane" >&2; exit 1; }
(( tenant_a_tenant_rejections >= 1 )) || { echo "tenant A produced no tenant-scoped rejection" >&2; exit 1; }

for ordinal in $(seq 1 "${tenant_b_requests}"); do
  status="$(<"${run_dir}/b-status-${ordinal}")"
  [[ "${status}" == "200" ]] || { echo "tenant B was starved with HTTP ${status}" >&2; exit 1; }
  jq -S -c '.bindings[]' "${run_dir}/b-response-${ordinal}.json" | sort > "${run_dir}/b-actual-${ordinal}.jsonl"
  cmp "${run_dir}/expected-b.jsonl" "${run_dir}/b-actual-${ordinal}.jsonl"
done

curl --fail --silent --show-error "${NGKG_ONLINE_QUERY_URL}/metrics" > "${run_dir}/metrics-after.prom"
tenant_scope_rejections="$(awk -F' ' '/ngkg_admission_rejections_by_scope_total\{.*class="query".*scope="tenant"/ {value=$2} END {print value+0}' "${run_dir}/metrics-after.prom")"
(( tenant_scope_rejections >= tenant_a_tenant_rejections )) || { echo "tenant-scoped metric under-reported rejection evidence" >&2; exit 1; }
query_in_flight="$(awk -F' ' '/ngkg_admission_in_flight\{.*class="query"/ {value=$2} END {print value+0}' "${run_dir}/metrics-after.prom")"
[[ "${query_in_flight}" == "0" ]] || { echo "query permits did not drain" >&2; exit 1; }
grep -q '^ngkg_tenant_admission_configured{' "${run_dir}/metrics-after.prom"

kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get statefulset ngkg-query-shard -o json | jq -e '
  (.spec.template.spec.containers[0].env |
    map(select(.name == "NGKG_TENANT_ADMISSION_POLICY_FILE" or
               .name == "NGKG_TENANT_ADMISSION_POLICY_SHA256" or
               .name == "NGKG_AUTH_TOKENS_FILE_SHA256" or
               .name == "NGKG_MAX_ADMISSION_TENANTS")) | length == 4) and
  (.spec.template.metadata.annotations["ngkg.io/auth-tokens-file-sha256"] | test("^[0-9a-f]{64}$")) and
  (.spec.template.metadata.annotations["ngkg.io/tenant-admission-policy-sha256"] | test("^[0-9a-f]{64}$"))
' >/dev/null
kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get hpa ngkg-query-shard -o json | jq -e '
  all(.spec.metrics[] | select(.type == "Resource"); .resource.target.averageUtilization <= 80)
' >/dev/null

jq -n \
  --argjson tenantASuccesses "${tenant_a_successes}" \
  --argjson tenantARejections "${tenant_a_rejections}" \
  --argjson tenantATenantRejections "${tenant_a_tenant_rejections}" \
  --argjson tenantBSuccesses "${tenant_b_requests}" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase28-tenant-isolation-passed", tenantASuccesses: $tenantASuccesses,
    tenantARejections: $tenantARejections, tenantATenantRejections: $tenantATenantRejections,
    tenantBSuccesses: $tenantBSuccesses,
    evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Also run multi-pod imbalance, policy-rollout, slow-reader, cancellation, cross-tenant authorization, RSS plateau, service-mesh, HPA and RKE2 node-growth gates."}'
