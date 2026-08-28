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
  if [[ -z "${!variable:-}" ]]; then
    echo "${variable} is required" >&2
    exit 2
  fi
done
for command in curl jq kubectl sha256sum cmp sort; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 2; }
done
for file in "${NGKG_CERTIFIED_QUERY_FILE}" "${NGKG_EXPECTED_RESULTS_FILE}"; do
  [[ -f "${file}" ]] || { echo "required qualification input is missing: ${file}" >&2; exit 2; }
done

run_dir="$(mktemp -d)"
request="${run_dir}/query-request.json"
first="${run_dir}/first-response.json"
second="${run_dir}/second-response.json"
expected_rows="${run_dir}/expected-bindings.jsonl"
first_rows="${run_dir}/first-bindings.jsonl"
second_rows="${run_dir}/second-bindings.jsonl"
first_payload="${run_dir}/first-payload.jsonl"
second_payload="${run_dir}/second-payload.jsonl"
query_sha256="$(sha256sum "${NGKG_CERTIFIED_QUERY_FILE}" | cut -d ' ' -f 1)"

jq -n --rawfile query "${NGKG_CERTIFIED_QUERY_FILE}" \
  '{query: $query, hydrate: true}' > "${request}"
for response in "${first}" "${second}"; do
  curl --fail-with-body --silent --show-error \
    -H "Authorization: Bearer ${NGKG_API_TOKEN}" \
    -H 'Content-Type: application/json' \
    --data-binary @"${request}" \
    "${NGKG_ONLINE_QUERY_URL}/v1/datasets/${NGKG_DATASET_ID}/query" > "${response}"
  jq -e --arg querySha256 "${query_sha256}" '
    .complete == true and .querySha256 == $querySha256 and
    .execution.mode == "certified_partitioned_shuffle" and
    .execution.shuffleCacheMode == "snapshot_checksum_local_nvme_v1" and
    .execution.shuffleCacheHits >= 0 and
    .execution.shuffleSpillMode == "bounded_local_nvme_v1" and
    .execution.shuffleWorkerCount >= 2 and
    (.bindings | type == "array") and
    (.hydratedPayload | length > 0)
  ' "${response}" >/dev/null
done

if [[ "$(jq -r .snapshotId "${first}")" != "$(jq -r .snapshotId "${second}")" ]] ||
   [[ "$(jq -r .execution.planSha256 "${first}")" != "$(jq -r .execution.planSha256 "${second}")" ]]; then
  echo "warm-cache qualification crossed a snapshot or plan boundary" >&2
  exit 1
fi
first_hits="$(jq -r .execution.shuffleCacheHits "${first}")"
second_hits="$(jq -r .execution.shuffleCacheHits "${second}")"
if (( second_hits < 1 || second_hits < first_hits )); then
  echo "identical second query did not reuse the immutable partition cache" >&2
  exit 1
fi

jq -S -c '.results.bindings[]' "${NGKG_EXPECTED_RESULTS_FILE}" | sort > "${expected_rows}"
jq -S -c '.bindings[]' "${first}" | sort > "${first_rows}"
jq -S -c '.bindings[]' "${second}" | sort > "${second_rows}"
cmp "${expected_rows}" "${first_rows}"
cmp "${expected_rows}" "${second_rows}"
jq -S -c '.hydratedPayload[]' "${first}" | sort > "${first_payload}"
jq -S -c '.hydratedPayload[]' "${second}" | sort > "${second_payload}"
cmp "${first_payload}" "${second_payload}"

kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get statefulset ngkg-fragment-worker -o json | jq -e '
  .spec.template.spec.nodeSelector["ngkg.io/workload"] == "sparql-fragment-processing" and
  (.spec.template.spec.containers[0].resources.requests == .spec.template.spec.containers[0].resources.limits) and
  (.spec.template.spec.containers[0].resources.requests["ephemeral-storage"] != null) and
  any(.spec.template.spec.volumes[]; .name == "shuffle-cache" and .emptyDir.sizeLimit != null) and
  (.spec.template.spec.containers[0].env |
    map(select(.name == "NGKG_SHUFFLE_CACHE_ROOT" or
               .name == "NGKG_MAX_SHUFFLE_CACHE_BYTES" or
               .name == "NGKG_MAX_SHUFFLE_CACHE_ENTRIES" or
               .name == "NGKG_MAX_SHUFFLE_CACHE_ENTRY_BYTES")) | length == 4) and
  (.spec.template.spec.containers[0].env |
    map(select(.name == "OMP_NUM_THREADS" or .name == "OPENBLAS_NUM_THREADS" or .name == "MKL_NUM_THREADS")) |
    length == 3 and all(.value == "1"))
' >/dev/null

jq -n \
  --arg snapshotId "$(jq -r .snapshotId "${second}")" \
  --arg planSha256 "$(jq -r .execution.planSha256 "${second}")" \
  --argjson firstHits "${first_hits}" \
  --argjson secondHits "${second_hits}" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "phase26-snapshot-safe-shuffle-cache-passed", snapshotId: $snapshotId,
    planSha256: $planSha256, firstHits: $firstHits, secondHits: $secondHits,
    evidenceDirectory: $evidenceDirectory,
    qualificationBoundary: "Also run pinned Rust tests, fresh-pod cold/warm tests, cache corruption and cancellation matrices, snapshot/plan invalidation, enterprise load and skew, RKE2 NVMe placement, Helm server dry-run, mTLS, and sustained 79/80 percent autoscaling before release."}'
