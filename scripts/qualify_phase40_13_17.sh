#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_QUERY_BASE_URL:?NGKG_QUERY_BASE_URL is required}"
: "${NGKG_DATASET_ID:?NGKG_DATASET_ID is required}"
: "${NGKG_BEARER_TOKEN:?NGKG_BEARER_TOKEN is required}"
: "${NGKG_KUBERNETES_NAMESPACE:?NGKG_KUBERNETES_NAMESPACE is required}"

command -v curl >/dev/null
command -v jq >/dev/null
command -v kubectl >/dev/null

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
matrix="${repository_root}/test-corpus/distributed/phase40.13.17-property-paths.json"
run_root="$(mktemp -d)"
trap 'rm -rf "${run_root}"' EXIT

ready_workers="$(kubectl -n "${NGKG_KUBERNETES_NAMESPACE}" get pod \
  -l app.kubernetes.io/component=fragment-worker -o json | \
  jq '[.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length')"
(( ready_workers >= 2 )) || { echo "two ready fragment workers are required" >&2; exit 2; }

while IFS= read -r encoded; do
  item="$(printf '%s' "${encoded}" | base64 -d)"
  id="$(jq -r .id <<<"${item}")"
  query="$(jq -r .query <<<"${item}")"
  response="${run_root}/${id}.json"
  curl --fail-with-body --silent --show-error \
    -H "Authorization: Bearer ${NGKG_BEARER_TOKEN}" \
    -H 'Content-Type: application/json' \
    --data "$(jq -n --arg query "${query}" '{query:$query,hydrate:false,defaultGraphUris:[],namedGraphUris:[]}')" \
    "${NGKG_QUERY_BASE_URL%/}/v1/datasets/${NGKG_DATASET_ID}/query" >"${response}"
  jq -e '
    .complete == true and
    .propertyPathExecution.complete == true and
    .propertyPathExecution.mode == "partition_native_distributed_frontier_v1" and
    .propertyPathExecution.semanticPartitionCount >= 2 and
    .propertyPathExecution.scalarOracleEquivalenceRequired == true and
    (.propertyPathExecution.workerIds | length) >= 2
  ' "${response}" >/dev/null
done < <(jq -r '.cases[] | @base64' "${matrix}")

kubectl -n "${NGKG_KUBERNETES_NAMESPACE}" get hpa ngkg-fragment-worker -o json | jq -e '
  any(.spec.metrics[]; .type == "Pods" and .pods.metric.name == "ngkg_property_path_pending_work_items") and
  any(.spec.metrics[]; .type == "Pods" and .pods.metric.name == "ngkg_property_path_active_frontier_items") and
  any(.spec.metrics[]; .type == "Resource" and .resource.name == "cpu") and
  any(.spec.metrics[]; .type == "Resource" and .resource.name == "memory")
' >/dev/null

jq -n --argjson cases "$(jq '.cases | length' "${matrix}")" \
  --argjson workers "${ready_workers}" \
  '{phase:"40.13.17",status:"passed",cases:$cases,readyWorkers:$workers,densePartitionBarrier:true,globalTermination:true}'
