#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_CONTEXT_URL:?broker base URL is required}"
: "${NGKG_CONTEXT_OAUTH_TOKEN:?OAuth/delegation bearer is required}"
: "${NGKG_CONTEXT_DATASET_ID:?dataset ID is required}"
: "${NGKG_CONTEXT_SNAPSHOT_ID:?snapshot ID is required}"
: "${NGKG_CONTEXT_GRAPH_SET_SHA256:?authorized graph-set hash is required}"
: "${NGKG_CONTEXT_SEMANTIC_RESULT_SHA256:?semantic result hash is required}"

work="$(mktemp -d)"
trap 'rm -rf -- "${work}"' EXIT
payload="${work}/context.nt"
printf '<urn:s> <urn:p> <urn:o> .\n' >"${payload}"
bytes="$(wc -c <"${payload}" | tr -d ' ')"
digest="$(sha256sum "${payload}" | cut -d' ' -f1)"
auth="Authorization: Bearer ${NGKG_CONTEXT_OAUTH_TOKEN}"

curl --fail-with-body --silent --show-error -H "${auth}" -H 'content-type: application/json' \
  -d "{\"datasetId\":\"${NGKG_CONTEXT_DATASET_ID}\",\"snapshotId\":\"${NGKG_CONTEXT_SNAPSHOT_ID}\",\"authorizedGraphSetSha256\":\"${NGKG_CONTEXT_GRAPH_SET_SHA256}\",\"semanticResultSha256\":\"${NGKG_CONTEXT_SEMANTIC_RESULT_SHA256}\",\"mediaType\":\"application/n-triples\",\"chunkSizeBytes\":65536,\"expectedTotalBytes\":${bytes},\"totalTriples\":1,\"ttlSeconds\":300}" \
  "${NGKG_CONTEXT_URL%/}/v1/context-slices" >"${work}/created.json"
slice_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["sliceId"])' <"${work}/created.json")"

curl --fail-with-body --silent --show-error -X PUT -H "${auth}" -H "X-NGKG-Content-SHA256: ${digest}" \
  --data-binary @"${payload}" "${NGKG_CONTEXT_URL%/}/v1/context-slices/${slice_id}/chunks/0"
curl --fail-with-body --silent --show-error -H "${auth}" -H 'content-type: application/json' \
  -d "{\"contentSha256\":\"${digest}\"}" "${NGKG_CONTEXT_URL%/}/v1/context-slices/${slice_id}/finalize" >"${work}/final.json"
test "$(python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' <"${work}/final.json")" = ACTIVE

audience="${NGKG_CONTEXT_CAPABILITY_AUDIENCE:-ngkg-agent-orchestrator}"
curl --fail-with-body --silent --show-error -H "${auth}" -H 'content-type: application/json' \
  -d "{\"audience\":\"${audience}\",\"rangeStart\":0,\"rangeEndExclusive\":${bytes},\"ttlSeconds\":60}" \
  "${NGKG_CONTEXT_URL%/}/v1/context-slices/${slice_id}/capabilities" >"${work}/capability.json"
capability="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])' <"${work}/capability.json")"
curl --fail-with-body --silent --show-error -H "X-NGKG-Slice-Capability: ${capability}" -H "X-NGKG-Capability-Audience: ${audience}" \
  "${NGKG_CONTEXT_URL%/}/v1/context-slices/${slice_id}/content" >"${work}/read.nt"
cmp "${payload}" "${work}/read.nt"

# Range expansion and wrong audience must fail closed.
if curl --fail --silent -H "X-NGKG-Slice-Capability: ${capability}" -H 'X-NGKG-Capability-Audience: wrong-audience' \
  "${NGKG_CONTEXT_URL%/}/v1/context-slices/${slice_id}/content" >/dev/null 2>&1; then
  echo "wrong-audience capability unexpectedly succeeded" >&2; exit 1
fi
curl --fail-with-body --silent --show-error -H "${auth}" -X POST "${NGKG_CONTEXT_URL%/}/v1/context-slices/${slice_id}/expire" >/dev/null
if curl --fail --silent -H "X-NGKG-Slice-Capability: ${capability}" -H "X-NGKG-Capability-Audience: ${audience}" \
  "${NGKG_CONTEXT_URL%/}/v1/context-slices/${slice_id}/content" >/dev/null 2>&1; then
  echo "expired slice unexpectedly succeeded" >&2; exit 1
fi
echo "Phase 10 live API lifecycle: PASS"
