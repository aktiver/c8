#!/usr/bin/env bash
set -euo pipefail

required_variables=(
  NGKG_API_URL
  NGKG_API_TOKEN
  NGKG_DATASET_ID
  NGKG_DATASET_NAMESPACE
  NGKG_POLICY_VERSION
  NGKG_BUNDLE_OBJECT_KEY
  NGKG_BUNDLE_SHA256
  NGKG_TARGET_SNAPSHOT_ID
  NGKG_IDEMPOTENCY_KEY
  NGKG_RESOURCE_PROFILE
  NGKG_KUBERNETES_NAMESPACE
  NGKG_KUEUE_QUEUE
  NGKG_PHASE14_TIMEOUT_SECONDS
)
for variable in "${required_variables[@]}"; do
  if [[ -z "${!variable:-}" ]]; then
    echo "${variable} is required" >&2
    exit 2
  fi
done
for command in curl jq kubectl; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 2; }
done

run_dir="$(mktemp -d)"
dataset_body="${run_dir}/dataset.json"
ingestion_body="${run_dir}/ingestion.json"
accepted_one="${run_dir}/accepted-one.json"
accepted_two="${run_dir}/accepted-two.json"
job_body="${run_dir}/job.json"
snapshot_body="${run_dir}/snapshot.json"

jq -n \
  --arg identityNamespace "${NGKG_DATASET_NAMESPACE}" \
  --arg policyVersion "${NGKG_POLICY_VERSION}" \
  '{identityNamespace: $identityNamespace, policyVersion: $policyVersion}' \
  > "${dataset_body}"

parent_json="null"
if [[ -n "${NGKG_PARENT_SNAPSHOT_ID:-}" ]]; then
  parent_json="\"${NGKG_PARENT_SNAPSHOT_ID}\""
fi
jq -n \
  --arg bundleObjectKey "${NGKG_BUNDLE_OBJECT_KEY}" \
  --arg bundleSha256 "${NGKG_BUNDLE_SHA256}" \
  --arg targetSnapshotId "${NGKG_TARGET_SNAPSHOT_ID}" \
  --arg resourceProfile "${NGKG_RESOURCE_PROFILE}" \
  --argjson parentSnapshotId "${parent_json}" \
  '{bundleObjectKey: $bundleObjectKey, bundleSha256: $bundleSha256,
    parentSnapshotId: $parentSnapshotId, targetSnapshotId: $targetSnapshotId,
    publicationPolicy: "manual-after-certification", resourceProfile: $resourceProfile}' \
  > "${ingestion_body}"

auth_header="Authorization: Bearer ${NGKG_API_TOKEN}"
curl --fail-with-body --silent --show-error \
  -X PUT -H "${auth_header}" -H 'Content-Type: application/json' \
  --data-binary "@${dataset_body}" \
  "${NGKG_API_URL}/v1/datasets/${NGKG_DATASET_ID}"

for output in "${accepted_one}" "${accepted_two}"; do
  curl --fail-with-body --silent --show-error \
    -X POST -H "${auth_header}" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: ${NGKG_IDEMPOTENCY_KEY}" \
    --data-binary "@${ingestion_body}" \
    "${NGKG_API_URL}/v1/datasets/${NGKG_DATASET_ID}/ingestions" > "${output}"
done
operation_id="$(jq -er '.operationId' "${accepted_one}")"
test "${operation_id}" = "$(jq -er '.operationId' "${accepted_two}")"
test "REGISTERED" = "$(jq -er '.state' "${accepted_one}")"

changed_body="${run_dir}/changed-ingestion.json"
jq '.publicationPolicy = "automatic-after-certification"' "${ingestion_body}" > "${changed_body}"
conflict_code="$(curl --silent --show-error -o "${run_dir}/conflict.json" -w '%{http_code}' \
  -X POST -H "${auth_header}" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${NGKG_IDEMPOTENCY_KEY}" \
  --data-binary "@${changed_body}" \
  "${NGKG_API_URL}/v1/datasets/${NGKG_DATASET_ID}/ingestions")"
test "409" = "${conflict_code}"

deadline="$((SECONDS + NGKG_PHASE14_TIMEOUT_SECONDS))"
state="REGISTERED"
while (( SECONDS < deadline )); do
  curl --fail-with-body --silent --show-error \
    -H "${auth_header}" \
    "${NGKG_API_URL}/v1/jobs/${operation_id}" > "${job_body}"
  state="$(jq -er '.operation.state' "${job_body}")"
  case "${state}" in
    CERTIFIED|PUBLISHED) break ;;
    FAILED|CANCELLED)
      jq . "${job_body}" >&2
      exit 1
      ;;
  esac
  sleep 5
done
test "${state}" = "CERTIFIED" -o "${state}" = "PUBLISHED"

if [[ "${state}" = "CERTIFIED" ]]; then
  publish_body="${run_dir}/publish.json"
  jq -n --argjson expectedParentSnapshotId "${parent_json}" \
    '{expectedParentSnapshotId: $expectedParentSnapshotId}' > "${publish_body}"
  curl --fail-with-body --silent --show-error \
    -X POST -H "${auth_header}" -H 'Content-Type: application/json' \
    --data-binary "@${publish_body}" \
    "${NGKG_API_URL}/v1/datasets/${NGKG_DATASET_ID}/snapshots/${NGKG_TARGET_SNAPSHOT_ID}/publish" \
    > "${snapshot_body}"
else
  curl --fail-with-body --silent --show-error \
    -H "${auth_header}" \
    "${NGKG_API_URL}/v1/datasets/${NGKG_DATASET_ID}/snapshots/${NGKG_TARGET_SNAPSHOT_ID}" \
    > "${snapshot_body}"
fi
test "PUBLISHED" = "$(jq -er '.state' "${snapshot_body}")"
test "${NGKG_TARGET_SNAPSHOT_ID}" = "$(jq -er '.snapshotId' "${snapshot_body}")"

resource_name="ngkg-${operation_id//-/}"
kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get ngkgcompilation "${resource_name}" -o json \
  > "${run_dir}/compilation-resource.json"
test "${operation_id}" = "$(jq -er '.spec.operationId' "${run_dir}/compilation-resource.json")"
kubectl --namespace "${NGKG_KUBERNETES_NAMESPACE}" get job "${resource_name}-reference" -o json \
  > "${run_dir}/job-resource.json"
test "${operation_id}" = "$(jq -er '.metadata.labels["ngkg.io/operation-id"]' "${run_dir}/job-resource.json")"
test "${NGKG_KUEUE_QUEUE}" = "$(jq -er '.metadata.labels["kueue.x-k8s.io/queue-name"]' "${run_dir}/job-resource.json")"
test "semantic-projection" = "$(jq -er '.spec.template.spec.nodeSelector["ngkg.io/workload"]' "${run_dir}/job-resource.json")"
jq -e '.metadata.annotations["ngkg.io/work-spec-sha256"] | test("^[0-9a-f]{64}$")' \
  "${run_dir}/job-resource.json" >/dev/null
jq -e '.spec.template.spec.containers[0].image | test("@sha256:[0-9a-f]{64}$")' \
  "${run_dir}/job-resource.json" >/dev/null
jq -e '.spec.template.spec.containers[0].resources as $r |
  $r.requests.cpu == $r.limits.cpu and
  $r.requests.memory == $r.limits.memory and
  $r.requests["ephemeral-storage"] == $r.limits["ephemeral-storage"]' \
  "${run_dir}/job-resource.json" >/dev/null
compilation_uid="$(jq -er '.metadata.uid' "${run_dir}/compilation-resource.json")"
test "${compilation_uid}" = "$(jq -er '.metadata.ownerReferences[] | select(.controller == true and .kind == "NgkgCompilation") | .uid' "${run_dir}/job-resource.json")"

jq -n \
  --arg operationId "${operation_id}" \
  --arg snapshotId "${NGKG_TARGET_SNAPSHOT_ID}" \
  --arg evidenceDirectory "${run_dir}" \
  '{status: "smoke-passed", operationId: $operationId, snapshotId: $snapshotId,
    evidenceDirectory: $evidenceDirectory}'
