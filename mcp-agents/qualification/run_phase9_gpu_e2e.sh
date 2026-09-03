#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_NAMESPACE:?set NGKG_NAMESPACE}"
: "${NGKG_INFERENCE_URL:?set the internal or forwarded ngkg-vllm URL}"
: "${NGKG_MODEL_NAME:?set NGKG_MODEL_NAME}"
: "${NGKG_TEST_PROMPT:=Return the single word ready.}"
: "${NGKG_GPU_SCALE_TIMEOUT_SECONDS:=1200}"

command -v kubectl >/dev/null
command -v curl >/dev/null
command -v jq >/dev/null

initial_replicas="$(kubectl -n "${NGKG_NAMESPACE}" get deployment ngkg-vllm-backend -o jsonpath='{.status.readyReplicas}')"
initial_replicas="${initial_replicas:-0}"
status_json="$(curl --fail --silent --show-error --max-time 10 "${NGKG_INFERENCE_URL%/}/v1/status")"
jq -e '.scope == "INSTANCE" and (.ready == true)' <<<"${status_json}" >/dev/null

request_file="$(mktemp)"
response_file="$(mktemp)"
cleanup() { rm -f "${request_file}" "${response_file}"; }
trap cleanup EXIT
jq -n --arg model "${NGKG_MODEL_NAME}" --arg prompt "${NGKG_TEST_PROMPT}" \
  '{model:$model,messages:[{role:"user",content:$prompt}],stream:false,max_tokens:8}' >"${request_file}"

curl --fail-with-body --silent --show-error --max-time "${NGKG_GPU_SCALE_TIMEOUT_SECONDS}" \
  -H 'content-type: application/json' --data-binary "@${request_file}" \
  "${NGKG_INFERENCE_URL%/}/v1/chat/completions" >"${response_file}" &
request_pid="$!"

deadline="$((SECONDS + NGKG_GPU_SCALE_TIMEOUT_SECONDS))"
scaled=false
while (( SECONDS < deadline )); do
  desired="$(kubectl -n "${NGKG_NAMESPACE}" get deployment ngkg-vllm-backend -o jsonpath='{.spec.replicas}')"
  if (( desired > initial_replicas )); then scaled=true; break; fi
  sleep 5
done
if [[ "${scaled}" != true ]]; then
  kill "${request_pid}" 2>/dev/null || true
  echo 'GPU backend did not scale in response to the admission queue' >&2
  exit 1
fi

wait "${request_pid}"
jq -e --arg model "${NGKG_MODEL_NAME}" '.model == $model and (.choices | length > 0)' "${response_file}" >/dev/null
kubectl -n "${NGKG_NAMESPACE}" wait --for=condition=Ready pod -l app.kubernetes.io/component=vllm-backend --timeout="${NGKG_GPU_SCALE_TIMEOUT_SECONDS}s"
kubectl -n "${NGKG_NAMESPACE}" get scaledobject ngkg-vllm-backend -o json | jq -e '.status.conditions[] | select(.type == "Ready" and .status == "True")' >/dev/null

echo 'Phase 9 live scale-from-zero inference qualification: PASS'
