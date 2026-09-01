#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
for command in helm kubectl jq sha256sum; do require_command "${command}"; done

: "${NGKG_PROVIDER:?provider is required}"
: "${NGKG_KUBECTL_CONTEXT:?kubectl context is required}"
: "${NGKG_NAMESPACE:?qualification namespace is required}"
: "${NGKG_PHASE3_IMAGE_LOCK:?image lock is required}"
: "${NGKG_PLATFORM_VALUES:?approved platform values file is required}"
: "${NGKG_WORKLOAD_VALUES:?approved workloads values file is required}"
: "${NGKG_AGENT_VALUES:?approved agents values file is required}"
: "${NGKG_PHASE3_DEPLOYMENT_EVIDENCE:?deployment evidence output is required}"
: "${NGKG_PHASE3_TOOLCHAIN_LOCK:?approved controlled-runner toolchain lock is required}"

[[ "${NGKG_PROVIDER}" =~ ^(rke2|eks|aks|gke)$ ]] || die "unsupported provider"
for file in "${NGKG_PHASE3_IMAGE_LOCK}" "${NGKG_PLATFORM_VALUES}" "${NGKG_WORKLOAD_VALUES}" "${NGKG_AGENT_VALUES}"; do require_file "${file}"; done
image() { jq -r --arg name "$1" '.images[] | select(.name==$name) | .repository+" "+.digest' "${NGKG_PHASE3_IMAGE_LOCK}"; }
read -r api_repo api_digest <<<"$(image ngkg-api)"
read -r migrator_repo migrator_digest <<<"$(image ngkg-catalog-migrator)"
read -r operator_repo operator_digest <<<"$(image ngkg-operator)"
read -r dist_operator_repo dist_operator_digest <<<"$(image ngkg-distributed-operator)"
read -r dist_worker_repo dist_worker_digest <<<"$(image ngkg-distributed-worker)"
read -r reference_repo reference_digest <<<"$(image ngkg-reference-worker)"
read -r recovery_operator_repo recovery_operator_digest <<<"$(image ngkg-storage-recovery-operator)"
read -r recovery_worker_repo recovery_worker_digest <<<"$(image ngkg-storage-recovery-worker)"
read -r online_repo online_digest <<<"$(image ngkg-online-serving)"
read -r reasoner_repo reasoner_digest <<<"$(image ngkg-direct-reasoner-worker)"
read -r agents_repo agents_digest <<<"$(image ngkg-agents)"
read -r vllm_repo vllm_digest <<<"$(image ngkg-vllm)"

kubectl --context "${NGKG_KUBECTL_CONTEXT}" get namespace kube-system >/dev/null
kubectl --context "${NGKG_KUBECTL_CONTEXT}" create namespace "${NGKG_NAMESPACE}" --dry-run=client -o yaml | kubectl --context "${NGKG_KUBECTL_CONTEXT}" apply -f - >/dev/null
evidence_dir="$(dirname "${NGKG_PHASE3_DEPLOYMENT_EVIDENCE}")"
mkdir -p "${evidence_dir}"
toolchain_evidence="${NGKG_PHASE3_DEPLOYMENT_EVIDENCE%.json}-toolchain.json"
python3 "${phase3_root}/scripts/verify_toolchain.py" --lock "${NGKG_PHASE3_TOOLCHAIN_LOCK}" \
  --require helm --require kubectl --require jq --require python3 \
  --output "${toolchain_evidence}"
rendered="${evidence_dir}/${NGKG_PROVIDER}-rendered.yaml"
: >"${rendered}"

platform_set=(
  --set-string "images.api.repository=${api_repo}" --set-string "images.api.digest=${api_digest}"
  --set-string "images.catalogMigrator.repository=${migrator_repo}" --set-string "images.catalogMigrator.digest=${migrator_digest}"
  --set-string "images.operator.repository=${operator_repo}" --set-string "images.operator.digest=${operator_digest}"
  --set-string "images.distributedOperator.repository=${dist_operator_repo}" --set-string "images.distributedOperator.digest=${dist_operator_digest}"
  --set-string "images.distributedWorker.repository=${dist_worker_repo}" --set-string "images.distributedWorker.digest=${dist_worker_digest}"
  --set-string "images.referenceWorker.repository=${reference_repo}" --set-string "images.referenceWorker.digest=${reference_digest}"
  --set-string "images.storageRecoveryOperator.repository=${recovery_operator_repo}" --set-string "images.storageRecoveryOperator.digest=${recovery_operator_digest}"
  --set-string "images.storageRecoveryWorker.repository=${recovery_worker_repo}" --set-string "images.storageRecoveryWorker.digest=${recovery_worker_digest}"
)
workload_set=(
  --set-string "platform.kubernetesDistribution=${NGKG_PROVIDER}"
  --set-string "images.query.repository=${online_repo}" --set-string "images.query.digest=${online_digest}"
  --set-string "images.fragment.repository=${online_repo}" --set-string "images.fragment.digest=${online_digest}"
  --set-string "images.locator.repository=${online_repo}" --set-string "images.locator.digest=${online_digest}"
  --set-string "images.hydration.repository=${online_repo}" --set-string "images.hydration.digest=${online_digest}"
  --set-string "images.reasoner.repository=${reasoner_repo}" --set-string "images.reasoner.digest=${reasoner_digest}"
)
agent_set=(
  --set-string "image.repository=${agents_repo}" --set-string "image.digest=${agents_digest}"
  --set-string "vllm.image.repository=${vllm_repo}" --set-string "vllm.image.digest=${vllm_digest}"
)

helm template ngkg-crds "${candidate_root}/NGKG_1_0_0_GA/charts/ngkg-crds" --namespace "${NGKG_NAMESPACE}" --kube-version 1.33.0 >>"${rendered}"
helm template ngkg-platform "${candidate_root}/NGKG_1_0_0_GA/charts/ngkg-platform" --namespace "${NGKG_NAMESPACE}" --kube-version 1.33.0 --values "${NGKG_PLATFORM_VALUES}" "${platform_set[@]}" >>"${rendered}"
helm template ngkg-workloads "${candidate_root}/NGKG_1_0_0_GA/charts/ngkg-workloads" --namespace "${NGKG_NAMESPACE}" --kube-version 1.33.0 --values "${NGKG_WORKLOAD_VALUES}" "${workload_set[@]}" >>"${rendered}"
helm template ngkg-agents "${candidate_root}/ngkg-agents/charts/ngkg-agents" --namespace "${NGKG_NAMESPACE}" --kube-version 1.33.0 --values "${NGKG_AGENT_VALUES}" "${agent_set[@]}" >>"${rendered}"

timeout="${NGKG_HELM_TIMEOUT:-30m}"
helm upgrade --install ngkg-crds "${candidate_root}/NGKG_1_0_0_GA/charts/ngkg-crds" --kube-context "${NGKG_KUBECTL_CONTEXT}" --namespace "${NGKG_NAMESPACE}" --atomic --wait --timeout "${timeout}"
helm upgrade --install ngkg-platform "${candidate_root}/NGKG_1_0_0_GA/charts/ngkg-platform" --kube-context "${NGKG_KUBECTL_CONTEXT}" --namespace "${NGKG_NAMESPACE}" --atomic --wait --wait-for-jobs --timeout "${timeout}" --values "${NGKG_PLATFORM_VALUES}" "${platform_set[@]}"
helm upgrade --install ngkg-workloads "${candidate_root}/NGKG_1_0_0_GA/charts/ngkg-workloads" --kube-context "${NGKG_KUBECTL_CONTEXT}" --namespace "${NGKG_NAMESPACE}" --atomic --wait --timeout "${timeout}" --values "${NGKG_WORKLOAD_VALUES}" "${workload_set[@]}"
helm upgrade --install ngkg-agents "${candidate_root}/ngkg-agents/charts/ngkg-agents" --kube-context "${NGKG_KUBECTL_CONTEXT}" --namespace "${NGKG_NAMESPACE}" --atomic --wait --wait-for-jobs --timeout "${timeout}" --values "${NGKG_AGENT_VALUES}" "${agent_set[@]}"

cluster_uid="$(kubectl --context "${NGKG_KUBECTL_CONTEXT}" get namespace kube-system -o jsonpath='{.metadata.uid}')"
jq -n -cS --arg provider "${NGKG_PROVIDER}" --arg uid "${cluster_uid}" \
  --arg imageLockSha256 "$(sha256_file "${NGKG_PHASE3_IMAGE_LOCK}")" \
  --arg toolchainEvidenceSha256 "$(sha256_file "${toolchain_evidence}")" \
  --arg manifestSha256 "$(sha256_file "${rendered}")" \
  '{formatVersion:1,provider:$provider,clusterUid:$uid,imageLockSha256:$imageLockSha256,toolchainEvidenceSha256:$toolchainEvidenceSha256,renderedManifestSha256:$manifestSha256,helmAtomic:true,waitForJobs:true,complete:true}' \
  >"${NGKG_PHASE3_DEPLOYMENT_EVIDENCE}"
echo "Phase 3 atomic Helm deployment: PASS (${NGKG_PROVIDER})"
