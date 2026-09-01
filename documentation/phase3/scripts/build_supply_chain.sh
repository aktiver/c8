#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

for command in docker crane syft grype trivy cosign jq sha256sum python3; do
  require_command "${command}"
done

: "${NGKG_IMAGE_REGISTRY:?destination registry/repository prefix is required}"
: "${NGKG_IMAGE_TAG:?immutable source-derived tag is required}"
: "${NGKG_SOURCE_REVISION:?40-64 character lowercase hexadecimal source revision is required}"
: "${NGKG_SOURCE_URI:?source repository URI is required}"
: "${NGKG_BUILDER_ID:?controlled runner identity URI is required}"
: "${NGKG_RUST_BUILDER_IMAGE:?digest-pinned Rust builder is required}"
: "${NGKG_RUNTIME_IMAGE:?digest-pinned nonroot runtime is required}"
: "${NGKG_MAVEN_BUILDER_IMAGE:?digest-pinned Maven builder is required}"
: "${NGKG_JAVA_RUNTIME_IMAGE:?digest-pinned Java runtime is required}"
: "${NGKG_VLLM_SOURCE_IMAGE:?digest-pinned approved vLLM source image is required}"
: "${NGKG_PHASE3_TOOLCHAIN_LOCK:?approved controlled-runner toolchain lock is required}"

[[ "${NGKG_SOURCE_REVISION}" =~ ^[0-9a-f]{40,64}$ ]] || die "NGKG_SOURCE_REVISION is invalid"
[[ "${NGKG_IMAGE_TAG}" != latest && "${NGKG_IMAGE_TAG}" != *[[:space:]]* ]] || die "NGKG_IMAGE_TAG must be immutable and whitespace-free"
require_digest_ref NGKG_RUST_BUILDER_IMAGE "${NGKG_RUST_BUILDER_IMAGE}"
require_digest_ref NGKG_RUNTIME_IMAGE "${NGKG_RUNTIME_IMAGE}"
require_digest_ref NGKG_MAVEN_BUILDER_IMAGE "${NGKG_MAVEN_BUILDER_IMAGE}"
require_digest_ref NGKG_JAVA_RUNTIME_IMAGE "${NGKG_JAVA_RUNTIME_IMAGE}"
require_digest_ref NGKG_VLLM_SOURCE_IMAGE "${NGKG_VLLM_SOURCE_IMAGE}"
require_file "${NGKG_PHASE3_TOOLCHAIN_LOCK}"

platforms="${NGKG_PLATFORMS:-linux/amd64,linux/arm64}"
[[ "${platforms}" == "linux/amd64,linux/arm64" || "${platforms}" == "linux/arm64,linux/amd64" ]] || die "Phase 3 requires linux/amd64 and linux/arm64"
evidence_dir="${NGKG_PHASE3_EVIDENCE_DIR:-${candidate_root}/phase3-evidence/supply-chain}"
mkdir -p "${evidence_dir}/images"
chmod 0700 "${evidence_dir}"
python3 "${phase3_root}/scripts/verify_toolchain.py" --lock "${NGKG_PHASE3_TOOLCHAIN_LOCK}" \
  --require docker --require crane --require syft --require grype --require trivy \
  --require cosign --require jq --require python3 --output "${evidence_dir}/toolchain-evidence.json"
catalog="${phase3_root}/config/images.json"
require_file "${catalog}"
export GRYPE_DB_AUTO_UPDATE=false

docker buildx inspect --bootstrap >/dev/null
lock_tmp="$(mktemp)"
trap 'rm -f "${lock_tmp}"' EXIT
jq -n --arg revision "${NGKG_SOURCE_REVISION}" --arg platforms "${platforms}" \
  --arg toolchain "$(sha256_file "${evidence_dir}/toolchain-evidence.json")" \
  '{formatVersion:1,sourceRevision:$revision,platforms:($platforms|split(",")),toolchainEvidenceSha256:$toolchain,images:[]}' >"${lock_tmp}"

cosign_sign() {
  local reference="$1"
  local spdx="$2"
  local cyclonedx="$3"
  local provenance="$4"
  if [[ "${NGKG_COSIGN_MODE:-keyless}" == key ]]; then
    : "${NGKG_COSIGN_KEY:?NGKG_COSIGN_KEY is required for key signing}"
    cosign sign --yes --key "${NGKG_COSIGN_KEY}" "${reference}"
    cosign attest --yes --key "${NGKG_COSIGN_KEY}" --type spdxjson --predicate "${spdx}" "${reference}"
    cosign attest --yes --key "${NGKG_COSIGN_KEY}" --type cyclonedx --predicate "${cyclonedx}" "${reference}"
    cosign attest --yes --key "${NGKG_COSIGN_KEY}" --type slsaprovenance --predicate "${provenance}" "${reference}"
    cosign verify --key "${NGKG_COSIGN_KEY}" "${reference}" >/dev/null
  else
    : "${NGKG_COSIGN_CERTIFICATE_IDENTITY_REGEXP:?keyless verification identity regexp is required}"
    : "${NGKG_COSIGN_OIDC_ISSUER:?keyless OIDC issuer is required}"
    cosign sign --yes "${reference}"
    cosign attest --yes --type spdxjson --predicate "${spdx}" "${reference}"
    cosign attest --yes --type cyclonedx --predicate "${cyclonedx}" "${reference}"
    cosign attest --yes --type slsaprovenance --predicate "${provenance}" "${reference}"
    cosign verify \
      --certificate-identity-regexp "${NGKG_COSIGN_CERTIFICATE_IDENTITY_REGEXP}" \
      --certificate-oidc-issuer "${NGKG_COSIGN_OIDC_ISSUER}" "${reference}" >/dev/null
  fi
}

while IFS= read -r item; do
  name="$(jq -r .name <<<"${item}")"
  kind="$(jq -r .kind <<<"${item}")"
  target="${NGKG_IMAGE_REGISTRY}/${name}:${NGKG_IMAGE_TAG}"
  image_dir="${evidence_dir}/images/${name}"
  mkdir -p "${image_dir}"
  if [[ "${kind}" == build ]]; then
    context="$(jq -r .context <<<"${item}")"
    dockerfile="$(jq -r .dockerfile <<<"${item}")"
    require_file "${candidate_root}/${context}/${dockerfile}"
    docker buildx build --pull=false --network=none --push \
      --platform "${platforms}" \
      --provenance=mode=max --sbom=true \
      --build-arg "RUST_BUILDER_IMAGE=${NGKG_RUST_BUILDER_IMAGE}" \
      --build-arg "RUNTIME_IMAGE=${NGKG_RUNTIME_IMAGE}" \
      --build-arg "MAVEN_BUILDER_IMAGE=${NGKG_MAVEN_BUILDER_IMAGE}" \
      --build-arg "JAVA_RUNTIME_IMAGE=${NGKG_JAVA_RUNTIME_IMAGE}" \
      --file "${candidate_root}/${context}/${dockerfile}" \
      --tag "${target}" "${candidate_root}/${context}"
  elif [[ "${kind}" == mirror ]]; then
    crane copy "${NGKG_VLLM_SOURCE_IMAGE}" "${target}"
  else
    die "unsupported image catalog kind: ${kind}"
  fi
  digest="$(crane digest "${target}")"
  [[ "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]] || die "registry returned invalid digest for ${name}"
  immutable="${NGKG_IMAGE_REGISTRY}/${name}@${digest}"
  spdx="${image_dir}/sbom.spdx.json"
  cyclonedx="${image_dir}/sbom.cyclonedx.json"
  grype_json="${image_dir}/grype.json"
  trivy_json="${image_dir}/trivy.json"
  provenance="${image_dir}/provenance.json"
  syft "${immutable}" -o "spdx-json=${spdx}" -o "cyclonedx-json=${cyclonedx}"
  grype "${immutable}" --fail-on high -o json >"${grype_json}"
  trivy image --quiet --offline-scan --skip-db-update --skip-java-db-update \
    --exit-code 1 --severity HIGH,CRITICAL --format json --output "${trivy_json}" "${immutable}"
  python3 "${phase3_root}/scripts/write_provenance.py" \
    --name "${name}" --digest "${digest}" --source-uri "${NGKG_SOURCE_URI}" \
    --source-revision "${NGKG_SOURCE_REVISION}" --builder-id "${NGKG_BUILDER_ID}" \
    --platforms "${platforms}" \
    --base "${NGKG_RUST_BUILDER_IMAGE}" --base "${NGKG_RUNTIME_IMAGE}" \
    --base "${NGKG_MAVEN_BUILDER_IMAGE}" --base "${NGKG_JAVA_RUNTIME_IMAGE}" \
    --output "${provenance}"
  cosign_sign "${immutable}" "${spdx}" "${cyclonedx}" "${provenance}"
  high="$(jq '[.matches[]? | select(.vulnerability.severity == "High")] | length' "${grype_json}")"
  critical="$(jq '[.matches[]? | select(.vulnerability.severity == "Critical")] | length' "${grype_json}")"
  [[ "${high}" -eq 0 && "${critical}" -eq 0 ]] || die "unapproved high/critical vulnerability in ${name}"
  jq -n -cS \
    --arg revision "${NGKG_SOURCE_REVISION}" --arg image "${NGKG_IMAGE_REGISTRY}/${name}" \
    --arg digest "${digest}" --arg platforms "${platforms}" \
    --arg spdx "$(sha256_file "${spdx}")" --arg cdx "$(sha256_file "${cyclonedx}")" \
    --arg grype "$(sha256_file "${grype_json}")" --arg trivy "$(sha256_file "${trivy_json}")" \
    --arg provenance "$(sha256_file "${provenance}")" \
    '{formatVersion:1,sourceRevision:$revision,image:$image,digest:$digest,platforms:($platforms|split(",")),spdxSha256:$spdx,cycloneDxSha256:$cdx,grypeSha256:$grype,trivySha256:$trivy,provenanceSha256:$provenance,signatureVerified:true,highVulnerabilities:0,criticalVulnerabilities:0,complete:true}' \
    >"${image_dir}/evidence.json"
  jq --arg name "${name}" --arg repository "${NGKG_IMAGE_REGISTRY}/${name}" --arg digest "${digest}" \
    '.images += [{name:$name,repository:$repository,digest:$digest}] | .images |= sort_by(.name)' \
    "${lock_tmp}" >"${lock_tmp}.next"
  mv "${lock_tmp}.next" "${lock_tmp}"
done < <(jq -c '.images[]' "${catalog}")

cp "${lock_tmp}" "${evidence_dir}/image-lock.json"
sha256_file "${evidence_dir}/image-lock.json" >"${evidence_dir}/image-lock.sha256"
echo "Phase 3 supply-chain build complete: $(jq '.images | length' "${evidence_dir}/image-lock.json") immutable images"
