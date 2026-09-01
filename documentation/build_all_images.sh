#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_RUST_BUILDER_IMAGE:?set an immutable Rust builder image reference}"
: "${NGKG_RUNTIME_IMAGE:?set an immutable nonroot runtime image reference}"
: "${NGKG_MAVEN_BUILDER_IMAGE:?set an immutable Maven builder image reference}"
: "${NGKG_JAVA_RUNTIME_IMAGE:?set an immutable Java runtime image reference}"
: "${NGKG_IMAGE_REGISTRY:?set the destination registry/repository prefix}"
: "${NGKG_IMAGE_TAG:?set an immutable source-derived image tag}"

container_cli="${NGKG_CONTAINER_CLI:-docker}"
if ! command -v "${container_cli}" >/dev/null 2>&1; then
  echo "container CLI not found: ${container_cli}" >&2
  exit 127
fi

require_digest_ref() {
  local name="$1"
  local reference="$2"
  if [[ ! "${reference}" =~ @sha256:[0-9a-f]{64}$ ]]; then
    echo "${name} must be an immutable image reference ending in @sha256:<64 lowercase hex characters>" >&2
    exit 2
  fi
}

require_digest_ref NGKG_RUST_BUILDER_IMAGE "${NGKG_RUST_BUILDER_IMAGE}"
require_digest_ref NGKG_RUNTIME_IMAGE "${NGKG_RUNTIME_IMAGE}"
require_digest_ref NGKG_MAVEN_BUILDER_IMAGE "${NGKG_MAVEN_BUILDER_IMAGE}"
require_digest_ref NGKG_JAVA_RUNTIME_IMAGE "${NGKG_JAVA_RUNTIME_IMAGE}"

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

(
  cd "${root_dir}/NGKG_1_0_0_GA"
  if [[ "${container_cli}" != docker ]]; then
    echo "NGKG_1_0_0_GA/scripts/build_images.sh currently requires docker" >&2
    exit 2
  fi
  ./scripts/build_images.sh
)

"${container_cli}" build --pull=false --network=none \
  --build-arg "RUST_BUILDER_IMAGE=${NGKG_RUST_BUILDER_IMAGE}" \
  --build-arg "RUNTIME_IMAGE=${NGKG_RUNTIME_IMAGE}" \
  --file "${root_dir}/ngkg-agents/deploy/mcp-gateway/Dockerfile" \
  --tag "${NGKG_IMAGE_REGISTRY}/ngkg-agents:${NGKG_IMAGE_TAG}" \
  "${root_dir}/ngkg-agents"

echo "Built 11 NGKG application images with network-disabled build steps."
