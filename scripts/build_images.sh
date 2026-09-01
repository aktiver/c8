#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_RUST_BUILDER_IMAGE:?set an immutable Rust builder image reference}"
: "${NGKG_RUNTIME_IMAGE:?set an immutable nonroot runtime image reference}"
: "${NGKG_MAVEN_BUILDER_IMAGE:?set an immutable Maven builder image reference}"
: "${NGKG_JAVA_RUNTIME_IMAGE:?set an immutable Java runtime image reference}"
: "${NGKG_IMAGE_REGISTRY:?set the destination registry/repository prefix}"
: "${NGKG_IMAGE_TAG:?set an immutable source-derived image tag}"

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

build_rust() {
  local name="$1"
  local dockerfile="$2"
  docker build --pull=false --network=none \
    --build-arg "RUST_BUILDER_IMAGE=${NGKG_RUST_BUILDER_IMAGE}" \
    --build-arg "RUNTIME_IMAGE=${NGKG_RUNTIME_IMAGE}" \
    --file "${dockerfile}" \
    --tag "${NGKG_IMAGE_REGISTRY}/${name}:${NGKG_IMAGE_TAG}" .
}

build_java() {
  local name="$1"
  local dockerfile="$2"
  docker build --pull=false --network=none \
    --build-arg "RUST_BUILDER_IMAGE=${NGKG_RUST_BUILDER_IMAGE}" \
    --build-arg "MAVEN_BUILDER_IMAGE=${NGKG_MAVEN_BUILDER_IMAGE}" \
    --build-arg "JAVA_RUNTIME_IMAGE=${NGKG_JAVA_RUNTIME_IMAGE}" \
    --file "${dockerfile}" \
    --tag "${NGKG_IMAGE_REGISTRY}/${name}:${NGKG_IMAGE_TAG}" .
}

build_rust ngkg-api deploy/api/Dockerfile
build_rust ngkg-catalog-migrator deploy/catalog-migrator/Dockerfile
build_rust ngkg-distributed-operator deploy/distributed-operator/Dockerfile
build_rust ngkg-distributed-worker deploy/distributed-worker/Dockerfile
build_rust ngkg-operator deploy/operator/Dockerfile
build_rust ngkg-storage-recovery-operator deploy/storage-recovery-operator/Dockerfile
build_rust ngkg-storage-recovery-worker deploy/storage-recovery-worker/Dockerfile
build_java ngkg-direct-reasoner-worker deploy/direct-reasoner-worker/Dockerfile
build_java ngkg-reference-worker deploy/reference-worker/Dockerfile
build_java ngkg-online-serving deploy/online-serving/Dockerfile
