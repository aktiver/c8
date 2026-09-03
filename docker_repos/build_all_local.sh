#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
repo_root="$(CDPATH= cd -- "${script_dir}/.." && pwd -P)"

for command_name in docker python3; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "required command is missing: ${command_name}" >&2
    exit 69
  fi
done
if ! docker info >/dev/null 2>&1; then
  echo 'Docker is unavailable or the current user cannot access its daemon' >&2
  exit 69
fi
if ! docker buildx inspect >/dev/null 2>&1; then
  echo 'Docker Buildx has no active builder; run docker buildx create --use first' >&2
  exit 69
fi

registry="${NGKG_LOCAL_REGISTRY:-}"
namespace="${NGKG_LOCAL_REGISTRY_NAMESPACE:-ngkg}"
platform="${NGKG_BUILD_PLATFORM:-linux/amd64}"
build_offline="${NGKG_BUILD_OFFLINE:-false}"
if [[ "${build_offline}" != true && "${build_offline}" != false ]]; then
  echo 'NGKG_BUILD_OFFLINE must be exactly true or false' >&2
  exit 64
fi
if [[ -z "${registry}" || "${registry}" == */* || "${registry}" == *://* ]]; then
  echo 'NGKG_LOCAL_REGISTRY must be a node-reachable registry authority such as registry.lan:5000' >&2
  exit 64
fi
if [[ "${registry}" == localhost:* || "${registry}" == 127.0.0.1:* ]]; then
  if [[ "${NGKG_ALLOW_NODE_LOCAL_REGISTRY:-false}" != true ]]; then
    echo 'localhost registries are node-local; use a registry address reachable from every Kubernetes node' >&2
    exit 64
  fi
fi

required_images=(
  NGKG_RUST_BUILDER_IMAGE
  NGKG_RUNTIME_IMAGE
  NGKG_MAVEN_BUILDER_IMAGE
  NGKG_JAVA_RUNTIME_IMAGE
  NGKG_VLLM_SOURCE_IMAGE
  NGKG_MPI_BUILDER_IMAGE
  NGKG_MPI_RUNTIME_IMAGE
)
for variable_name in "${required_images[@]}"; do
  value="${!variable_name:-}"
  if [[ ! "${value}" =~ @sha256:[0-9a-f]{64}$ ]]; then
    echo "${variable_name} must be a locally available digest-pinned image reference" >&2
    exit 64
  fi
done

if [[ -n "${NGKG_LOCAL_REGISTRY_USERNAME:-}" ]]; then
  if [[ -z "${NGKG_LOCAL_REGISTRY_PASSWORD:-}" ]]; then
    echo 'NGKG_LOCAL_REGISTRY_PASSWORD is required when a registry username is configured' >&2
    exit 64
  fi
  printf '%s' "${NGKG_LOCAL_REGISTRY_PASSWORD}" | docker login "${registry}" \
    --username "${NGKG_LOCAL_REGISTRY_USERNAME}" --password-stdin >/dev/null
fi

cd "${repo_root}"
python3 docker_repos/validate_image_parity.py
python3 docker_repos/build_and_push_local.py \
  --registry "${registry}" \
  --namespace "${namespace}" \
  --platform "${platform}" \
  --output docker_repos/generated \
  "$@"
python3 docker_repos/validate_image_parity.py --lock docker_repos/generated/image-lock.json

echo 'All NGKG images were pushed and digest-pinned Helm values were generated:'
echo '  docker_repos/generated/platform-local-registry-values.yaml'
echo '  docker_repos/generated/workloads-local-registry-values.yaml'
echo '  docker_repos/generated/agents-local-registry-values.yaml'
