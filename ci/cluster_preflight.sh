#!/usr/bin/env bash
set -euo pipefail

# Read-only preflight for a cluster that will run the NGKG database.
# This script never installs, patches, or deletes cluster resources.

usage() {
  cat <<'EOF'
Usage: cluster_preflight.sh [options]

Options:
  --namespace NAME          NGKG namespace (default: ngkg)
  --pull-secret NAME        Required registry pull secret
  --storage-class NAME      Required StorageClass
  --require-mpi             Require the MPIJob API
  --require-kueue           Require the Kueue APIs
  --require-keda            Require the ScaledObject API
  --require-metrics         Require metrics.k8s.io
  --help                    Show this help
EOF
}

namespace="ngkg"
pull_secret=""
storage_class=""
require_mpi=false
require_kueue=false
require_keda=false
require_metrics=false

while (($#)); do
  case "$1" in
    --namespace) namespace=${2:?missing namespace}; shift 2 ;;
    --pull-secret) pull_secret=${2:?missing pull secret}; shift 2 ;;
    --storage-class) storage_class=${2:?missing storage class}; shift 2 ;;
    --require-mpi) require_mpi=true; shift ;;
    --require-kueue) require_kueue=true; shift ;;
    --require-keda) require_keda=true; shift ;;
    --require-metrics) require_metrics=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

for command_name in kubectl helm; do
  command -v "$command_name" >/dev/null 2>&1 || {
    printf 'FAIL missing command: %s\n' "$command_name" >&2
    exit 1
  }
done

failures=0
check() {
  local label=$1
  shift
  if "$@" >/dev/null 2>&1; then
    printf 'PASS %s\n' "$label"
  else
    printf 'FAIL %s\n' "$label" >&2
    failures=$((failures + 1))
  fi
}

check "kubectl can reach the selected context" kubectl version
check "Kubernetes nodes exist" kubectl get nodes

not_ready_nodes=$(kubectl get nodes --no-headers 2>/dev/null | awk '$2 != "Ready" {count++} END {print count+0}')
if ((not_ready_nodes == 0)); then
  printf 'PASS all Kubernetes nodes are Ready\n'
else
  printf 'FAIL %s Kubernetes node(s) are not Ready\n' "$not_ready_nodes" >&2
  failures=$((failures + 1))
fi

check "namespace $namespace exists" kubectl get namespace "$namespace"
check "NGKG compilation CRD exists" kubectl get crd ngkgcompilations.ngkg.io
check "NGKG source import CRD exists" kubectl get crd ngkgsourceimports.ngkg.io
check "NGKG storage recovery CRD exists" kubectl get crd ngkgstoragerecoveries.ngkg.io

if [[ -n "$pull_secret" ]]; then
  check "registry pull secret $namespace/$pull_secret exists" \
    kubectl --namespace "$namespace" get secret "$pull_secret"
fi
if [[ -n "$storage_class" ]]; then
  check "StorageClass $storage_class exists" kubectl get storageclass "$storage_class"
fi
if [[ "$require_mpi" == true ]]; then
  check "MPIJob API is discoverable" kubectl api-resources --api-group kubeflow.org
  kubectl api-resources --api-group kubeflow.org -o name 2>/dev/null | grep -qx mpijobs || {
    printf 'FAIL MPIJob resource is absent from kubeflow.org\n' >&2
    failures=$((failures + 1))
  }
fi
if [[ "$require_kueue" == true ]]; then
  check "Kueue API is discoverable" kubectl api-resources --api-group kueue.x-k8s.io
fi
if [[ "$require_keda" == true ]]; then
  check "KEDA ScaledObject API is discoverable" kubectl get crd scaledobjects.keda.sh
fi
if [[ "$require_metrics" == true ]]; then
  check "resource metrics API is available" kubectl get --raw /apis/metrics.k8s.io/v1beta1
fi

default_storage_classes=$(kubectl get storageclass \
  -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")]}{.metadata.name}{"\n"}{end}' \
  2>/dev/null || true)
if [[ -n "$storage_class" || -n "$default_storage_classes" ]]; then
  printf 'PASS persistent storage provisioning is configured\n'
else
  printf 'FAIL no required or default StorageClass was found\n' >&2
  failures=$((failures + 1))
fi

if ((failures)); then
  printf 'Preflight failed with %s blocking condition(s).\n' "$failures" >&2
  exit 1
fi

printf 'Cluster preflight passed for context %s and namespace %s.\n' \
  "$(kubectl config current-context)" "$namespace"
