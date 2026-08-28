#!/usr/bin/env bash
set -euo pipefail

: "${NGKG_NAMESPACE:?NGKG_NAMESPACE is required}"
: "${NGKG_AUTOSCALER_NAMESPACE:?NGKG_AUTOSCALER_NAMESPACE is required}"
: "${NGKG_AUTOSCALER_WORKLOAD:?NGKG_AUTOSCALER_WORKLOAD is required}"
: "${NGKG_AUTOSCALER_STATUS_CONFIGMAP:?NGKG_AUTOSCALER_STATUS_CONFIGMAP is required}"

command -v kubectl >/dev/null
kubectl get --raw /readyz >/dev/null
kubectl get --raw /apis/metrics.k8s.io/v1beta1/nodes >/dev/null
kubectl api-resources --api-group=kueue.x-k8s.io | grep -q ResourceFlavor
kubectl api-resources --api-group=keda.sh | grep -q ScaledJob
kubectl api-resources --api-group=gateway.networking.k8s.io | grep -q Gateway
kubectl -n "$NGKG_AUTOSCALER_NAMESPACE" rollout status "deployment/$NGKG_AUTOSCALER_WORKLOAD" --timeout=120s
kubectl -n "$NGKG_AUTOSCALER_NAMESPACE" get configmap "$NGKG_AUTOSCALER_STATUS_CONFIGMAP" >/dev/null

responsibilities=(semantic-projection semantic-artifact-build reasoning index-build sparql-query-processing sparql-fragment-processing parquet-hydration maintenance-export)
for responsibility in "${responsibilities[@]}"; do
  nodes=$(kubectl get nodes -l "ngkg.io/workload=$responsibility" -o name)
  if [[ -z "$nodes" && "$responsibility" != "maintenance-export" ]]; then
    echo "no ready node for $responsibility; verify a valid scale-from-zero Rancher machine pool" >&2
    exit 1
  fi
done

kubectl -n "$NGKG_NAMESPACE" auth can-i create jobs.batch --as "system:serviceaccount:$NGKG_NAMESPACE:ngkg-operator" | grep -qx yes
