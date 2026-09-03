#!/usr/bin/env bash
set -euo pipefail
: "${AZURE_RESOURCE_GROUP:?set AZURE_RESOURCE_GROUP}"
: "${AKS_CLUSTER:?set AKS_CLUSTER}"
: "${AKS_GPU_VM_SIZE:=Standard_NC24ads_A100_v4}"
: "${AKS_GPU_MAX_NODES:=16}"
az aks nodepool add --resource-group "${AZURE_RESOURCE_GROUP}" --cluster-name "${AKS_CLUSTER}" \
  --name ngkggpu --node-vm-size "${AKS_GPU_VM_SIZE}" --node-count 0 --min-count 0 \
  --max-count "${AKS_GPU_MAX_NODES}" --enable-cluster-autoscaler \
  --labels ngkg.io/accelerator=nvidia-gpu --node-taints ngkg.io/gpu=true:NoSchedule \
  --mode User --os-sku AzureLinux
