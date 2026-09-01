#!/usr/bin/env bash
set -euo pipefail
: "${GCP_PROJECT:?set GCP_PROJECT}"
: "${GKE_CLUSTER:?set GKE_CLUSTER}"
: "${GKE_LOCATION:?set GKE_LOCATION}"
: "${GKE_GPU_TYPE:=nvidia-l4}"
: "${GKE_GPU_MAX_NODES:=16}"
gcloud container node-pools create ngkg-gpu --project "${GCP_PROJECT}" \
  --cluster "${GKE_CLUSTER}" --location "${GKE_LOCATION}" --machine-type g2-standard-24 \
  --accelerator "type=${GKE_GPU_TYPE},count=2,gpu-driver-version=default" \
  --num-nodes 0 --enable-autoscaling --min-nodes 0 --max-nodes "${GKE_GPU_MAX_NODES}" \
  --node-labels ngkg.io/accelerator=nvidia-gpu \
  --node-taints ngkg.io/gpu=true:NoSchedule --shielded-secure-boot
