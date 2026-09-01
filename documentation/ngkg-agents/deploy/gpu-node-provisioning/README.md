# GPU node provisioning contract

NGKG does not create cloud infrastructure from its application Helm release. Install an autoscaler-capable GPU pool first, then select the matching profile. Every provider must expose `nvidia.com/gpu`, label nodes `ngkg.io/accelerator=nvidia-gpu`, and taint them `ngkg.io/gpu=true:NoSchedule`. The pool may have a minimum of zero because the CPU admission gateway holds bounded calls while KEDA creates pending GPU pods.

## EKS

Install the NVIDIA device plugin and Karpenter, replace the role, pinned AMI, cluster-name and discovery placeholders in `eks-karpenter.yaml`, and apply it. Karpenter observes the pending `ngkg-vllm-backend` pods and provisions a compatible `g` or `p` instance. Use `profiles/eks-gpu.yaml` with Helm. For production, restrict instance families, zones and capacity types to the combinations qualified by your organization.

## AKS

Run `aks-create-nodepool.sh` with `AZURE_RESOURCE_GROUP`, `AKS_CLUSTER`, and approved VM-size/maximum settings. It creates an autoscaled user pool with minimum zero. Confirm the NVIDIA driver/device-plugin path supported by the selected AKS image, then use `profiles/aks-gpu.yaml`.

## GKE

Run `gke-create-nodepool.sh` with project, cluster and location. It creates an autoscaled, tainted L4 pool with minimum zero and managed drivers. Change the machine/GPU pair only to a GKE-compatible combination, then use `profiles/gke-gpu.yaml`.

## RKE2/RKE

Install NVIDIA GPU Operator or the pinned NVIDIA device plugin/runtime on dedicated worker templates. Merge `rke2-node-registration.yaml` into GPU-node configuration. Configure the infrastructure provider plus Kubernetes Cluster Autoscaler (or Rancher-managed machine pool autoscaling) with minimum zero and the exact label/taint template. Use `profiles/rke2-gpu.yaml`. Bare-metal nodes cannot scale from zero unless an external machine provisioner exists.

## Required checks

Before Helm installation, verify `kubectl get nodes -L ngkg.io/accelerator` and `kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.allocatable.nvidia\\.com/gpu}{"\n"}{end}'`. A test GPU pod must reach Ready, the device plugin must survive node replacement, and Prometheus/KEDA must be able to read `ngkg_inference_waiting_requests`. Prefer a preloaded immutable model PVC or image. If remote model download is allowed, configure workload identity and only the required `modelSourceEgressIpBlocks`; do not put Hugging Face tokens in Helm values.

Version-sensitive provider contracts must be rechecked before every supported-matrix freeze: [KEDA ScaledObject](https://keda.sh/docs/2.20/reference/scaledobject-spec/), [KEDA Prometheus scaler](https://keda.sh/docs/2.20/scalers/prometheus/), [Karpenter NodePools](https://karpenter.sh/docs/concepts/nodepools/), [AKS GPU pools](https://learn.microsoft.com/azure/aks/use-nvidia-gpu), and [GKE GPU pools](https://cloud.google.com/kubernetes-engine/docs/how-to/gpus).
