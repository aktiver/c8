# Phase 9 vLLM/GPU deployment

Phase 9 adds a Kubernetes-native, provider-neutral vLLM serving plane without placing model output inside NGKG's semantic trust boundary. Models still propose text and claims; NGKG authorization, the pinned OWL 2 DL snapshot, proof validation and answer certification remain authoritative.

## Runtime topology

`ngkg-vllm` is now an HA CPU admission Service, not a GPU Service. It accepts only bounded OpenAI-compatible JSON, validates the requested served-model name, and keeps at most `maximumWaiting` calls in memory while GPU capacity starts. Its Prometheus gauge `ngkg_inference_waiting_requests` exists even with zero GPU pods, so KEDA can create `ngkg-vllm-backend` pods. Pending pods then activate Karpenter, Cluster Autoscaler or the provider node provisioner.

Each GPU pod contains vLLM and `ngkg-vllm-pod-agent`. vLLM binds only `127.0.0.1`; the pod agent is the sole network endpoint. It verifies `/health` and `/v1/models`, exposes readiness only for the exact configured model, bounds concurrency and response bytes, and never retries a POST after an ambiguous failure. Its admin listener binds only `127.0.0.1`. The pre-stop hook marks the endpoint unready, rejects new work, and waits for admitted calls while the vLLM container remains alive for the drain interval.

Tensor parallelism uses all GPUs allocated to one pod and Helm fails unless `tensorParallelSize`, the GPU request and GPU limit are equal. Kubernetes scales replicas across nodes and spreads them by zone and hostname. This phase does not claim that one model instance spans several nodes; a model larger than one qualified node requires a separately qualified vLLM distributed-executor profile.

## Prerequisites

Install Metrics Server, Prometheus, KEDA and an infrastructure node autoscaler. Install the provider-supported NVIDIA driver/device plugin and verify that nodes publish `nvidia.com/gpu`. Provision an isolated GPU pool with the label `ngkg.io/accelerator=nvidia-gpu` and taint `ngkg.io/gpu=true:NoSchedule`. See `deploy/gpu-node-provisioning/README.md` and use one of the four chart profiles.

Pin both application and vLLM images by `sha256:` digest. For predictable scale-from-zero, place the approved model on an immutable ReadOnlyMany PVC or a provider cache that is prepared before readiness. Remote downloads require workload identity, a mounted secret if the repository is private, adequate bounded ephemeral storage, and explicit `modelSourceEgressIpBlocks`; credentials never belong in values files.

## Helm example

Create a private values file:

```yaml
image:
  repository: registry.example.com/ngkg/agents
  digest: sha256:REPLACE_APPLICATION_DIGEST
vllm:
  enabled: true
  image:
    repository: vllm/vllm-openai
    digest: sha256:REPLACE_VLLM_DIGEST
  model: /models/approved-model
  servedModelName: approved-model
  tensorParallelSize: 2
  modelCache:
    enabled: true
    existingClaim: approved-model-rox
    mountPath: /models
    readOnly: true
  resources:
    requests: {cpu: '8', memory: 64Gi, nvidia.com/gpu: '2'}
    limits: {cpu: '16', memory: 128Gi, nvidia.com/gpu: '2'}
  autoscaling:
    minReplicas: 0
    maxReplicas: 16
serviceMonitor:
  enabled: true
```

Install with the provider profile, for example:

```bash
helm upgrade --install ngkg-agents ./charts/ngkg-agents \
  --namespace ngkg --create-namespace \
  --values charts/ngkg-agents/profiles/eks-gpu.yaml \
  --values private-production-values.yaml \
  --atomic --timeout 30m
```

Use `aks-gpu.yaml`, `gke-gpu.yaml`, `rke-gpu.yaml`, or `rke2-gpu.yaml` on the other platforms. The provider overlay configures only scheduling contracts; infrastructure remains under the cloud/Rancher control plane.

## REST and metrics contracts

The CPU tier publishes `GET /health/live`, `GET /health/ready`, `GET /v1/status`, `POST /v1/chat/completions`, `GET /metrics`, and `GET /openapi.yaml`. `/v1/status` is instance-scoped and reports readiness, drain state, backend readiness, waiting/in-flight requests, monotonic counters, served model and observation epoch. It is internal operational state, not tenant data and not a semantic certificate. The immutable API definition is `contracts/inference-gateway-openapi.yaml`.

The GPU pod agent publishes live/ready, completions, metrics and its OpenAPI contract through the backend Service. The same contract documents `POST /admin/drain` with an operation-level loopback server; that operation is not exposed by the Kubernetes Service and listens only on `127.0.0.1`. The pod-agent contract is `contracts/vllm-pod-agent-openapi.yaml`.

KEDA activates on `sum(ngkg_inference_waiting_requests)` at one queued request, then uses queue pressure plus 80% CPU and 80% memory resource triggers. Prometheus must be deployed as an HA dependency because KEDA does not support its fallback mechanism for CPU/memory triggers. If the activation signal is unavailable, queued calls fail at the configured cold-start deadline rather than waiting forever. Scale-down is deliberately slow to avoid model reload thrash.

## Qualification

Run `python3 qualification/validate_vllm_gpu_phase9.py` for deterministic source/config validation. On every supported provider, render and server-side validate the chart, start with the backend at zero, then run `qualification/run_phase9_gpu_e2e.sh`. Capture KEDA status, node creation time, model-ready time, completion response, pod/node placement, drain behavior and scale-down evidence. A source candidate is not provider-qualified until those live artifacts pass for the pinned Kubernetes, KEDA, driver, vLLM image, GPU type and node-provisioner versions.
