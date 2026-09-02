# Phase 40.13.20 delivery report

Phase 40.13.20 implements the production autoscaling control and evidence boundary on top of the
Phase 40.13.19 multinode storage candidate.

## Delivered

- Executable Rust policy for CPU-or-memory scale-out at exactly 80 percent.
- Requested-versus-live resource charging to remain safe during metric lag.
- Scale-from-zero, maximum-node, pending-pod, and checkpoint/spill-safe drain decisions.
- Deterministic policy, decision, and qualification contracts.
- Cross-scale, retry, and node-loss semantic result and artifact-root equivalence requirements.
- Cgroup-v2 CPU/memory envelope and bounded-buffer admission in `ngkg-hpc-runtime`.
- Storage-recovery multipart admission against remaining memory inside the 80-percent envelope.
- Common production overlay plus validated generic, RKE, RKE2, EKS, AKS, and GKE provider overlays.
- External Cluster Autoscaler, Rancher Cluster Autoscaler, EKS Karpenter, AKS managed autoscaler,
  and GKE managed autoscaler discovery contracts.
- Read-only Amazon S3, Azure Blob, and GCS CSI TriG mounts with workload identity, registered-driver
  preflight, and the existing checksum-frozen cloud-source compiler handoff.
- KEDA hydration ScaledObject with CPU, memory, and pending-work triggers; the HPA template is
  mutually excluded for the same target.
- Cross-field Helm validation that rejects competing or incomplete autoscaling ownership.
- Checksum-bindable autoscaling policy ConfigMap.
- Read-only live cluster evidence collector.
- Threshold, scale-from-zero, drain, and node-loss qualification matrix.

## Qualification boundary

Static and configuration checks can run in this source environment. Native Rust, Helm render,
metrics APIs, provider node provisioning, selected cloud CSI mounting, Kueue admission,
scale-from-zero, node loss, checkpoint
replay, and deterministic-result tests require the production-equivalent multinode cluster. The live
certificate must remain incomplete until those events are observed.

## Next phase

Phase 40.13.21 is Enterprise Security and Operations: workload identity, tenant isolation,
encryption, network policy, audit trails, rate limiting, operational telemetry, SLOs, and disaster
procedures.
