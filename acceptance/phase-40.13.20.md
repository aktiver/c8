# Phase 40.13.20 acceptance and remaining roadmap

Phase 40.13.20 is accepted only when the same immutable workload produces checksum-identical
semantic results and artifact roots before and after scaling, retry, and node loss.

## Prerequisite gates

1. Phases 40.13.10–40.13.19 cumulative source and acceptance contracts pass.
2. Rust 1.97.1 builds, formats, lints, and passes the complete native workspace test suite.
3. Helm lint/render and Kubernetes server-side validation pass for the common production overlay
   plus each claimed provider overlay.
4. The target cluster is HA and multinode; resource metrics, custom metrics, KEDA, Kueue, and the
   selected external or managed node provisioner are healthy.
5. CPU and memory requests equal limits, responsibility labels/taints are present, and cgroup v2
   exposes a finite memory limit and effective CPU set.
6. The selected S3, Azure Blob, or GCS CSI driver is registered; a workload-identity-backed,
   read-only TriG mount produces the expected checksum-frozen cloud-source manifest.
7. The 79/80-percent boundary, scale-from-zero, backlog, maximum-pool, safe-drain, node-loss,
   duplicate retry, checkpoint replay, and spill protection cases are observed live.
8. Baseline, scaled, and recovered semantic result hashes and artifact-root hashes are identical.
9. Backup/restore and Phase 40.13.19 recovery certificates remain valid under autoscaling.
10. The live collector emits `complete: true`; static source evidence alone cannot do so.

## Provider combinations

| Kubernetes | Node scaling | Native TriG source |
|---|---|---|
| RKE/RKE2 | Rancher Cluster Autoscaler | selected registered S3, Azure Blob, or GCS CSI driver |
| Amazon EKS | Karpenter | Amazon S3 CSI |
| Microsoft AKS | managed Cluster Autoscaler | Azure Blob CSI |
| Google GKE | managed Cluster Autoscaler | GCS FUSE CSI |
| Generic Kubernetes | external Cluster Autoscaler | selected installed and qualified CSI driver |

## Remaining planned milestones after Phase 40.13.20

1. Phase 40.13.21 — Enterprise Security and Operations.
2. Phase 40.13.22 — Standards and Differential Qualification.
3. Phase 40.13.23 — Performance and Capacity Qualification.
4. Phase 40.13.24 — Kubernetes Release Qualification.
5. Phase 1.0.0-RC1 — Final Release Candidate.
6. Phase 1.0.0 — General Availability.

There are six planned milestones after an accepted Phase 40.13.20. Until its external prerequisite
and live-cluster gates pass, seven milestones remain including completion of Phase 40.13.20 itself.
