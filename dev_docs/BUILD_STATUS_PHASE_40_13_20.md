# Phase 40.13.20 build status

Status: **source-implemented candidate; live production qualification pending**

Parent archive: `NGKG_PHASE_40_13_19_CANDIDATE(1)(2).zip`  
Parent SHA-256: `1475461b64344fa611e9c603f55d2c4c105b4a6d7cf3d1b2326400615bed3dd0`

## Implemented gates

- Exact 80-percent CPU-or-memory autoscaling policy.
- Scale-from-zero and pending-pod demand decisions.
- Checkpoint/spill-safe scale-down decisions.
- Deterministic result/artifact qualification barrier.
- Cgroup-v2 memory and multipart-buffer admission.
- Generic/RKE/RKE2/EKS/AKS/GKE node-provisioner overlays and cross-field validation.
- Read-only S3/Azure Blob/GCS CSI source mounts with workload identity and driver discovery.
- JSON contracts, threshold corpus, live evidence collector, and static acceptance gate.

## Required external gates

- Cargo check, Clippy, Rustfmt, and all native tests with Rust 1.97.1.
- Helm lint/render and Kubernetes server-side validation.
- Metrics Server and custom metrics API observations.
- Kueue admission and selected provider node-provisioner status.
- Selected S3, Azure Blob, or GCS CSI mount and immutable TriG manifest ingestion.
- Real 79-to-80 percent CPU and memory threshold exercises.
- Scale-from-zero for every eligible batch pool.
- Pod/node loss, checkpoint replay, and drain qualification.
- Baseline/scaled/retried result and artifact-root checksum equality.

No release or production qualification claim is enabled by static evidence alone.

The cumulative structural validator still reports the parent candidate's unresolved-token findings
in a Phase 40.13.16 report and vendored Oxigraph/spareval/sparopt sources. No Phase 40.13.20 file
contains such a token; the inherited findings remain release blockers rather than being rewritten.
