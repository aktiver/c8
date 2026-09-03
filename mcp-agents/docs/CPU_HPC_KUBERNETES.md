# CPU Kubernetes and HPC work plane

Phase 8 separates the public gateway, managed orchestrator, memory API, tool broker, prompt compiler and qualification worker into independently scalable Kubernetes workloads. The same checksum-bound HTTP binary uses `NGKG_COMPONENT_ROLE` to expose only its assigned REST routes. An optional Gateway API `HTTPRoute` keeps one public endpoint while sending agent, memory and tool paths to their dedicated services.

All CPU services scale on either 80% CPU or 80% memory utilization. The qualification worker can use ordinary HPA or a KEDA `ScaledObject`; KEDA adds queue-driven scale from zero while retaining the 80% CPU and memory triggers. Resource requests, selectors and tolerations are the provider-neutral contract with Cluster Autoscaler, Karpenter, GKE node auto-provisioning, AKS cluster autoscaler or an RKE/RKE2-supported node provisioner. Helm does not create cloud infrastructure.

## Distributed qualification lifecycle

1. Upload one or more immutable parts with the existing `/v1/agent-inputs` API and finalize the checksum manifest.
2. `POST /v1/qualification-workloads` with the frozen `inputId`, an idempotency key and explicit limits.
3. PostgreSQL creates tenant-RLS workload/partition rows and opaque queue rows in one transaction.
4. Workers use `FOR UPDATE SKIP LOCKED` through a narrow security-definer function. A lease can be recovered only after expiry and is bounded by `maximumAttempts`.
5. Each worker verifies the cloud object checksum before computation. The kernel uses a cgroup-sized Rayon pool, deterministic sort runs, a hard spill ceiling, atomic run files and a deterministic cross-partition Merkle-style root.
6. Heartbeats create immutable checkpoints and extend the lease. Completion is committed only by the current lease token. The workload becomes complete only after every contiguous partition result is present.
7. `GET /v1/qualification-workloads/{workloadId}` and `/checkpoints` expose progress and evidence without exposing object-store references.

`emptyDir.sizeLimit`, the kernel spill limit and the container memory limit are independent barriers. The worker reserves 30% of its cgroup memory by default for runtime/object-store overhead. Spill is scratch data; durable recovery uses PostgreSQL leases/checkpoints and immutable source objects.

## HPC policy

The Rust kernel uses Rayon rather than OpenMP. `OMP_NUM_THREADS`, BLAS and MKL thread counts remain one to prevent nested oversubscription in Kubernetes CPU cgroups. mmap is deliberately not used for cloud response buffers or variable-width scratch runs. It remains reserved for a later, measured context-broker implementation over checksum-verified immutable fixed-width local indexes.

Outputs must be checksum-identical with one or many cores and one or many nodes. A pod or node failure may repeat a partition but cannot publish two results for the same lease or expose a partially completed root.

## Required live qualification

Static chart/source checks do not prove production behavior. Release evidence must exercise Metrics Server, Prometheus/KEDA where selected, pending-pod node growth, CPU-only and memory-only 80% triggers, zone/node loss, lease recovery, duplicate delivery, spill exhaustion, object corruption, scale-down drain, PostgreSQL failover and checksum equality across RKE/RKE2, EKS, AKS and GKE.
