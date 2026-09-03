# Enterprise Remediation Phase 8 implementation

Source subject: Enterprise Remediation Phase 7 candidate  
Milestone: MPI/OpenMP/Parquet HPC optimization after correctness closure  
Release posture: source implementation candidate; all optimized paths remain opt-in until the controlled live evidence matrix passes.

## What Phase 8 changes

Phase 8 adds a finite, deterministic HPC lane without changing NGKG's semantic authority. Kubernetes and Kueue admit a fixed MPI gang, MPI assigns immutable Parquet partitions across nodes, and each rank uses exactly one local compute owner: either bounded Rust threads or bounded OpenMP threads. Parquet projection and row-group statistics reduce I/O, but the ordinary Rust predicate and authorization checks remain the final semantic check.

The gateway, public query coordinator and long-lived operators do not join MPI communicators. Those services keep their existing HPA/KEDA behavior and scale at 80% CPU or memory. MPI ranks are fixed for the life of one admitted job; the cluster autoscaler adds nodes for pending gang members rather than changing the active MPI world size.

## Execution contract

1. A controller creates an immutable `HpcRunPlan` bound to the published snapshot, semantic root and authorized graph-set hashes.
2. Kueue admits the complete MPIJob. One rank is placed per node by the default topology contract.
3. The native launcher normalizes rank, world-size and local-rank environment values and invokes the Rust worker.
4. Rank `r` executes only partitions whose ordinal satisfies `ordinal % rankCount == r`.
5. Each input file is absolute, regular, non-symlinked, size-bound and SHA-256 verified before Parquet decoding.
6. Projection and sound min/max statistics pruning select the minimum required columns and candidate row groups.
7. Arrow batches remain bounded by the plan and the live cgroup memory envelope. OpenMP uses a deterministic static schedule and cannot run as a nested thread pool.
8. Every rank publishes a no-overwrite, checksum-bound receipt. Rank zero accepts only the exact dense rank and partition sets before producing the final certificate.

## File map

| Area | Files | Responsibility |
|---|---|---|
| Cgroup and MPI admission | `NGKG_1_0_0_GA/crates/ngkg-hpc-runtime/src/lib.rs` | Detect cgroup v1/v2 CPU/RAM, enforce the 80% envelope, validate one-owner thread budgets, normalize MPI rank identity and admit bounded Arrow buffers. |
| Parquet/OpenMP runtime | `NGKG_1_0_0_GA/crates/ngkg-native-runtime/src/lib.rs`, `openmp.rs` | Project required columns, prune row groups, stream bounded Arrow batches and optionally invoke the reviewed OpenMP kernel through a strict binary protocol. |
| MPI receipts | `NGKG_1_0_0_GA/crates/ngkg-native-runtime/src/hpc.rs` | Validate immutable plans, assign partitions, run local lanes, verify dense rank completion and publish deterministic certificates without overwriting conflicts. |
| Worker commands | `NGKG_1_0_0_GA/services/distributed-worker/src/main.rs` | Implement `hpc-parquet-rank` and `hpc-parquet-finalize` with resource admission and durable receipt output. |
| Native kernels | `hpc/native/ngkg_mpi_exec.c`, `hpc/native/ngkg_openmp_filter.c` | Supply the MPI collective boundary and deterministic OpenMP predicate kernel. |
| Kubernetes gang | `NGKG_1_0_0_GA/charts/ngkg-platform/templates/hpc-mpi.yaml` | Define opt-in MPIJob, Kueue queue, security context, RWX work volume, resource limits and topology-aware scheduling. |
| OCI build | `docker_repos/ngkg-hpc-worker/Dockerfile`, `docker_repos/build_all_local.sh` | Build the Rust worker and both native kernels, push all 13 images to the node-reachable registry and generate digest-pinned Helm values. |
| Contracts | `NGKG_1_0_0_GA/contracts/hpc-*.schema.json` | Define strict plans, rank receipts and run certificates. |
| REST/Swagger | `NGKG_1_0_0_GA/services/online-serving/src/main.rs`, `NGKG_1_0_0_GA/api/online-openapi.yaml` | Expose authenticated runtime capabilities and document the extended native leaf response. |
| Acceptance | `phase8/` | Check image parity, strict schemas, MPI/OpenMP invariants, Parquet optimization markers and every Swagger operation description. |

## Local-registry build and Helm use

Copy `docker_repos/example.env`, replace every example with a reviewed digest-pinned base image, and source it. Set `NGKG_LOCAL_REGISTRY` to an authority reachable from every Kubernetes node; `localhost` is rejected by default because each node has its own loopback interface. Then run:

```bash
./docker_repos/build_all_local.sh
```

The script pushes all 13 images, resolves the registry manifest digest for each image and emits three values files under `docker_repos/generated/`. Install the charts with the corresponding generated values file so every workload uses `repository@sha256:digest` and `IfNotPresent`; the chart never depends on a workstation-only image cache.

## REST and Swagger behavior

`GET /v1/hpc/capabilities` is an authenticated read-only route on the query service. It reports effective cgroup CPU and memory, thread ownership, available Rust/OpenMP leaf modes, the 80% saturation ceiling and the rule that MPI is batch-only and non-elastic during an active run. It does not submit work, disclose cluster-wide capacity or grant access to a graph.

The native leaf request now accepts `executionMode: "rust" | "openmp"`; `rust` is the default. Its response distinguishes logical rows considered, physical rows decoded, row groups pruned and rows pruned so performance claims can be independently reconciled. All 113 shipped operations have three- or four-sentence Swagger descriptions, schema-linked payloads and a generated catalog at `NGKG_1_0_0_GA/docs/API_ROUTE_CATALOG_PHASE8.md`.

## Required production evidence

Phase 8 is not qualified by source checks alone. The final digest lock must prove Rust compilation, Clippy and tests; Helm lint/render against the installed MPI Operator and Kueue CRDs; all 13 OCI builds, SBOMs, scans and signatures; Rust-versus-OpenMP differential equality; MPI rank/node identity; node-loss and cancellation behavior; cgroup memory ceilings; row-group pruning correctness; throughput and saturation gains; and RKE, RKE2, EKS, AKS and GKE execution. A failed or missing result keeps `hpc.enabled=false` and the ordinary bounded Rust lane active.

## Plan ahead

Phase 9 executes the signed five-provider live matrix using the exact Phase 8 image lock, including PostgreSQL HA, three-cloud object storage, MPI/Kueue, 80% HPA/KEDA/node scaling, GPU isolation, recovery, chaos and tenant-negative tests. Phase 10 then performs the final two-builder reproducibility run, closes the evidence graph, freezes compatibility decisions and emits the signed release candidate. Two phases remain after this source milestone.
