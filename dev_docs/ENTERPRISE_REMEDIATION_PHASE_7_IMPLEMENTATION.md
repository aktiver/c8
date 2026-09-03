# Enterprise Remediation Phase 7 implementation

Source subject: Enterprise Stabilization Phase 6 candidate  
Plan: `NGKG-ENTERPRISE-REMEDIATION-2026-09-01`  
Release posture: source implementation candidate; production qualification remains blocked until the controlled live matrix passes.

## Implementation order

1. Freeze and migration repair.
2. Evidence trust and append-only workflow.
3. Native/oracle differential correctness.
4. Capacity, autoscaling, topology, GPU, and tenant evidence.
5. File-backed mmap and cgroup-safe concurrency.
6. Kubernetes network, PDB, PV, and five-provider safety.
7. Hermetic CI, local-registry OCI builds, chart/image parity, and release manifests.
8. Live Phase 3 and Phase 7 qualification.
9. MPI/OpenMP/Parquet optimization only after correctness gates remain green.

## File-level implementation map

| Concern | Implementation |
|---|---|
| Evidence paths and signatures | `phase6/scripts/evidence_security.py`, `phase5/verify_live_prerequisites.py`, `phase6/scripts/verify_and_issue.py` |
| Durable attempts and redaction | `phase6/scripts/phase6_common.py`, `phase6/scripts/qualify_provider.py` |
| Controlled hierarchy | `phase6/scripts/run_controlled.py`, `.github/workflows/phase6-controlled-release.yml` |
| Semantic differential | `phase6/scripts/run_differential.py`, `phase6/tests/test_differential.py` |
| Real artifact verification | `phase6/scripts/verify_reproducible_build.py`, `phase6/scripts/verify_supply_chain.py` |
| Five-provider capacity | `phase6/scripts/qualify_provider.py`, Phase 3/6 schemas and workflows |
| File-backed context index | `ngkg-agents/crates/ngkg-context-slice/src/index.rs` |
| Bounded context broker | `ngkg-agents/services/context-slice-broker/src/main.rs`, context-slice Helm workload |
| cgroup v1/v2 | Both `ngkg-hpc-runtime/src/lib.rs` implementations |
| Network isolation | Agent component NetworkPolicies and core query/metrics ingress policy |
| PDB safety | Agent gateway, component, prompt, inference, and context-slice PDB templates |
| Source PV ownership | Core operator `ensure_import_volume` and finalizer cleanup |
| Forward-only migration | Restored frozen 0002/0006 plus `0011_forward_only_contract_repairs.sql` |
| Local OCI registry | `docker_repos/` Dockerfiles, catalog, builder, generated digest values, and validation |
| CI closure | Phase 6 workflow, exact Python environment check, both Rust workspaces, every chart |

## Correctness and HPC rules

The online gateway, operators, and control services do not run MPI. Durable Kubernetes workers remain the fault and scaling boundary. Rayon, OpenMP, and BLAS receive one cgroup/cpuset-derived local budget and must not run nested pools. MPI is reserved for finite, gang-scheduled batch qualification or partition kernels. Every MPI rank must emit a rank/partition receipt, and rank zero must reduce receipts in canonical ordinal order before committing a semantic root.

Parquet optimization is allowed only in the native partition runtime: projection, predicate and row-group pruning feed bounded Arrow batches; spill and checkpoint paths remain execution-owned. Benchmark work must compare the same snapshot, authorized graph set, datatype policy, imports, and query hash. An optimization that changes multiset or graph canonicalization is rejected.

HPA resource metrics use 80% CPU and 80% memory targets. Kubernetes uses the maximum recommendation, implementing CPU-or-memory scaling. KEDA scales durable queues, while provider node autoscalers respond to pending pods with explicit requests. Application code never directly creates cloud nodes.

## Qualification barrier

This source tree must not issue a production certificate until all of these are observed on the final digest lock:

- controlled multi-architecture build, SBOM, scan, Cosign signature, and provenance for all 12 images;
- database migration tests from empty, frozen GA, interrupted/retry, and preflight states;
- isolated native and oracle services with equal semantic identity headers and independently canonicalized results;
- exact warmup/measured cardinality and saturation duration;
- Kubernetes/cgroup/container-runtime or Prometheus resource observations;
- HPA/KEDA/node-provisioner event chains at the 80% thresholds;
- real GPU allocation, GPU time, scale-from-zero, health, drain, and tenant-negative tests;
- RKE, RKE2, EKS, AKS, and GKE HA/chaos/storage/recovery scenarios; and
- no open release-blocking row in `phase7/defect-ledger.json`.

## Plan ahead: Phases 8–10

Phase 8 may introduce MPIJob templates, rank barriers, OpenMP kernels and additional native Parquet pruning only after Phase 7 correctness passes. Phase 9 executes the signed five-provider live matrix with real PostgreSQL, object stores, autoscalers and GPUs. Phase 10 performs two isolated reproducible builds, seals the complete evidence graph, freezes compatibility decisions and creates the final release artifact. No source-only gate substitutes for those phases.
