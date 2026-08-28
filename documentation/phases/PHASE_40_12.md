# Phase 40.12 — Online and distributed-fragment Phase 40 ceiling wiring

Phase 40.12 consumes the workload-side Direct-BGP admission ceilings declared in Phase 40.10.

## Runtime contract

`ngkg-workloads` renders `phase40.directAdmission` into the immutable `ngkg-phase40-online-ceilings` ConfigMap. Every `online-serving` role loads and validates this bundle at startup. The query role passes the trusted limits into the Phase 40.7 Direct-BGP classifier; fragment, locator, and hydration roles validate the same policy identity so one online release cannot run with divergent admission configuration.

The offline `ngkg-distributed-worker` performs ingestion/projection/artifact-build work and does not execute SPARQL Direct BGPs, so these admission ceilings are intentionally not applied to that binary.

## HPC rule

`maxClassificationCpuLanes` is a ceiling, not a thread request. Effective classifier lanes are:

`min(Helm ceiling, visible CPU parallelism, NGKG_RUST_COMPUTE_THREADS)`.

This prevents nested classifier threads from oversubscribing a constrained Kubernetes pod while preserving deterministic typed-BGP classification.

## Deferred work

Phase 40.13 remains responsible for operator/distributed-operator propagation of the exact-reasoner ceiling bundle into generated Kubernetes jobs and envelopes.
