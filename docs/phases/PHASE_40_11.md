# Phase 40.11 — reference-worker ceiling enforcement

Phase 40.11 makes the Phase 40.10 `ngkg-platform.phase40.direct` values consumable and enforceable by `ngkg-reference-worker` without advancing operator propagation early.

The chart renders the ten trusted values into an immutable reference-worker ConfigMap. The reference worker requires those values as environment variables for `direct-bgp` execution. A job may request a smaller per-execution budget, but it cannot raise candidate, partition, grounding, JVM lane/heap, or timeout limits above the trusted environment bundle.

The worker also enforces `maxExactPartitions`, `maxCertificateBytes`, and `maxProofSupportIds` inside the exact coordinator, so those limits cannot be bypassed by constructing `DirectExactLimits` from untrusted job JSON.

## HPC/resource invariants

* `reasonerConcurrency` cannot exceed CPU parallelism visible to the worker.
* When a finite cgroup memory limit is visible, concurrent JVM heap budget may not exceed 80% of it.
* Candidate space must fit within the configured maximum partition count.
* CPU count changes concurrency only; it never changes candidate ordinals or proof/certificate identity.
* Missing or malformed trusted Phase 40 environment causes the direct path to fail before ontology materialization or JVM launch.

Operator/distributed-operator injection of the ConfigMap remains Phase 40.13. Until then, a manually launched reference-worker pod/job must inject the immutable ConfigMap explicitly.
