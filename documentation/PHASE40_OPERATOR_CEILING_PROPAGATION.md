# Phase 40 operator ceiling propagation

The immutable `ngkg-platform` Phase 40 ConfigMap is imported into both operator Deployments with `envFrom`. `ngkg-operator-core::Phase40DirectCeilings` parses all ten values, verifies candidate/partition coverage and reviewed hard caps, and computes the same `ngkg-phase40-reference-worker-ceilings-v1` digest used by the reference worker.

Generated reference/reasoner Jobs receive all ten `NGKG_PHASE40_DIRECT_*` values plus `NGKG_PHASE40_DIRECT_CEILINGS_SHA256`. Job and Pod annotations carry `ngkg.io/phase40-direct-ceilings-sha256`, and the work-spec hash includes the full ceiling object and digest. This makes a change in semantic resource policy a workload identity change rather than an invisible mutable setting.

The distributed operator only adds the exact-reasoner bundle to `Stage::Reasoner`; its plan, projection, reducer, artifact, and serving-root stages retain their existing resource contracts.
