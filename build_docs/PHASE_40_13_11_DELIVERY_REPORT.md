# Phase 40.13.11 delivery report

Phase 40.13.11 implements the cloud-source compiler handoff on the verified Phase 40.13.10 parent.
Existing bucket TriG files no longer stop at discovery: the source manifest now deterministically
produces complete-object decode work, an Indexed Kubernetes Job executes that work, and a finalizer
publishes a checksum-bound input for Phase 40.13.12 only after every expected artifact verifies.

## Runtime behavior

The planner uses deterministic largest-first balancing and never invents byte offsets inside TriG.
Each pod runs a bounded number of parser/upload lanes across complete objects, streams RDF through
Oxigraph's strict TriG parser into N-Quads, rechecks source file metadata/hash/quad count, and uploads
immutable fragments. Blank-node labels remain paired with a deterministic object scope so Phase
40.13.12 can build globally correct dictionaries without collisions between source documents.

Kubernetes completion indexes select immutable work IDs. Kueue controls batch admission; pending
`source-ingestion` pods create demand for the external node autoscaler; equal resource requests and
limits bound CPU, RAM, and scratch. OpenMP/BLAS/MKL are pinned to one because RDF parsing is not a
dense numerical kernel, while bounded Rust lanes and many Indexed completions provide core/node
parallelism.

The finalizer downloads every expected completion manifest, validates its exact plan/work identity,
streams every remote decoded fragment through SHA-256 verification, rejects gaps or duplicate source
ordinals, and publishes `compiler-handoff.json` last. No partial handoff can be observed as complete.

## Namespace and semantic boundary

The canonical roles are now:

- `https://c8-next-generation.io/<scope>/<subdomain>/semkg`
- `https://c8-next-generation.io/<scope>/<subdomain>/closure`
- `https://c8-next-generation.io/<scope>/<subdomain>/provenance`

This is ontology loading/compilation infrastructure, not ontology alignment. No raw-data mapper,
schema matcher, alignment graph producer, closure generator, OWL qualifier, or query publisher was
added. Only authorized asserted `semkg` graphs may enter the later OWL assembly stage.

## Honest qualification state

All executable local structural, compatibility, API parity, and serialization-syntax gates pass.
The current container lacks Cargo/Rust, Maven, Helm, kubectl, cloud CSI credentials, Kueue, and a
multinode cluster, so native compilation and live scaling/failure gates are blocked rather than
reported as passes. See `BUILD_STATUS_PHASE_40_13_11.md` and
`verification/phase-40.13.11-summary.json`.
