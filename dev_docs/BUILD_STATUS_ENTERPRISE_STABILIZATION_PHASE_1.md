# Enterprise Stabilization Phase 1 build status

This phase repairs the first deployability and enterprise-configuration defects identified by the 29 August 2026 audit. It is a stabilization source candidate, not a production qualification certificate.

Implemented:

- Restored the checksum-bound root and vendor Cargo lockfiles.
- Corrected the ingestion target snapshot UUID contract.
- Added the previously unreachable cloud-import scopes to both authorizers and the token schema.
- Enforced graph authorization before fragment semantic state and execution.
- Expanded the operation immutability trigger to all updates.
- Made the named-dataset backfill safe for a table-owner migration under forced tenant RLS.
- Corrected storage-recovery Merge status patch parameters and added bounded writable `/tmp`.
- Aligned the workloads values schema with the values consumed by templates.
- Made empty dependency/federation egress configuration fail explicitly.
- Content-addressed immutable ceiling ConfigMaps so Helm upgrades create new objects and roll consumers safely.
- Made HPA minReplicas the rendered replica owner for query, fragment and hydration workloads.
- Preserved 80% CPU and RAM scaling thresholds in an enterprise-secure Helm overlay.
- Filtered reference-worker arguments to the worker's command-specific allowlist.
- Added reproducible image build orchestration, a Docker context policy and Java-capable reasoner images.
- Added a pinned, least-privilege enterprise CI workflow.

Validation available in this workspace:

- Source and configuration gates.
- JSON/YAML parsing.
- OpenAPI/route parity.
- SHA-256 manifests.

Environment-blocked validation:

- Native Rust formatting, compilation, Clippy and tests.
- Helm lint/template.
- Container builds.
- PostgreSQL migrations against non-empty tenant data.
- Live Kubernetes, HPA/KEDA, node-autoscaler and failure testing.

The separate `ngkg-agents` workspace still needs a generated and reviewed `Cargo.lock`; it was never present in the supplied Phase 10 archive and cannot be safely fabricated without Cargo dependency resolution. Phase 2 must close that build gate and execute native CI before any distributed-runtime redesign begins.
