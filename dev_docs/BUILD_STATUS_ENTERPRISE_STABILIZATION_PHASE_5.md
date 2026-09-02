# Build Status — Enterprise Stabilization Phase 5

Status: source implemented; production qualification blocked on controlled infrastructure.

## Implemented

- New Rust-only `ngkg-native-runtime` workspace crate with no Oxigraph/reference-runtime dependency.
- Fail-closed native plan and OWL coverage admission; scalar-oracle stages are forbidden in required mode.
- Checksum- and byte-bound Parquet leaf scans with Arrow batch bounds, server-derived graph authorization, cancellation, full-partition row evidence, and no partial success.
- Exact partition completion barrier with checked resource totals, dense partition coverage, idempotent duplicate delivery and conflicting-retry rejection.
- Native Rust finalization for exact OWL BGP SELECT relations over `JOIN`, `UNION`, `MINUS`, `PROJECT`, `DISTINCT`, `REDUCED`, and `SLICE` bag operators.
- Internal authenticated `POST /v1/datasets/{datasetId}/native/leaves/{querySha256}/{partition}/scan` REST route and complete Swagger/OpenAPI schemas.
- `disabled`, `shadow`, and `required` Helm cutover policy wired into query, fragment, locator, and hydration roles.
- Enterprise overlay selects `required`; general defaults remain `shadow` until live evidence exists.
- Fail-closed Phase 5 source gate and production prerequisite verifier.

## Executed on this runner

- Input ZIP integrity and all 1,426 original bundle hashes: passed before modification.
- Phase 4 cumulative source/contract gate: passed.
- Phase 5 source/contract gate: passed.
- Retained Phase 3 source acceptance and verifier unit test: passed.
- Cumulative MCP-agent static acceptance through context-slice Phase 10: passed.
- Online OpenAPI, Helm values, enterprise overlay and JSON Schema parsing: passed.
- Phase 5 Python compilation and shell syntax checks: passed.
- Negative production gate: passed by correctly rejecting a missing signed Phase 3 certificate.

## Not executable on this runner

Rust/Cargo, Helm, an OCI builder/scanner/signer, PostgreSQL HA and Kubernetes clusters are unavailable. Native compilation, formatting, Clippy, Rust tests, Helm lint/render, multi-architecture images and live RKE2/EKS/AKS/GKE qualification were not executed here.

Production qualification remains blocked until the controlled workflow emits all three signed artifacts: `phase3-certificate.json`, `phase4-live-certificate.json`, and `phase5-live-certificate.json`. Required Phase 5 evidence includes native/scalar differential results, exact-reasoning coverage, multi-node partition loss, bounded spill/checkpoint recovery, tenant isolation, and CPU/RAM HPA behavior at 80%.
