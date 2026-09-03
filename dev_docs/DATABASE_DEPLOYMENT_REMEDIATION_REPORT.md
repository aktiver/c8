# NGKG database deployment remediation report

Date: 2026-09-02  
Input: `NGKG_MCP_AGENT_ENTERPRISE_REMEDIATION_PHASE_8_CANDIDATE(6).zip`  
Input SHA-256: `d6629d58f956c40e4a7157a6a3ee679dba39124524f38fb2e5db9acebac486d2`

## Scope

This remediation is confined to the NGKG database and its deployable supporting services. Upstream automation owns raw-data alignment, ontology mapping and OWL 2 DL certification. NGKG accepts the resulting TriG artifacts, validates their database contracts and checksums, builds distributed storage/query artifacts, publishes immutable snapshots and executes authorized SPARQL queries.

## Implemented fixes

- Replaced workspace-level `unsafe_code = "forbid"` contradictions with deny-by-default policy and two narrowly reviewed mmap exceptions.
- Removed first-party Clippy-denied `.expect()` calls and pinned both Rust workspaces to Rust 1.97.1.
- Made locked Cargo and Maven dependency fetching selectable for first-build online and cached offline modes.
- Corrected the controlled MPI image build inputs, provenance and HPC image-lock Helm overlay.
- Added private-registry pull-secret support to static and operator-generated workloads.
- Added schema-valid image pull policies and configurable service-account annotations for cloud workload identity.
- Corrected generated Helm values precedence and node-reachable registry checks.
- Repaired obsolete Phase 18, Phase 40.12 and Phase 40.13.21 static gates.
- Added `scripts/cluster_preflight.sh`, a read-only live-cluster prerequisite check.
- Added `scripts/database_api_smoke_test.py` for deployed readiness, Swagger/OpenAPI, immutable TriG upload and published-snapshot SPARQL testing.
- Corrected the deployment documentation to describe the explicit NGKG upload, ingestion and publication lifecycle without inventing an alignment or certification service.

## Validation completed

- Deployment static preflight: PASS.
- Phase 3 source acceptance: PASS.
- Phase 8 acceptance: PASS; 13 images and 113 cumulative OpenAPI operations catalogued.
- Phase 18, Phase 40.12 and Phase 40.13.21 static verification: PASS.
- Core structural validation: PASS; 1,214 files and zero errors.
- Runtime/OpenAPI parity: PASS; 19 control-plane and 22 online-plane operations.
- Database API smoke-test mock: PASS, including checksum-bound TriG and named-graph response checks.
- Shell and Python syntax validation: PASS.

## Execution boundary

This environment does not provide Rust/Cargo, Docker/Buildx, Helm, kubectl, MPI, a registry or a Kubernetes cluster. Consequently, native compilation, OCI construction, cluster-side image pulls, Helm installation and live distributed-query execution remain unexecuted release gates. The included preflight, build wrapper, runbook and smoke test are the executable path for collecting that evidence on the target infrastructure.
