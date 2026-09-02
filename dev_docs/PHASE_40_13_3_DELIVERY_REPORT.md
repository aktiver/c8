# NGKG Phase 40.13.3 delivery report

## Outcome

This increment implements the next approved production-plan slice on top of Phase 40.13.2: a standards-driven, bounded-parallel SPARQL conformance foundation. It does not claim full SPARQL, online HermiT completeness, multinode production readiness or 100% Kubernetes qualification.

The native workspace builds and all 196 Rust tests pass. The pinned official test suite now yields an actionable baseline: 644 passes, 51 real executable failures and 107 explicitly unsupported cases across 802 inventoried cases. All 357 TriG cases pass. The SPARQL query/results subset passes 287 of 338 cases.

## Code delivered

- Replaced per-case `cargo run` execution with one prebuilt Rust driver shared by a bounded worker pool.
- Added cgroup quota/cpuset/affinity-aware job sizing, coordinator CPU reservation, per-case deadlines, isolated work directories, captured-output ceilings, deterministic result ordering and atomic report publication.
- Forces OpenMP, OpenBLAS, MKL, BLIS, NumExpr and Rayon to one lane per child so process-level case parallelism cannot oversubscribe the pod.
- Rejects manifest inputs that escape the pinned suite root or resolve through symlinks.
- Added base-IRI-aware SPARQL compilation and RDF fixture loading.
- Added official TriG dataset evaluation using base-IRI-aware parsing and blank-node canonicalization; all 143 evaluation tests and all 214 syntax tests pass.
- Added SPARQL XML/CSV/TSV and RDF result-set normalization, removing harness-induced false failures.
- Entailment-regime cases are detected and classified as unsupported until exact reasoning is actually dispatched; they can no longer accidentally pass under simple RDF semantics.
- Added a schema-validated, evidence-linked SPARQL functional matrix that separately reports parser, scalar reference, distributed and online maturity.
- Added an acceptance gate and reproducible Python dependency lock for the conformance toolchain.

## HPC and Kubernetes relationship

The conformance executor now obeys the same resource-discipline principles as the serving plane: bounded concurrency, cgroup awareness and no nested native oversubscription. Phase 40.13.2 already supplies CPU/memory/admission-pressure HPA inputs for query, fragment and hydration replicas. This increment preserves and statically verifies that contract rather than duplicating it.

Autoscaling is still not considered qualified. A real cluster must prove the custom-metrics adapter, HPA decisions, cluster-autoscaler/node-provisioner response, topology/locality behavior, drain fencing, retry idempotency and semantic equality while pods move across nodes.

## Exact conformance backlog

The remaining 51 executable query/results failures are grouped as follows:

- aggregates: 14
- functions: 8
- casts: 6
- CSV/TSV results: 6
- EXISTS: 5
- property paths: 4
- BIND: 2
- JSON results: 2
- positive syntax edges: 2
- VALUES/GRAPH: 1
- MINUS/GRAPH: 1

The full locked inventory also exposes 70 entailment-regime cases and 37 protocol/service-description cases that need purpose-built execution paths.

## Next coding order

1. Close the 51 scalar/reference SPARQL failures by feature family, keeping the official report at zero regressions after each patch.
2. Connect OWL Direct entailment selection to the exact HermiT adapter and make the 70 entailment cases executable with certified failure/completeness semantics.
3. Differentially qualify distributed active-dataset semantics and algebra operators against the scalar engine: joins, OPTIONAL, MINUS, UNION, grouping/aggregates/modifiers, then property-path frontier execution.
4. Add allowlisted, SSRF-safe, deadline- and byte-bounded `SERVICE`, followed by protocol and service-description harnesses.
5. Run multinode storage/join/failure tests and live Kubernetes metrics/HPA/node-autoscaler qualification before any production claim.

Ontology alignment is absent and remains explicitly outside the database product.
