# NGKG Phase 39 Build Status

Status: **implementation-candidate-not-production-qualified**. Workspace version: **0.7.0**.

Phase 39 is cumulative on Phase 38. Standards claims remain disabled until the full release qualification evidence is checksum-bound and all applicable conformance, build, security, and live RKE2 gates pass.

## Implemented in this candidate

- The exact scalar SPARQL path consumes the shared typed SPARQL 1.1 algebra and supports `SELECT`, `ASK`, `CONSTRUCT`, and `DESCRIBE` evaluator result forms.
- SPARQL 1.1 algebra such as joins, `OPTIONAL`, `UNION`, `MINUS`, `FILTER`, `BIND`, `VALUES`, subqueries, grouping/aggregates, ordering/slicing, and property paths is delegated to the pinned standards evaluator rather than reimplemented with SQL-like shortcuts.
- Result certification is form-aware and versioned: SELECT uses exact RDF-term bag/sequence semantics, ASK binds the exact boolean, and graph-producing queries are compared after RDFC-1.0/SHA-256 graph canonicalization.
- Graph canonicalization has an explicit blank-node ceiling because RDFC canonicalization can become computationally expensive on adversarial blank-node structures.
- Runtime result ceilings are checksum-bound in the snapshot and may only be tightened by deployment configuration.
- Exact scalar execution is cooperatively cancellable and wrapped in an operator-configured wall-clock timeout; timeout returns 504 and cannot emit a successful partial result.
- Query result row, graph triple, graph blank-node, and timeout bounds are deployment-configurable through Helm and injected into every online role.
- The existing distributed Arrow/NVMe/Grace-join fast path remains SELECT-only and is selected only when the typed Phase 38 compiler emitted a proven-equivalent distributed certificate.
- ASK uses W3C SPARQL result serializers. CONSTRUCT/DESCRIBE use negotiated Turtle, N-Triples, or RDF/XML serializers. SELECT continues to support JSON/XML/TSV/CSV.
- HPA, Kueue, bounded per-pod concurrency, and the RKE2 Cluster Autoscaler topology remain active so horizontal pod/node scaling complements rather than oversubscribes in-pod HPC execution.

## Qualification boundary

This candidate does **not** claim production SPARQL 1.1 compliance merely because the scalar evaluator path is implemented. The applicable W3C query/result suites, Rust build/tests, Maven reasoner tests, Helm rendering, authorization/failure tests, and live RKE2 autoscaling gates still have to execute successfully. `Cargo.lock` must be generated and verified by the pinned Cargo toolchain; it is never fabricated.

`SERVICE`/remote federation and volatile query functions remain subject to the existing fail-closed admission policy and therefore the public full-SPARQL standards flag remains disabled pending the later qualification boundary.

## Cumulative archive inheritance

The Phase 39 package carries checksum-bound ancestry for the exact Phase 38 candidate archive. `verification/archive-parent.json` binds the Phase 38 ZIP SHA-256 and embedded parent file manifest, and `scripts/verify_phase_inheritance.py` verifies that every Phase 38 payload file remains present. Any modified parent file must have exact old/new SHA-256 values declared; deleting a parent file is forbidden. Git-tag ancestry remains the preferred source-repository proof when `.git` is available.

## Inherited Phase 29 gate reconciliation

Phase 29 no longer keys cache safety to the obsolete SELECT-only test name. Its inherited gate now requires the Phase 39 form-aware cache regression (`query_cache_revalidates_form_aware_result_and_guid`) plus the v2 canonical payload/result certificate path. This is a strict supersession: cache replay must prove form-aware semantic equality and GUID identity before a hit can be served.
