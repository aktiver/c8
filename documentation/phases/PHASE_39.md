# Phase 39 — Exact scalar SPARQL 1.1 algebra and query forms

## Objective

Phase 39 turns the Phase 38 typed compiler into a form-aware exact scalar execution reference. The pinned SPARQL 1.1 evaluator consumes the already parsed algebra; NGKG does not reproduce SPARQL semantics with SQL approximations. The exact path is the correctness oracle that later semantic indexes and native/distributed kernels must match.

## Query and algebra coverage

The exact scalar path accepts the four query result forms produced by SPARQL 1.1 Query: `SELECT`, `ASK`, `CONSTRUCT`, and `DESCRIBE`. The typed evaluator handles basic graph patterns and joins, `OPTIONAL`, `UNION`, `MINUS`, `FILTER`, `BIND`, `VALUES`, subqueries, grouping and aggregates, `DISTINCT`, `REDUCED`, solution modifiers, and property paths. SPARQL multiset behavior, unbound variables, and expression errors are retained by the standards evaluator rather than rewritten as SQL set/NULL semantics.

Remote `SERVICE` execution and volatile result-generating functions remain fail-closed under the certified-query policy. Consequently the public full-SPARQL standards flag remains disabled until the later qualification boundary explicitly covers any admitted extension to that policy.

## Form-specific correctness certificates

Certified query records use result hash version 2:

- `SELECT` without top-level `ORDER BY`: exact RDF-term multiset.
- `SELECT` with top-level `ORDER BY`: exact certified sequence.
- `ASK`: exact Boolean.
- `CONSTRUCT` and `DESCRIBE`: RDF graph equality after RDFC-1.0 canonicalization using SHA-256.

The legacy SELECT multiset hash is retained only as an optional compatibility field for the already proven distributed SELECT fragment/shuffle path. It is not used as a generic graph or Boolean result identity.

## Enterprise bounds and cancellation

Each snapshot certifies maximum solution rows, graph triples, and graph-result blank nodes. Deployment configuration can only tighten those limits. Online exact evaluation uses a cooperative Oxigraph cancellation token and an operator-configured query timeout; timeout produces HTTP 504 and cannot be converted to a complete result. RDFC graph canonicalization has its own blank-node ceiling because canonicalization cost can rise sharply for adversarial blank-node structures.

Helm supplies:

- `onlineServing.maxQueryResultRows`
- `onlineServing.maxQueryGraphTriples`
- `onlineServing.maxQueryGraphBlankNodes`
- `onlineServing.queryTimeoutSeconds`

The same explicit contract is injected into every online role. `maxQueryResultRows` cannot exceed the distributed intermediate-row ceiling in the validated deployment profile.

## HPC and autoscaling boundary

Phase 39 does not replace the Phase 20–38 HPC machinery. Exact scalar evaluation remains bounded inside a pod; HPA grows query/fragment/hydration replicas and the RKE2 Cluster Autoscaler grows matching node pools. Long-running offline jobs remain Kueue-owned. The existing Arrow IPC, checksum-verified NVMe spools, partitioned shuffle, bounded Grace hash joins, Parquet hydration, caches, and tenant admission remain active.

The distributed fast path stays SELECT-only and can be selected only when Phase 38 emitted a typed constant-`GRAPH` inner-join decomposition whose final multiset was proven identical to scalar execution. Phase 39 operators are not independently parallelized until their equivalence tests exist; C++ remains Phase 42 work.

## Protocol representations

`SELECT` and `ASK` negotiate W3C SPARQL result JSON, XML, TSV, and CSV. `CONSTRUCT` and `DESCRIBE` negotiate Turtle, N-Triples, or RDF/XML. The enriched query API carries `queryForm`, optional `booleanResult`, and canonical `graphNtriples` in addition to SELECT bindings.

## Qualification gates

Phase 39 is not production-qualified until inherited gates and `scripts/qualify_phase39.sh` succeed. At minimum this requires the pinned Rust toolchain, Cargo lockfile, Rust tests, applicable W3C query/result tests at the later conformance gate, Maven reasoner validation, Helm rendering, Kubernetes server dry-run, authorization/failure testing, and live RKE2 autoscaling qualification. Missing toolchains or skipped gates never count as a pass.
