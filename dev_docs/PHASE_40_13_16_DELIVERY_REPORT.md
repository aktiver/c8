# Phase 40.13.16 delivery report

## Outcome

This candidate activates a distributed, checksum-bound scalar-oracle execution lane for ordinary
`/sparql` and `/query` requests. Deterministic non-federated SELECT, ASK, CONSTRUCT, and DESCRIBE
queries—including OPTIONAL, UNION, MINUS, FILTER/BIND, grouping/aggregation, DISTINCT/REDUCED,
ordering, slicing, and subqueries—can execute concurrently on distinct fragment-worker pods. The
coordinator publishes no answer until the dense replica set is complete and canonically equal.

Existing exact native Arrow fragment and Grace-shuffle joins remain unchanged. Volatile functions
retain one uncached scalar query context, and secured SERVICE execution remains later federation
scope. No ontology alignment, schema matching, or raw-data mapping was added.

## Production logic added

- authenticated internal `POST /v1/datasets/{datasetId}/algebra/{querySha256}/{replica}/execute`;
- original-query, rewritten-query, snapshot, manifest, graph authorization, active-dataset, limit,
  and replica checksum bindings;
- canonical SELECT multiset/order, ASK Boolean, and RDF graph-result comparison;
- distinct-worker and dense-replica barriers with fail-closed timeout/mismatch handling;
- normal exact-query routing to the distributed lane when deterministic and non-federated;
- cloud activation hash as the semantic serving identity when no hydration layout exists;
- bounded request/response bytes and cgroup-aware fragment-worker execution;
- Helm configuration requiring at least two workers and enforcing concurrency ceilings;
- algebra/shuffle admission metrics feeding backlog, spill, CPU, and memory HPA signals;
- OpenAPI, JSON Schema, live equivalence corpus, cluster qualification script, and acceptance gate.

## Qualification executed here

- Phase 40.13.16 static contract: passed.
- Phase 40.13.8–15 cumulative static contracts: passed.
- REST/OpenAPI parity: 16 control-plane and 17 online operations, passed.
- Base and production-overlay workload values: passed.
- JSON/TOML and non-template YAML parsing: passed.
- Shell syntax for the new qualification scripts: passed.

## Open gates

Cargo, rustc/rustfmt, Maven, Helm, and kubectl were unavailable. Native formatting, compilation,
Clippy, Rust tests, HermiT Maven tests, Helm rendering, live fault injection, HPA/node scaling, and
multinode differential execution were therefore not run.

The worker lane still opens the Phase 40.13.15 scalar compatibility image. It does not yet execute
leaf scans directly from all Phase 40.13.12 semantic partitions, so the 64 GiB scalar-image limit
and the 500 GB partition-native activation requirement remain production blockers. The structural
repository scanner also continues to flag inherited TODO tokens inside vendored upstream Oxigraph
and spareval/sparopt sources.
