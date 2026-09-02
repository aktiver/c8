# NGKG Phase 40.13.8 delivery report

## Outcome

Phase 40.13.8 adds the typed distributed-SPARQL algebra foundation on top of Phase 40.13.7. It is
a source candidate, not a complete-distributed-SPARQL or production qualification claim.

## Delivered

- Every parsed query form compiles into a checksum-bound post-order DAG covering BGPs, paths,
  joins, lateral joins, OPTIONAL, UNION, MINUS, FILTER, BIND/extend, VALUES, GRAPH, projection,
  DISTINCT, REDUCED, grouping, ordering, slicing, subqueries, SERVICE, and ASK/CONSTRUCT/DESCRIBE
  finalization.
- Every stage declares its execution lane, inputs, stable partition count, row ceilings, Arrow
  exchange ceiling, spill ceiling, and algebra SHA-256.
- Deterministic topological waves expose independent branches and all logical partitions for
  bounded concurrent scheduling across cores and fragment-worker pods.
- Lane validation prevents expression evaluation, aggregation, RDF-term ordering, subqueries,
  paths, and graph finalization from silently entering a home-grown native kernel.
- Exact native SPARQL JSON multiset kernels implement JOIN, bag UNION, expression-free OPTIONAL,
  MINUS including disjoint-domain behavior, DISTINCT, projection, permitted REDUCED preservation,
  VALUES, and global slicing.
- Complete groups receive stable partition owners, including unbound group keys. The scalar oracle
  owns aggregate evaluation within each complete group.
- Ordered ranges use the scalar evaluator's comparator and a bounded k-way merge; native code does
  not invent an RDF-term collation.
- Every successful stage requires all partitions with matching query/plan/stage identities and
  verified output checksums. Missing, duplicate, foreign, incomplete, or checksum-invalid results
  fail closed.
- Ordinary exact online queries now checksum-bind the complete algebra plan, stage count,
  dependency-wave count, and logical work-item count alongside HermiT proof evidence.
- The fragment-worker HPA adds algebra/shuffle admission backlog and active spill pressure to its
  CPU and memory signals. Existing whole-CPU, OpenMP/BLAS, Arrow streaming, NVMe spill, anti-affinity,
  and node-pool contracts remain in force.
- OpenAPI documents the exact-entailment and distributed-algebra evidence envelope.

## Qualification executed here

- Parent archive integrity and all 867 parent payload hashes: passed.
- Phase 40.13.1 through Phase 40.13.8 static contracts: passed.
- Control-plane and online-data-plane OpenAPI route parity: passed.
- Cargo manifest, Cargo.lock dependency closure, JSON, OpenAPI YAML, and Helm values/profile YAML
  parsing: passed.
- Phase 40.13.8 Python syntax contract: passed.

## Blocking gates not executable here

- Rust formatting, compilation, Clippy, and native tests: Cargo/Rust are absent.
- HermiT adapter Maven build/tests: Maven is absent.
- Helm lint/render: Helm is absent.
- Worker-RPC activation and differential equality for every distributed operator.
- The complete applicable W3C entailment suite and the zero-failure scalar corpus in the same build.
- Live multinode HPA, node autoscaler, skew, spill, retry, pod-drain, and deterministic-answer tests.

Until those gates pass, optimized algebra execution must remain evidence-gated and fall back to the
qualified scalar path. Phase 40.13.9 property-path frontier work must not be treated as a substitute
for completing this qualification.

No ontology-alignment or raw-data-mapping database functionality was added.
