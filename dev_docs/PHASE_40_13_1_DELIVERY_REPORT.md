# NGKG Phase 40.13.1 delivery report

## Outcome

This increment starts the production repair on the Phase 40.13 baseline. It is a source and native-test candidate, not a production release. Ontology alignment is not present and is not planned.

## Implemented

- Exact-reasoner partition merge now receives and enforces the trusted proof-support and certificate-byte ceilings, including boundary tests.
- Legal SPARQL `SERVICE` and volatile functions are parsed into typed algebra. Certification/cache eligibility is now a separate policy decision.
- Volatile queries can use the bounded uncached scalar route. `SERVICE` fails explicitly at the unavailable federated-execution capability boundary.
- Top-level `ORDER BY` detection no longer mistakes an ordered subquery for an ordered outer SELECT.
- Online roles validate one cpuset-aware Rust/blocking/OpenMP/BLAS/control-thread budget at startup.
- Kubernetes CPU/thread budgets were corrected for Guaranteed QoS, and optional admission-backlog HPA metrics were added behind a production profile requiring the custom-metrics API.
- Native blockers were repaired across OWL algebra traversal, RDF/Arrow handling, Kubernetes resource types, distributed artifact planning, reasoner lifetimes, and query planning.
- Artifact-plan validation was restored at the artifact-materialization stage, where the plan actually exists, instead of the earlier projection stage.
- `Cargo.lock` now pins the Rust dependency graph.

## Qualification evidence

- Rust toolchain: 1.97.1.
- Workspace check excluding `ngkg-online-serving`: passed with all targets and all features.
- Targeted tests: 32 passed, 0 failed across SPARQL, OWL Direct, exact reasoning, HPC runtime, query planning, distributed build/artifacts, and hydration.
- Cumulative static gates: all 44 gates from Phase 15 through Phase 40.13 passed.
- API/OpenAPI parity: 12 control-plane and 15 online-data-plane operations passed.
- Helm values validation: passed.

## Release blockers and next build order

1. Repair the `ngkg-online-serving` Axum handler future boundary so Rust 1.97.1 proves every handler generally `Send`; then require full workspace check and test success.
2. Build a normative SPARQL 1.1 feature matrix and executable W3C manifest runner. Implement every missing parser, algebra, dataset, expression, aggregate, solution-modifier, property-path, query-form, protocol, result-format, and error-semantics case before enabling the standards flag.
3. Implement policy-controlled federated `SERVICE` execution with SSRF controls, authorization propagation, time/byte/row budgets, cancellation, and deterministic failure semantics.
4. Wire the pinned exact HermiT adapter into online OWL Direct query dispatch with bounded distributed partitions and proof/certificate verification. Keep the native Rust/C++ reasoner rewrite as a differential-validated replacement track, not a shortcut around HermiT correctness.
5. Extend distributed planning/execution beyond certified constant-graph inner joins: OPTIONAL, UNION, MINUS, aggregates, subqueries, property paths, ordering/top-k, spill, skew, cancellation, and snapshot-bound retries.
6. Qualify custom-metric delivery, HPA behavior, Kueue scheduling, multinode storage, failure recovery, security, observability, and Apache Jena benchmark gates on a real Kubernetes cluster.

No standards or production flag may be enabled until its executable conformance and operational gates pass.
