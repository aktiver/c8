# Phase 37 — Lossless RDF dataset

## Objective

Phase 37 preserves RDF dataset meaning through ingestion, compilation, columnar storage, authorization, routing, reasoning, caching, distributed execution, and hydration. The service-default dataset is modeled as the authorized set-union of query-visible named graphs while named graphs remain independently addressable.

## Implemented dataset model

`ngkg-dataset` owns the semantic dataset contract. The physical source default graph is graph ID zero and is not represented as a named graph IRI. Named graphs receive deterministic dense IDs in lexical IRI order. Every named graph is declared with an explicit role, authorization-label set, query-visibility flag, and reasoning-visibility flag. Declarations with zero observed quads remain in the immutable graph catalog, preserving explicitly declared empty graphs.

The active dataset resolver applies this precedence:

1. SPARQL Protocol `default-graph-uri` and `named-graph-uri` parameters when present.
2. Parsed query `FROM` and `FROM NAMED` clauses when no protocol dataset is supplied.
3. The authorized union-default service dataset when neither explicit dataset form is present.

Requested graphs must exist, be query-visible, and be authorized. A forbidden explicit graph is rejected rather than silently removed. The authorized graph set and active default/named dataset each have deterministic SHA-256 identities that are bound into runtime and cache contracts.

## Lossless RDF terms

- Default and named graph identity is retained independently from the internal graph dictionary key.
- Named resources and blank nodes retain distinct RDF term kinds even when both have deterministic internal GUIDs.
- Blank-node graph names are rejected by the NGKG input profile.
- Source-scoped blank-node identity is deterministic across canonical distributed N-Quads shards without serializing blank nodes as IRIs.
- Repeated statements in repeated TriG blocks for the same named graph collapse through deterministic fact identity, implementing RDF graph set semantics.
- Semantic and payload Parquet schemas carry RDF resource kind and graph scope explicitly.
- Distributed artifact Parquet carries the same fields.
- Hydration reconstructs resource kind and graph scope rather than inferring them from internal identifiers.

## Graph catalog, routing, and authorization

The compiler writes `indexes/rdf-dataset-catalog.json` and a format-version-2 graph capability index. Capability records bind graph IDs, IRIs, roles, authorization labels, reasoning visibility, query-visible fact counts, predicate/class indexes, and conservative cross-graph entity dependencies.

Online serving loads graph authorization before shared semantic state. Because the current finite reasoner closure is compiled from all reasoning-visible graphs, a principal must currently be authorized for every reasoning-visible graph before that closure can be exposed. This deliberately restrictive rule prevents inference leakage until graph-sensitive proof dependencies are implemented in Phase 41.

## Union-default and explicit datasets

The exact reference store retains named graphs and exposes their RDF set-union as the service default. The physical uploaded default graph is preserved in source/columnar artifacts but does not silently enter the declared union-default service dataset. The internal finite reasoner materialization may contribute to the active default graph only when the offline certificate says it did, and it is not exposed as a public named graph.

`GRAPH <iri>` remains graph-local and `GRAPH ?g` retains graph identity. Query dataset clauses are parsed through the standards parser rather than by the routing text scanner.

## Kubernetes scaling

The query, fragment, and hydration data-plane workloads have `autoscaling/v2` HPAs. The RKE2 profile requires the Rancher-compatible cluster-autoscaler path for matching node-pool growth. Resource HPA targets remain at 80 percent or lower to reserve failure, shuffle, and spill headroom. Offline compilation and reasoning retain the Kueue scheduling path.

Within each pod, thread and request ceilings remain explicit. Kubernetes scales pods/nodes; Tokio/Rust worker pools and later OpenMP kernels consume only the CPU set allocated to one pod.

## Acceptance criteria

Phase 37 is qualified only when executable tests demonstrate all of the following in addition to Phase 36 gates:

- Default graph round-trip remains default, never a synthetic named graph.
- IRI-named graph isolation for `GRAPH <g>`.
- Correct graph binding for `GRAPH ?g`.
- Authorized union-default behavior.
- Explicit `FROM` and `FROM NAMED` behavior.
- Multiple `FROM` graphs use RDF merge semantics and standardize blank nodes apart per input graph.
- Protocol dataset precedence.
- Blank-node term identity through distributed artifacts and hydration.
- Empty declared named graphs in the graph catalog.
- Zero unauthorized graph, closure, cache, or hydration leakage.
- Scalar and distributed certified result multisets remain identical for every admitted fast path.

## Current boundary

The lossless dataset implementation and static contract gates are present, but Phase 37 is not production-qualified in this environment because the Rust, Maven, Helm, and live RKE2 gates have not executed here. The compiler's relevance-routing optimizer still contains the Phase 35 lexical hint scanner; replacement by one immutable typed SPARQL algebra is Phase 38. The public query service remains a certified SELECT subset and does not claim complete SPARQL 1.1 or complete OWL Direct query answering.
