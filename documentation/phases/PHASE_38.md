# Phase 38 — Typed SPARQL compiler and protocol

## Objective

Phase 38 removes query meaning from text scanners. One shared standards parser produces an immutable typed SPARQL 1.1 algebra that is consumed by offline certification and online serving. Dataset construction, graph routing, deterministic-function policy, safe distributed decomposition, and semantic certification are derived from that typed representation.

Phase 38 does not broaden the public language claim beyond the exact certified `SELECT` subset. `ASK`, `CONSTRUCT`, and `DESCRIBE` remain explicitly rejected until Phase 39 supplies scalar reference semantics and result-equivalence certification for those forms.

## Shared typed compiler

`ngkg-sparql-compiler` pins `spargebra` and enables standard Unicode escaping. It parses each query once and records:

- query form;
- query-level `FROM` and `FROM NAMED` dataset specification;
- constant graph IRIs addressed by `GRAPH`;
- variable-graph use;
- active-default graph-pattern use;
- constant predicate/class/property-path IRIs used for conservative capability routing;
- property-path presence;
- canonical SPARQL S-expression;
- SHA-256 of that canonical algebra representation.

`SERVICE` and nondeterministic certified-mode functions such as `RAND`, `NOW`, `UUID`, `STRUUID`, and `BNODE` are rejected from typed nodes rather than discovered by token scanning.

Every certified query binds both the exact input byte SHA-256 and a versioned canonical algebra SHA-256. The runtime reparses the request with the same shared compiler and rejects a mismatch before execution.

Online parsing is executed in the query pod's bounded Rust compute/blocking pool after admission, not on the Tokio control thread. This keeps a large but admitted query from monopolizing request scheduling and makes parser CPU scale horizontally with the query HPA. The PostgreSQL pool is also controlled by `NGKG_DATABASE_MAX_CONNECTIONS` / `onlineServing.databaseMaxConnections` rather than a source constant, allowing total database pressure to be sized against HPA maximum replicas.

## Dataset and routing semantics

Protocol dataset parameters retain precedence over query `FROM`/`FROM NAMED`, which retain precedence over the authorized service union-default dataset. The active-dataset hash identifies the semantic default/named graph sets; the selection source is bound separately in execution and cache evidence.

The certified subset does not treat protocol dataset parameters as permission to execute an arbitrary graph set. After precedence and graph authorization are applied, the resulting active-dataset hash must equal the offline certificate for the exact query. A different authorized graph set is rejected as uncertified rather than silently evaluated with stale proof or closure assumptions.

Capability pruning is constrained to the typed active dataset. Constant `GRAPH` patterns are graph-local. `GRAPH ?g` conservatively retains all active named graphs. Property paths conservatively retain the full active default graph set because predicate capability pruning is not yet sufficient to prove exact path reachability. Any optimized route is re-executed against the exact reference evaluator during compilation; failure to match the independent expected multiset falls back to the full active dataset or blocks certification.

## Safe distributed decomposition

Distributed fragments are generated only when the typed `SELECT` algebra, after projection removal, is a pure inner-join tree whose leaves are constant `GRAPH <iri>` patterns. The fragment query text is generated from the typed algebra rather than sliced from the original source string.

Each fragment is executed and hashed independently. The existing exact Rust join pipeline then combines the fragment multisets, projects the final variables, and the complete distributed result must match the scalar certified result hash before a distributed plan is published. Other algebra remains on the exact local path.

The existing enterprise HPC mechanisms remain unchanged: Arrow IPC exchange, bounded Tokio pools, checksum-verified NVMe spools, Grace hash joins, per-tenant admission, Parquet hydration, HPA/KEDA-compatible metrics, Kueue for offline work, and RKE2 cluster-autoscaler node growth.

## SPARQL Protocol results

The authenticated SPARQL endpoint supports the existing GET, raw `application/sparql-query` POST, and form-encoded POST request forms. `SELECT` output is serialized by the standards results serializer and negotiated across:

- `application/sparql-results+json`;
- `application/sparql-results+xml`;
- `text/tab-separated-values`;
- `text/csv`.

Serialization is bounded by the configured response byte ceiling. Malformed RDF terms, undeclared variables, unacceptable media ranges, and serializer failures are errors; a partial result is never labeled complete. Transport deadlines and upstream HTTP 408/504 responses are classified as `504 Gateway Timeout`, while non-timeout dependency failures remain `503`, so enterprise clients can distinguish retryable deadline exhaustion from dependency outages.

## Swagger and offline operation

Swagger UI assets are embedded from an exact vendored dependency and served locally under `/docs`. The UI loads `/openapi.json` from the same service and does not require a public CDN at runtime. The response includes a self-only script policy and `X-Content-Type-Options: nosniff`.

## Acceptance criteria

Phase 38 is qualified only when all inherited Phase 36/37 gates pass and executable tests prove:

- the shared parser accepts applicable SPARQL 1.1 grammar cases and rejects malformed queries;
- offline and online algebra hashes are identical for the same query;
- semantic-equivalent surface whitespace produces the same algebra hash while raw request certification remains exact-byte scoped;
- no lexical routing scanner remains in the semantic compiler;
- protocol-over-query dataset precedence is enforced before certificate comparison;
- typed optimized routes equal the exact reference multiset;
- distributed constant-graph plans equal the exact scalar result;
- JSON, XML, TSV, and CSV solution serialization passes applicable W3C result-format tests;
- Swagger and OpenAPI are served locally in a network-restricted deployment;
- all prior authorization, cache, failure, HPA, and RKE2 scaling invariants remain green.

## Current boundary

This source tree is an implementation candidate, not a production qualification. The environment used to build the archive does not contain Cargo/rustc, Maven, Helm, kubectl, or a live RKE2 API server, so those gates are not claimed. Standards feature flags remain disabled. Phase 39 is next: complete scalar SPARQL algebra and the four query forms before extending distributed physical operators.
