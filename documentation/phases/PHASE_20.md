# Phase 20 — Certified online semantic qualification and direct hydration

Phase 20 is the first real online reader for the immutable physical representation admitted in Phase 19. One Rust binary runs as three independently scalable roles: `query`, `locator`, and `hydration`. All roles resolve the catalog's active `PUBLISHED` snapshot under tenant row-level security and refuse a request when its snapshot, serving root, locator, namespace, or certificate differs from catalog truth.

## Executed online path

```text
authenticated REST query
  → active published snapshot + exact serving certificate
  → selectively cache and verify snapshot-manifest.json, query-dataset.nq and closure.nt
  → require the exact SPARQL byte hash to have an offline reasoner certificate
  → execute the certified semantic query and derive deterministic GUIDs
  → send the snapshot-bound GUID envelope to a hydration replica
  → verify serving-root.json, locator.bin and dictionary.tsv
  → resolve GUIDs through the read-only mmap locator
  → fetch only named payload shards and exact Parquet row groups
  → return bindings, qualified entities, payload context, checksums and coverage scope
```

Object storage is never listed. A replica downloads only object keys committed by the catalog, reference manifest, or serving root and verifies every checksum before parsing. Local cache directories are snapshot-specific and immutable: a corrupt existing cache entry fails closed rather than becoming a cache miss.

## Semantic correctness boundary

The query service does not infer OWL consequences online. Offline HermiT classification/materialization and independent expected-result comparison create the certified query record. The online replica loads that exact materialized closure and accepts only a query whose raw UTF-8 bytes hash to a record in the active snapshot manifest. This makes the first online cutover reproduce Phase 19's admitted oracle instead of silently widening the semantic claim.

Oxigraph is used inside `ngkg-reference` as the cached SPARQL conformance evaluator for this certified replica. NGKG remains responsible for identity, catalog truth, immutable artifacts, mmap location, Parquet hydration, distribution, admission, and fail-closed orchestration. Phase 20 does not make NGKG an Oxigraph wrapper and does not claim arbitrary SPARQL under complete OWL 2 DL semantics.

Hydration cannot change semantic qualification. It re-derives every GUID from the public IRI and active identity namespace, requires the exact snapshot and serving-root hash, and reads only locator-addressed rows. SPARQL bindings preserve the query engine's bag semantics. The contextual payload is a distinct entity projection in this phase; it is not presented as one payload copy per duplicate solution binding.

## HPC and Kubernetes design

Query, locator, and hydration pods are pinned to responsibility-specific RKE2 pools. Required host anti-affinity makes new HPA replicas become pending when their pool has no free node; the separately installed Rancher Cluster Autoscaler then grows only the matching pool.

The runtime has three non-overlapping budgets:

- Tokio control threads run sockets, catalog calls, and object downloads.
- Rust blocking lanes execute synchronous SPARQL loading/execution and mmap/Parquet kernels.
- Hydration worker lanes partition exact Parquet row groups across cores.

The hydration role admits one CPU hydration kernel per replica at a time; its one blocking coordinator fans that request into the configured row-group lanes. Additional requests queue and create HPA pressure instead of multiplying `requests × workerThreads` into an oversubscribed node. Query replicas use several independent single-threaded blocking lanes because each certified SPARQL evaluation is its own CPU work unit.

OpenMP, OpenBLAS and MKL stay at one thread because these online kernels are sparse and branch-heavy. That prevents each request from multiplying native threads underneath Rust. BLAS is enabled only for a separately measured dense scoring kernel. The mmap locator is a checksum-verified read-only anonymous mapping backed by the operating system's virtual-memory/page-cache machinery; payload objects remain bounded cache files.

CPU and memory requests equal limits, supporting RKE2 static CPU Manager placement. Phase 20 HPAs use Metrics Server CPU and memory only, and both targets cannot exceed 80%. Queue-delay custom metrics remain disabled until the service exports and a pinned adapter verifies them; the chart does not reference fictional metrics. The remaining envelope is reserved for page cache, CNI, recovery, and system services. HPA changes replicas; Cluster Autoscaler changes VMs/nodes. Neither is confused with offline batch parallelism, which remains Kueue-controlled indexed work.

The external contract is REST/JSON and published as `/openapi.yaml`. Internal Phase 20 query-to-hydration exchange also uses authenticated REST to establish the correctness boundary. Arrow Flight exchange and graph-fragment shuffling remain a later performance phase; Kubernetes Services allow that transport to replace the wire format without changing snapshot or GUID invariants.

## Acceptance criteria

1. A role can serve only the catalog's active `PUBLISHED` snapshot and a matching Phase 19 serving certificate.
2. Tenant identity comes only from a bearer-token hash; all three endpoints require `queries:execute`.
3. Query bytes outside the configured bound or without an exact certificate return no semantic result.
4. Snapshot manifest, semantic dataset, closure, serving root, locator, dictionary, and payload shards are checksum- and size-verified before use.
5. Online code never lists object storage and never scans payload Parquet to discover a GUID.
6. The IRI-to-GUID derivation is repeated at the hydration trust boundary with the active identity namespace.
7. Mmap locator records identify every required partition and row group; missing qualified GUIDs fail closed.
8. Parquet hydration uses bounded worker lanes and a maximum result-row ceiling.
9. A publication change causes a cache-key change; stale snapshot/root requests return conflict rather than mixed-version results.
10. Query, locator, and hydration caches are disposable and reconstruct only from immutable catalog-addressed objects.
11. Every deployment has digest-pinned images, non-root read-only containers, health probes, default-deny networking, explicit dependency egress, and disruption budgets.
12. Query and hydration HPA CPU/memory targets are at most 80%; required anti-affinity and responsibility selectors drive the correct RKE2 node pool.
13. OpenMP/BLAS native thread counts remain one for sparse online roles; configured Rust thread totals fit the exclusive pod CPU request.
14. A real certified query returns the same SPARQL bindings and canonical hydrated payload as the Phase 19 reference/equivalence evidence.
15. Rust format, compile, Clippy and tests; PostgreSQL forced-RLS integration; S3 corruption tests; Helm render; and RKE2 HPA/Cluster Autoscaler fault tests all pass in the pinned release environment.

## Intentional boundary

Phase 20 is a horizontally replicated certified-query service, not yet a distributed cross-domain SPARQL planner. Each query replica currently caches the compact certified semantic dataset and closure and executes one exact certified query locally; hydration distributes independently across replicas and cores. Arbitrary query compilation, relevant-named-graph routing, cross-node semijoins, Arrow Flight exchange, adaptive context expansion, reasoner fallback, and 20–50× comparative performance qualification are subsequent gates.
