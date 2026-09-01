# Phase 21 — certified relevant-named-graph routing

Phase 21 changes the Phase 20 query replica from loading one complete asserted RDF dataset into loading one immutable, query-specific named-graph route. The offline compiler derives graph capabilities from the normalized facts, builds a conservative dependency graph, writes the reduced N-Quads route, executes the real SPARQL query against that route plus the exact reasoner closure, and accepts it only when the result and provenance evidence equal the independently authored expected answer. The online replica never invents a route.

## Production intent

An enterprise snapshot can contain many connected subdomains while a recurring query usually names or semantically touches only a few. Loading every asserted fact into every query replica wastes memory and increases parser, index-build, and cache-warm latency. Phase 21 moves that selection to immutable compilation, where it can be checked without risking a partial online result.

The compiler emits `indexes/graph-capabilities.json` with:

- every query-visible named graph in deterministic IRI order;
- exact query-visible fact counts;
- predicate-to-graph and class-to-graph capabilities;
- conservative cross-graph dependencies whenever one named entity occurs in multiple graphs.

For each certified query hash it emits `data/routes/<query-sha256>.nq` and a routing certificate bound to the capability-index checksum, selected graph IRIs, total graph count, routed artifact checksum and size, and canonical SPARQL result-multiset checksum.

## Offline routing algorithm

1. Parse and normalize the complete uploaded TriG/N-Quads input under the existing checksum, GUID, policy, and resource boundaries.
2. Build the capability and dependency index directly from encoded query-visible facts.
3. Extract absolute IRIs from the certified query while excluding comments and string literals, resolve `PREFIX` declarations, expand prefixed terms, and normalize `a` to `rdf:type`.
4. Preserve an explicitly declared SPARQL dataset when the query names source graphs.
5. Otherwise select graphs advertising the referenced predicates or classes and compute their dependency closure.
6. Use every graph when no safe capability can narrow the query.
7. Write only facts from the selected named graphs to the deterministic route artifact.
8. Load the routed assertions and the snapshot's exact offline HermiT closure into a fresh Oxigraph store.
9. Execute the actual query, compare its canonical multiset with the independent expected SPARQL JSON, and verify every required provenance source link.
10. If a selective route fails that comparison, repeat with all graphs. If the all-graph execution fails, compilation fails; no certificate or snapshot is published.

This is fail closed. Capability metadata proposes a route, but only the executed equivalence check certifies it.

## Online execution

The query replica resolves the active published snapshot and validates its Phase 19 serving certificate. It then downloads and verifies only the snapshot manifest, graph capability index, reasoner closure, and the route for the exact submitted query hash. Artifact object keys are derived from the signed manifest; the service does not list or discover objects.

The routed Oxigraph runtime is restricted to its one query hash. A checksum-valid artifact for one query cannot execute another query. Loaded runtimes are held in a bounded LRU configured by `onlineServing.maxResidentQueryRoutes`; eviction drops the in-memory replica and its local verified route file but never changes the immutable object-store snapshot. Cleanup takes the evicted query's single-flight lock so it cannot delete a file during concurrent reconstruction. Concurrent cold requests for the same route share that lock and perform one construction.

The response adds routing evidence:

```json
{
  "selectionMode": "declared_dataset",
  "selectedGraphIris": [
    "urn:ngkg:graph:hdfs",
    "urn:ngkg:graph:operations"
  ],
  "selectedGraphCount": 2,
  "totalGraphCount": 3,
  "capabilityIndexSha256": "64-lowercase-hex-characters",
  "routedDatasetSha256": "64-lowercase-hex-characters"
}
```

The final bindings are still hydrated by deterministic GUID through the Phase 20 mmap locator and exact Parquet row groups.

## Kubernetes and HPC behavior

Phase 21 remains inside the existing `sparql-query-processing` node group. Each query pod gets whole CPU and guaranteed memory, while relevant routes reduce resident asserted-graph memory and cold-load work. The chart exposes a hard resident-route count because Kubernetes memory limits and HPA cannot protect a process from an unbounded logical cache.

Sparse SPARQL, RDF parsing, mmap lookup, and row-group hydration do not benefit from dense BLAS kernels. `OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, and `MKL_NUM_THREADS` therefore remain one to prevent nested oversubscription. Rust control, blocking query, and hydration lanes retain explicit independent budgets. HPA CPU and memory targets remain at or below 80 percent; required role anti-affinity converts added replicas into unambiguous RKE2 node-pool demand while leaving 20 percent operational headroom.

Phase 21 does not exchange query fragments between nodes. A pod executes one certified routed dataset locally. Cross-node graph-fragment execution and bounded Arrow exchange require separate partition ownership, shuffle, cancellation, and distributed completeness work and remain a later phase.

## Acceptance criteria

Phase 21 is accepted only when all of the following pass:

1. Pinned Rust 1.97.1 formatting, compilation, Clippy with warnings denied, and all workspace tests.
2. A real HermiT compilation of the checked cross-domain corpus emits the capability index, selective route, route certificate, and snapshot artifact records.
3. The checked `q01-cross-domain.rq` route contains exactly the HDFS and operations graphs, excludes the provenance graph as asserted query data, and still returns the exact independent expected multiset and required source evidence.
4. Mutating the capability index, route bytes, selected graph list, route size, result hash, reasoner report, closure, or active snapshot causes offline compilation or online loading to fail.
5. The online API returns exactly the expected bindings, nonempty qualified GUIDs and hydrated payload, plus route evidence showing two selected graphs out of three.
6. More distinct queries than `maxResidentQueryRoutes` are executed and process memory stabilizes after LRU eviction; repeatedly accessing a hot route preserves it over an older cold route.
7. Concurrent cold requests for one query produce one artifact load and one runtime construction while all callers receive the same result.
8. Helm schema validation, lint, server-side dry-run, default-deny connectivity, digest pinning, probes, disruption budgets, and RKE2 node placement pass.
9. Sustained 79 percent load does not trigger growth, sustained 80 percent load does, the new replica creates demand only for the matching responsibility pool, and scale-down preserves the final available replica.
10. Node loss, partial object transfer, stale catalog state, cache corruption, and restart reconstruct the route from checksum-bound immutable artifacts without returning partial answers.

Run the application qualification against a deployed certified corpus:

```bash
NGKG_ONLINE_QUERY_URL=https://ngkg.example \
NGKG_API_TOKEN="${NGKG_API_TOKEN}" \
NGKG_DATASET_ID=4d2e1a82-c2bc-536a-a809-fda7643ef1f7 \
NGKG_CERTIFIED_QUERY_FILE=test-corpus/queries/q01-cross-domain.rq \
NGKG_EXPECTED_RESULTS_FILE=test-corpus/expected/q01-cross-domain.srj \
NGKG_EXPECTED_ROUTING_FILE=test-corpus/routing/q01-cross-domain.json \
NGKG_KUBERNETES_NAMESPACE=ngkg \
scripts/qualify_phase21.sh
```

## Intentional boundary

Phase 21 certifies relevant asserted named-graph routes for exact pre-certified query hashes and loads the snapshot's complete finite named-individual closure. It does not claim arbitrary ad hoc OWL 2 DL SPARQL completeness, proof-DAG export, closure partitioning, distributed SPARQL fragments, Arrow Flight shuffle, continuous updates, or a universal 20–50× speedup. Those claims require later implementation and measured release gates.

The route is nevertheless real application logic: real RDF is written, parsed, queried, compared, checksum-bound, stored, selectively loaded, cached under a hard bound, and used for the response. Static inspection is only a local evidence layer and is not production qualification.
