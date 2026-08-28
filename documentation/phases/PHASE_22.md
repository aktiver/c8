# Phase 22 — certified distributed named-graph fragments

Phase 22 builds directly on the Phase 21 relevant-graph route. It adds a real cross-node execution path for recurring, unordered, conjunctive SPARQL `GRAPH` patterns that can be decomposed and independently proven equivalent for an immutable reasoner-certified snapshot. Every other certified query keeps the complete Phase 21 local-route path; unsupported syntax never receives an optimistic distributed plan, so coverage gaps fail closed.

## Intent

The goal is to make cross-domain work concurrent without changing its meaning. The offline compiler separates explicit named-graph blocks, writes one minimal N-Quads dataset and one SPARQL query per block, executes every fragment against the snapshot's checksum-bound HermiT closure, and performs a bag-correct join. A distributed plan is published only when the projected bindings equal both the independently authored expected SPARQL result and the result from the complete certified Phase 21 route.

This is snapshot-specific proof, not a heuristic optimizer assertion. If decomposition, execution, row bounds, projection, provenance certification, or equality fails, the compiler emits no distributed certificate and online serving uses the already-certified local route.

## Immutable artifacts

For an eligible query hash, compilation adds:

```text
plans/distributed/<query-sha256>.json
data/distributed/<query-sha256>/fragment-0000.nq
data/distributed/<query-sha256>/fragment-0001.nq
queries/distributed/<query-sha256>/fragment-0000.rq
queries/distributed/<query-sha256>/fragment-0001.rq
```

The strict plan records dataset and snapshot UUIDs, the exact original query hash, final projection, join order, graph IRI, fragment head, certified row count, canonical fragment multiset hash, and the path, SHA-256 and byte count of every query and dataset artifact. The routing certificate binds the plan hash, plan size, fragment count, and canonical final distributed multiset. All artifacts enter the snapshot manifest before publication.

## Exact offline compilation

The decomposer accepts only a narrow fast path: an unordered certified query with at least two explicit, resolvable `GRAPH` blocks whose graphs are present in the Phase 21 selected set. It retains the query prologue, compiles a real `SELECT *` fragment for each block, and executes it through Oxigraph with the exact reasoner closure. Query-level forms that cannot be safely extracted simply produce no distributed plan.

SPARQL results are bags. `inner_join_sparql_json` therefore preserves repeated rows, compares complete RDF JSON terms on shared variables, supports SPARQL-compatible unbound shared variables, checks intermediate growth before every append, and projects without accidental deduplication. The compiler refuses the fast path if any intermediate exceeds the trusted compilation bound.

The final acceptance relation is:

```text
distributed fragment join
  = independently authored expected result
  = complete certified Phase 21 route result
```

Equality is exact over the canonical SPARQL multiset for this snapshot. A plan is never inferred from matching row counts alone.

## Online coordinator and workers

The query coordinator verifies the active catalog publication, snapshot manifest, capability index, route proof, distributed certificate, plan and every plan-listed artifact record. It resolves ready fragment pod IPs from the headless `ngkg-fragments` Service, requires at least two addresses, assigns fragments round-robin, and sends the snapshot UUID and manifest hash with the existing tenant-scoped bearer authorization.

Each fragment worker independently loads the active publication. It rejects an unknown query hash, unknown fragment ID, stale snapshot, stale manifest, unsafe artifact path, checksum mismatch, size mismatch, reasoner-report mismatch, plan mismatch, or result drift. It constructs a real Oxigraph store from the verified fragment plus closure, executes the immutable fragment query, and returns bindings only when the head, row count and canonical multiset equal the offline fragment certificate.

The coordinator accepts one response per planned fragment, validates every identity and certificate field, joins in the certified order, projects the final head, and compares the final canonical multiset with the offline distributed certificate. It additionally requires responses from at least two distinct worker identities. Only then can GUID qualification and the existing mmap locator/Parquet hydration path run.

## Resource and failure bounds

The online path has positive operator-controlled bounds for:

- distributed fragments per query;
- rows in each fragment and every join intermediate;
- bytes in one response and total bytes in the exchange;
- bytes in serialized query, locator and hydration responses;
- simultaneous fragment requests and request duration;
- query size, qualified GUIDs and hydrated rows inherited from Phase 20;
- resident fragment runtimes and local artifact cache size.

Response bodies are consumed as a stream and stopped before crossing the per-response limit. An atomic reservation stops concurrent responses from crossing the total exchange limit. Fragment-runtime construction is single-flight per snapshot/query/fragment, and LRU eviction drops the in-memory store and removes its verified local fragment files. Immutable object storage remains the recovery authority.

A timeout, missing worker, partial body, malformed JSON, duplicate fragment, unexpected fragment, worker identity collapse, join overflow, checksum mismatch, result drift, snapshot change or hydration failure returns an error. Partial rows are never converted into a successful empty or incomplete answer.

## Kubernetes and RKE2 execution

The Helm chart creates a dedicated `sparql-fragment-processing` responsibility group:

```yaml
hpcNodeGroups:
  sparql_fragment_processing_num_of_nodes: 3
autoscaling:
  sparqlFragmentProcessing: {owner: hpa, minNodes: 3, maxNodes: 60}
```

`ngkg-fragment-worker` is a parallel StatefulSet behind a headless Service. Required hostname anti-affinity and a matching label/taint allow one Guaranteed-QoS worker per dedicated node. CPU and memory requests equal limits and must match the pool's measured allocatable shape after RKE2 reservations. CPU and memory HPA targets are capped at 80 percent. At sustained 80 percent, HPA creates a new pod; if the responsibility pool is full, anti-affinity leaves it pending so Rancher Cluster Autoscaler grows only the `sparql-fragment-processing` machine pool.

Oxigraph fragment evaluation is sparse graph work, not a dense linear-algebra kernel. The pod uses multiple bounded Rust blocking lanes for independent requests but sets OpenMP, OpenBLAS and MKL to one thread. This prevents native-library and Rust nested oversubscription inside the exclusive cpuset. BLAS is not claimed as an acceleration for ordinary graph joins.

The data plane remains default-denied. NetworkPolicy permits query-to-fragment traffic on TCP 32040 plus the reviewed catalog and object-store dependencies. Phase 22 uses authenticated, bounded REST/JSON internally because that is the transport actually implemented. It does not label the path Arrow Flight. Production confidentiality still requires the already documented RKE2 service-mesh/CNI mTLS layer until application-native internal TLS is implemented and qualified.

## Acceptance criteria

Phase 22 is accepted only when all of the following pass:

1. Pinned Rust 1.97.1 format, compilation, Clippy with warnings denied, and all workspace tests pass.
2. A real HermiT compilation of the checked corpus emits two independently executable fragments, their queries, the strict plan and all manifest records.
3. Offline fragment execution and exact bag join equal the independent expected multiset and the complete Phase 21 route multiset.
4. A query that is ordered, unsupported, not exactly equivalent, or exceeds a compile bound emits no distributed certificate and still succeeds through its certified Phase 21 local route.
5. Mutating a plan, fragment, fragment query, closure, result hash, row count, graph IRI, join order, final projection, reasoner report, snapshot identity or artifact record fails closed.
6. A deployed query reports `certified_distributed_fragments`, at least two fragments and at least two worker identities, and returns exact expected bindings, deterministic GUIDs and Parquet payload.
7. Fragment timeout, worker loss, partial response, oversized response, excessive total exchange, row explosion and duplicate response return no partial answer.
8. More fragment runtimes than `maxResidentFragmentRuntimes` produces a stable process/`emptyDir` plateau; concurrent cold requests construct one runtime per fragment.
9. Helm schema, lint, render, server-side dry-run, digest pinning, probes, PDBs and default-deny connectivity pass.
10. Sustained 79 percent fragment load does not trigger resource-driven growth, sustained 80 percent does, and the pending pod grows only the Rancher `sparql-fragment-processing` pool.
11. Worker and node loss, cache corruption, stale publication and restart reconstruct only checksum-verified immutable state and never return a partial result.

Run deployed application qualification:

```bash
NGKG_ONLINE_QUERY_URL=https://ngkg.example \
NGKG_API_TOKEN="${NGKG_API_TOKEN}" \
NGKG_DATASET_ID=4d2e1a82-c2bc-536a-a809-fda7643ef1f7 \
NGKG_CERTIFIED_QUERY_FILE=test-corpus/queries/q01-cross-domain.rq \
NGKG_EXPECTED_RESULTS_FILE=test-corpus/expected/q01-cross-domain.srj \
NGKG_EXPECTED_ROUTING_FILE=test-corpus/routing/q01-cross-domain.json \
NGKG_KUBERNETES_NAMESPACE=ngkg \
scripts/qualify_phase22.sh
```

## Intentional boundary

Phase 22 implements real cross-node execution only for exact query hashes whose explicit named-graph decomposition is independently certified for one immutable snapshot. It does not claim arbitrary ad hoc SPARQL decomposition, ordered distributed queries, OPTIONAL/UNION/MINUS/subquery equivalence, property-path frontier distribution, partitioned shuffle, Arrow Flight, adaptive retries, proof-DAG export, continuous updates, arbitrary OWL 2 DL query completeness, or a universal 20–50× speedup. Full OWL 2 DL reasoning remains offline in the trusted HermiT adapter; online completeness derives from immutable reasoner output plus per-query expected-result and route/distributed equivalence certificates. Anything outside that coverage uses the complete certified local path or fails closed.
