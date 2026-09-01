# Phase 24 — certified partitioned hash-shuffle joins

Phase 24 builds cumulatively on Phase 23 and distributes eligible multi-fragment join computation across the existing `sparql-fragment-processing` worker pool. It does not widen the certified query language. The Phase 21 route, Phase 22 fragment plan, Phase 23 lossless Arrow transport, and offline final multiset remain the semantic authority.

## Intent

Phase 23 executes graph-local fragments on separate nodes but gathers every binding at one coordinator for the final join. That is exact, but the coordinator becomes the CPU and memory bottleneck as intermediate bags grow. Phase 24 partitions each fully bound equijoin by a stable hash of its shared RDF terms and sends independent partitions to multiple worker nodes.

```text
offline-certified graph fragments
  -> parallel fragment execution and Phase 23 certificate checks
  -> canonical hash partition of both join inputs
  -> bounded Arrow shuffle requests across fragment workers
  -> exact bag join inside each owned partition
  -> partition, identity, schema and multiset validation
  -> ordered partition union
  -> original offline final-multiset validation
  -> GUID qualification and selective Parquet hydration
```

This is real application logic. There is no placeholder worker, mock join, or smoke-test path used as query execution.

## Exact partition contract

For a stage with a non-empty ordered shared-variable set `K`, NGKG encodes every bound RDF term by term family, lexical value, datatype and language. It hashes domain-separated, length-prefixed variable names and canonical term fields with SHA-256:

```text
partition(binding) = first_u64(SHA256(canonical(K, binding))) mod P
```

`P` is the snapshot-serving deployment's configured partition count and must be at least two. Both sides are accepted only when every key is bound. The Arrow request binds dataset UUID, snapshot UUID, query hash, distributed-plan hash, request UUID, stage, partition, partition count, left and right heads, and ordered key variables. Unknown metadata, malformed RDF terms, foreign-partition rows, or mismatched plans are rejected.

The worker performs the existing exact SPARQL JSON bag join. Duplicate input rows remain duplicate and therefore produce the correct multiplicity. Each response carries its worker identity and a canonical multiset checksum. The coordinator re-hashes every returned row, recomputes the checksum, rejects duplicate or missing partitions, enforces row and byte ceilings, and concatenates partitions only after all partitions pass.

## Why the distributed join is correct

Let `L_i` and `R_i` be rows whose complete shared key hashes to partition `i`. Equal join keys have byte-identical canonical encodings, so any compatible left and right rows are assigned to the same partition. No compatible pair can be split across partitions. Every input row has exactly one owner, so the partitions are disjoint and cover both complete input bags.

Therefore, including bag multiplicity:

```text
L join R = bag-union over i in [0, P) of (L_i join R_i)
```

Applying this argument inductively to each sequential join stage proves equality with the original Phase 22 join for the supported fully bound equijoin class. NGKG still compares the projected result with the immutable offline-certified final multiset. A hashing, transport, execution, or assembly error consequently cannot become a successful answer.

## Eligibility and exact fallback

Partition shuffle is selected only when every stage has at least one shared variable and all participating rows bind every selected key. A cross product, an unbound shared key, an invalid plan contract, or a query outside the Phase 22 distributed class is not approximated:

- valid Phase 22 plans that are unsafe to shuffle use the exact Phase 23 coordinator-local bag join;
- unsupported or uncertified queries use the complete Phase 21 certified route when available;
- missing certificates, stale snapshots, invalid artifacts, limits, or inconsistencies fail closed.

This fallback is part of application behavior, not a test substitute.

## Resource and failure bounds

The following Helm values are mandatory:

- `shufflePartitions` — stable logical partitions per join stage, minimum two;
- `maxShuffleRequestBytes` and `maxShuffleResponseBytes` — per-exchange ceilings;
- `maxShuffleExchangeBytes` — atomic request-plus-response ceiling across the complete query;
- `shuffleExchangeConcurrency` — maximum in-flight partition requests and never greater than the partition count.

The existing distributed intermediate-row ceiling applies to each input side, worker output, and assembled stage. Arrow encoding runs on bounded blocking lanes and its synchronous-to-async channel retains backpressure. Timeouts use the existing fragment HTTP client deadline. Any missing worker, partial Arrow stream, wrong media type, excessive body, duplicate partition, foreign row, checksum mismatch, or final-certificate mismatch returns no partial result.

## Kubernetes, RKE2 and HPC behavior

Shuffle joins reuse the Phase 22/23 fragment worker StatefulSet because fragment evaluation and sparse hash joining share the same data, cache, security, and CPU shape. Creating another dedicated node group would add a network hop and split capacity without an independent resource responsibility. Partition requests are round-robin distributed over ready pod IPs from the headless service, and successful execution must report at least two distinct shuffle worker identities.

Each fragment pod requests whole CPUs with equal limits, has required hostname anti-affinity, and selects the `sparql-fragment-processing` RKE2 pool. Rust owns bounded concurrent join lanes. Sparse hashing and joins do not use dense linear algebra, so `OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, and `MKL_NUM_THREADS` remain one to prevent nested oversubscription. Read-only mmap and direct Parquet hydration remain in the locator/hydration path from prior phases.

The fragment HPA monitors CPU and memory at no more than 80 percent. Sustained pressure creates another one-pod-per-node replica; required anti-affinity makes it pending when the pool is full; Rancher Cluster Autoscaler then grows only the labelled and tainted `sparql-fragment-processing` pool. Kubernetes, HPA, and Cluster Autoscaler distribute pods and capacity; the application-level hash partitions distribute semantic work.

## Acceptance criteria

Phase 24 is accepted only when all of the following pass:

1. Pinned Rust 1.97.1 formatting, compilation, Clippy with warnings denied, and all workspace tests pass with a committed lockfile.
2. Unit and corruption tests prove deterministic partition ownership for URI, blank node, typed literal and language literal keys, preserve duplicate bags, reject unbound keys, reject foreign partitions, and reject unknown or truncated Arrow metadata.
3. For multiple cardinality and skew distributions, the union of partition joins is byte-canonically equal to the Phase 23 local join and the independent expected SPARQL result.
4. A deployed eligible cross-domain query reports `certified_partitioned_shuffle`, at least two partitions, at least two fragment workers, at least two shuffle workers, and exactly matches expected bindings and hydrated Parquet evidence.
5. Request, response, total exchange, row, concurrency, timeout, partial-stream, worker-loss, duplicate-partition and missing-partition tests fail closed without partial results.
6. Cross products and unbound-key queries demonstrably retain the certified Phase 23 local join and return the same final multiset.
7. OpenAPI, Helm schema, cross-field values validation, lint, rendering, server dry-run, digest pinning, probes, disruption budgets, default-deny networking and mTLS prerequisites pass.
8. On RKE2, sustained 79 percent fragment pressure does not cause resource-driven growth, sustained 80 percent does, and only the `sparql-fragment-processing` Rancher pool grows.
9. Enterprise benchmarks record coordinator CPU/RSS, worker CPU/RSS, request and response bytes, skew, p50/p95 latency, and equality for Phase 23 local versus Phase 24 shuffle. No speedup claim is published without those results.

Run the deployed acceptance harness with the same variables documented for Phase 23:

```bash
NGKG_ONLINE_QUERY_URL=https://ngkg.example \
NGKG_API_TOKEN="${NGKG_API_TOKEN}" \
NGKG_DATASET_ID=4d2e1a82-c2bc-536a-a809-fda7643ef1f7 \
NGKG_CERTIFIED_QUERY_FILE=test-corpus/queries/q01-cross-domain.rq \
NGKG_EXPECTED_RESULTS_FILE=test-corpus/expected/q01-cross-domain.srj \
NGKG_EXPECTED_ROUTING_FILE=test-corpus/routing/q01-cross-domain.json \
NGKG_KUBERNETES_NAMESPACE=ngkg \
scripts/qualify_phase24.sh
```

## Intentional boundary

Phase 24 implements coordinator-dispatched, partitioned hash joins for the fully bound equijoin stages of existing Phase 22 plans. It does not yet implement direct peer-to-peer shuffle, shuffle spill to object storage or NVMe, Arrow Flight, distributed property-path frontiers, adaptive repartitioning for skew, cost-based join reordering, arbitrary SPARQL decomposition, arbitrary OWL 2 DL query completeness, proof-DAG export, continuous updates, or a universal 20–50x speedup. The coordinator still dispatches and gathers partitions. These later capabilities require their own implementation and equivalence gates.
