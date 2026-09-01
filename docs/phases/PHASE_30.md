# Phase 30 — bounded worker-side Grace hash join

Phase 30 builds cumulatively on Phase 29 and removes a worker-memory failure mode from certified cross-domain OWL/SPARQL execution. Phase 25 already externalizes coordinator-side shuffle inputs, but a fragment worker still decoded an owned partition and built one hash table over its complete right relation. A large partition or a single highly repeated RDF join key could therefore fit every network and coordinator bound while exhausting a worker.

The Phase 30 worker retains the Phase 24 primary, cross-node shuffle owner. If the right input is below `inMemoryJoinBuildRows`, it uses the existing sparse Rust hash join. Otherwise it applies the domain-separated `ngkg-worker-grace-key-v1` hash, writes both relations to bounded local-NVMe buckets, verifies every bucket header, length, identity digest and SHA-256 trailer, and replays right build chunks against bounded left probe chunks. Even a single hot key cannot make the worker hash index exceed `maxWorkerJoinBuildRows`.

## Intent and data path

1. The query coordinator routes only a certified, unordered distributed plan.
2. The primary shuffle assigns every fully bound join key to exactly one cross-node partition.
3. The worker verifies tenant, dataset, snapshot, plan, stage, partition, input heads and input bags.
4. A small right relation takes the in-memory fast path.
5. An oversized relation is repartitioned locally with a different hash domain so primary hash bits cannot collapse the second level.
6. Spill records are length framed and checksum protected. Files are created exclusively under a marker-owned `emptyDir`; symlinks and unmanaged entries fail startup.
7. The worker loads at most one configured right chunk and one configured left chunk for a join invocation. It preserves SPARQL bag multiplicity and the existing hard output-row ceiling.
8. The worker validates that every output still belongs to the primary partition and stores the result with join evidence in the snapshot/checksum cache.
9. Response headers carry the physical mode, spill bytes, non-empty bucket count and largest build chunk. The coordinator rejects absent, contradictory or over-ceiling evidence.
10. The final unordered bag must still match the offline OWL/SPARQL certificate before a public response or Phase 29 cache entry is produced.

## Kubernetes and HPC contract

The fragment StatefulSet receives a dedicated `worker-join-spill` `emptyDir`, intended for node-local NVMe. Its total process allocation, per-request allocation, bucket/file count, row size and build/probe chunks are independently bounded. The Helm validator proves:

- process and per-request spill limits fit the volume;
- two files per bucket fit the configured open-file ceiling;
- build and probe chunks fit the distributed intermediate-row ceiling;
- the in-memory threshold does not exceed the bounded build chunk;
- one encoded row cannot exceed the shuffle request ceiling;
- fragment ephemeral storage covers immutable cache, shuffle cache and worker spill together;
- requests equal limits for Guaranteed QoS; and
- HPA CPU and memory targets remain at or below the requested 80-percent node boundary.

Primary partitions execute concurrently across Rust tasks, fragment pods and RKE2 nodes. The worker deliberately replays local buckets within its admitted blocking lane instead of creating nested native thread pools. OpenMP, OpenBLAS and MKL remain fixed to one thread because equality hashing and sparse RDF binding compatibility are not dense matrix kernels. BLAS remains available to cumulative dense-scoring phases, while mmap remains used by locator and response-cache artifacts and Parquet remains the payload/hydration store. Sequential buffered I/O is used for transient Grace spill because it gives bounded reads and avoids mapping attacker-sized temporary files.

## Correctness and failure behavior

The secondary hash changes physical placement only. For supported fully bound inner-join stages, the required equality is:

`Bag(GraceJoin(L,R)) = Bag(InMemoryJoin(L,R))`

The crate test independently computes both bags, including duplicate hot-key rows. Other tests prove the output ceiling fails closed, a small build uses the fast path, appended spill bytes are rejected, cleanup releases process accounting and the spill root contains only its ownership marker afterward.

The cache format advances to version 2. An older entry, unknown mode, invalid checksum, foreign primary partition, over-ceiling build count or contradictory mode-specific evidence is invalidated and recomputed. Cache hits return the original computation evidence; caches never authorize data or replace the final offline certificate.

## Acceptance criteria

Phase 30 is acceptable only when all of the following pass:

1. Pinned `cargo fmt`, Clippy and all workspace tests pass with `Cargo.lock` present.
2. The hot-key Grace test returns the same multiset as the independent in-memory operator and never loads more than the configured build chunk.
3. Corruption, row, output, request-spill and process-spill bounds fail closed and release resources safely.
4. A fresh enterprise qualification workload forces `grace_hash_nvme_v1`, returns the independent expected bag, reports positive spill and Grace partitions, and keeps `workerJoinMaxBuildRows` within Helm configuration.
5. Fragment metrics show Grace executions and spill bytes increased, while active spill bytes return to zero after the request.
6. Helm lint, server-side dry run and cross-field validation pass for the approved RKE2 profile.
7. The fragment pod has the dedicated bounded spill volume, fixed sparse-kernel native thread counts, Guaranteed QoS, required anti-affinity and the responsibility-specific node selector.
8. Query and fragment HPA resource targets do not exceed 80 percent, and a real RKE2 node-pool test proves pending anti-affine replicas trigger the intended Cluster Autoscaler group.
9. Sustained skew, cancellation, disk-full, pod-loss, cache-corruption, snapshot-replacement and concurrent-tenant qualification show bounded RSS/ephemeral storage and exact certified answers.

## Intentional boundary

Phase 30 bounds the worker's hash-index and transient bucket working set; it does not claim that Axum request extraction or Arrow decoding is fully streaming. The request body and decoded owned partition remain bounded by the existing shuffle byte and row ceilings. Final coordinator aggregation also remains bounded in memory. The local second-level replay does not distribute one primary hot key across several nodes; it makes that owner's computation memory-safe. A future phase may introduce streamed Arrow decode, out-of-core final aggregation or certified hot-key replication without weakening single-owner correctness.

This is transport-independent database execution code, not transport theory. It optimizes an exact sparse RDF equality join after network ownership has already been decided. It does not add new OWL 2 DL or SPARQL language coverage, and it does not claim a universal 20–50× speedup. It makes the existing certified fast path viable for larger and more skewed enterprise partitions.
