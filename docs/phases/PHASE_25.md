# Phase 25 — bounded local-NVMe shuffle spill

Phase 25 builds cumulatively on Phase 24 and removes a major coordinator memory multiplier from certified distributed joins. Before partition requests are dispatched, each join side is consumed into checksum-bound partition files on dedicated node-local ephemeral storage. Only the configured number of partitions is replayed concurrently into Arrow requests; the complete set of partition inputs no longer remains duplicated in coordinator RAM.

## Intent

Phase 24 already parallelizes sparse RDF joins. Hash partitions execute concurrently through Rust blocking lanes inside fragment pods, Kubernetes spreads those pods across dedicated nodes, and the coordinator bag-unions their results. Setting OpenMP, OpenBLAS and MKL to one thread does **not** make the RDF engine single-threaded. It prevents unrelated native libraries from creating nested thread pools on top of Rust's explicitly bounded partition concurrency.

Phase 25 changes the coordinator path to:

```text
offline-certified fragment bags
  -> consume rows into stable hash-owned spill partitions
  -> flush and fsync every partition file
  -> record exact bytes, rows and SHA-256
  -> replay only N concurrent partition pairs
  -> Arrow IPC request to fragment/shuffle workers
  -> parallel exact bag joins across cores and nodes
  -> validate worker, partition and multiset evidence
  -> bag-union all complete partitions
  -> remove the spill stage
  -> compare with the original offline-certified final multiset
```

There are no placeholder writers, mock storage adapters, skeleton endpoints or smoke-test functions in this path. The query service executes the spill implementation for every Phase 24 shuffle-eligible query.

## Spill format and integrity

Every side/partition file is created with `create_new` below a dedicated operator-owned spill root. Its binary header contains:

- format magic;
- dataset and snapshot UUIDs;
- exact query SHA-256;
- join stage;
- relation side;
- partition number and total partition count.

Rows are length-prefixed canonical SPARQL JSON bindings. The writer tracks exact row count, byte count and SHA-256 while writing, flushes the buffered stream and calls `sync_all` before the partition becomes readable. Replay validates file length, header identity, every record boundary, JSON syntax, stable hash ownership, exact row count, end-of-file and SHA-256. A truncated, extended, replaced, cross-stage or foreign-partition file fails closed.

The spill root has a fixed ownership marker. A new process refuses symlinks and unmanaged entries. On restart it removes only UUID-named `stage-*` directories below a valid marked root. A successful stage must remove its directory before query execution can continue; an error path uses RAII cleanup, and Kubernetes ultimately removes the disposable `emptyDir` with the pod.

## Bounded memory and storage

`maxShuffleSpillBytes` limits the complete left-plus-right spill footprint of one stage, including headers and record framing. `shuffleSpillSizeLimit` limits the Kubernetes volume. `maxShuffleOpenFiles` must permit exactly two writers per logical partition and prevents deployment configuration from exceeding the process descriptor budget.

The query pod requests ephemeral storage sufficient for both its existing immutable cache and shuffle spill volume. RKE2 query nodes must place kubelet/containerd ephemeral storage on the reviewed local NVMe device. `emptyDir` is disposable acceleration storage, never semantic truth; offline certificates, graph artifacts and Parquet payload remain in durable object storage and PostgreSQL catalog state.

Only `shuffleExchangeConcurrency` partition pairs are decoded and Arrow-encoded at once. Fragment and final stage results remain subject to the existing response, total exchange and intermediate-row ceilings. Phase 25 does not claim that the coordinator is fully out-of-core: the certified fragment responses and each assembled stage result are still bounded in memory. It removes the all-partition input duplication that preceded exchange.

## HPC execution model

Sparse RDF equality joins are not matrix multiplication. BLAS would add conversion overhead and cannot express RDF term compatibility or SPARQL bag multiplicity efficiently. Direct OpenMP parallelism would be appropriate only if NGKG moved a measured join kernel into native C/C++; using it alongside the current Rust pool would oversubscribe the Kubernetes cpuset.

NGKG therefore uses:

- stable hash partitions as independent multi-node work units;
- an expected-O(1) Rust hash index for fully bound shared RDF terms inside each partition;
- bounded asynchronous dispatch across ready fragment worker pods;
- bounded Rust `spawn_blocking` lanes across whole CPU cores inside each pod;
- final bag aggregation and offline-certificate validation at the coordinator;
- buffered sequential local-NVMe I/O for spill;
- read-only anonymous mmap for the existing fixed-width GUID locator's random lookup path;
- Arrow/Parquet columnar execution for semantic exchange and payload hydration.

File mmap is intentionally not used for sequential spill replay. The safe Rust workspace forbids direct unsafe OS mappings, the spill file is read once, and buffered sequential I/O lets the kernel page cache and readahead perform the appropriate optimization. Copying a file into an anonymous mmap would consume the RAM this phase is designed to protect.

## Kubernetes and autoscaling

No new node responsibility is introduced. Spill belongs to the query coordinator, so the query StatefulSet mounts `/var/lib/ngkg/shuffle` and reserves `ephemeral-storage`. Partition join CPU remains on `sparql-fragment-processing` nodes. Query and fragment CPU/memory HPA targets remain capped at 80 percent, while local storage exhaustion is a hard request failure rather than a reason to return a partial graph.

For RKE2, the query machine pool must expose enough allocatable CPU, memory and ephemeral storage after system reservations. Required anti-affinity keeps one query replica per node. When CPU or memory reaches the configured 80 percent threshold, HPA creates another replica and Rancher Cluster Autoscaler grows only the `sparql-query-processing` pool. Storage sizing is validated at deployment; a later custom-metrics phase may scale earlier from spill queue depth or bytes in flight.

## Acceptance criteria

Phase 25 is accepted only when all of the following pass:

1. Pinned Rust 1.97.1 formatting, compilation, Clippy with warnings denied and all workspace tests pass with a committed lockfile.
2. Spill round-trip tests preserve RDF term identity, unbound values, duplicate bag rows and partition ownership across multiple file sizes.
3. Header, length, JSON, partition, checksum, truncation, extension, symlink, unmanaged-root, byte-limit, row-limit and open-file-limit corruption cases fail closed.
4. The Phase 24 partition-union result, Phase 25 spill-replay result and independent expected SPARQL result have the same canonical multiset.
5. A deployed eligible query reports `shuffleSpillMode=bounded_local_nvme_v1`, positive spill bytes, at least two shuffle workers, exact expected bindings and exact GUID-hydrated Parquet evidence.
6. Query-pod ephemeral-storage requests equal limits and cover `cacheSizeLimit + shuffleSpillSizeLimit`; the spill directory returns to its marker-only baseline after success, failure, timeout and cancellation.
7. Enterprise load tests keep coordinator RSS within its configured pod limit while varying partitions, concurrency, row width, bag duplicates and skew.
8. Helm schema, values validation, lint, render, server-side dry-run, image digest, probes, PDB, NetworkPolicy and mTLS prerequisites pass.
9. On RKE2, local NVMe placement is proven, 79 percent CPU/memory causes no resource-driven growth, 80 percent does, and only the correct query or fragment pool grows for its measured bottleneck.

Run deployed qualification:

```bash
NGKG_ONLINE_QUERY_URL=https://ngkg.example \
NGKG_API_TOKEN="${NGKG_API_TOKEN}" \
NGKG_DATASET_ID=4d2e1a82-c2bc-536a-a809-fda7643ef1f7 \
NGKG_CERTIFIED_QUERY_FILE=test-corpus/queries/q01-cross-domain.rq \
NGKG_EXPECTED_RESULTS_FILE=test-corpus/expected/q01-cross-domain.srj \
NGKG_EXPECTED_ROUTING_FILE=test-corpus/routing/q01-cross-domain.json \
NGKG_KUBERNETES_NAMESPACE=ngkg \
scripts/qualify_phase25.sh
```

## Intentional boundary

Phase 25 implements bounded coordinator-side local spill and concurrent replay for existing certified hash-shuffle stages. It does not yet implement direct worker-to-worker shuffle, out-of-core worker hash tables, spillable final aggregation, persistent partition-result caching, cache invalidation across snapshots, Arrow Flight, adaptive skew repartitioning, distributed property paths, cost-based join reordering, arbitrary SPARQL decomposition, arbitrary OWL 2 DL query completeness, proof-DAG export, continuous updates or a universal 20–50x speedup. A safe future partition cache must key immutable snapshot, plan, stage, partition and input checksums and enforce its own size and provenance bounds. Caching is not required to parallelize the join and is not silently enabled: stale cached bindings would be a correctness defect, while an ephemeral spill file is only a bounded transport buffer for one executing query.
