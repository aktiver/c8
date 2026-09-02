# Phase 18 — Memory-mapped locator and sharded Parquet hydration kernel

Phase 18 turns the Phase 17 global locator and partitioned payload artifacts into a real serving kernel. It does not change semantic qualification: OWL-aware query execution still decides which GUIDs qualify. The new code only resolves those qualified GUIDs to exact physical rows and reconstructs their payload without listing objects or scanning unrelated Parquet files.

## Implemented data path

1. `compile-mmap-locator` verifies the immutable Phase 17 locator checksum.
2. The compiler converts sorted TSV rows into a fixed-width, snapshot-bound binary file.
3. A locator replica opens the immutable file read-only, maps it into memory, verifies the binary and source-locator checksums, validates strict sort order, and serves GUID lookups by binary search.
4. Each result GUID resolves to one or more `(partition, row_group, row_in_group, graph_id, predicate_id)` records.
5. Hydration groups records by partition and row group, opens only referenced Parquet shards, and processes independent groups across a bounded Rust thread pool.
6. Every returned row must match the locator GUID, predicate, graph, and snapshot. A missing key, shard, row, version, or checksum fails closed.
7. Query ordinal and multiplicity survive hydration so representation work cannot alter SPARQL bag semantics.

The binary locator has a 64-byte header followed by 44-byte big-endian records. The header binds the format magic, snapshot UUID, exact Phase 17 locator SHA-256, and record count. The output file is immutable; a failed compile removes its incomplete output so a retry cannot accidentally accept a partial index.

The current memory map is file-backed and read-only. `memmap2` requires an unsafe call because another process could mutate a mapped file, so NGKG isolates that call in one `#[allow(unsafe_code)]` function under the workspace's `unsafe_code = "deny"` policy. The descriptor is read-only, the admitted object path is immutable, and the complete mapped bytes are checksum-verified before any record is exposed.

## HPC and Kubernetes behavior

Hydration parallelism exists at two levels:

- across nodes, the HPA creates one hydration pod per `parquet-hydration` node and the RKE2 Cluster Autoscaler expands only that responsibility pool;
- inside a pod, exact `(partition, row group)` work is striped across `NGKG_HYDRATION_WORKER_THREADS` bounded lanes.

Row groups are independent immutable units. The page cache and node-local `emptyDir` cache retain hot Parquet and locator pages. No lane performs object-store listing, and no request creates one task per row. This limits scheduler overhead while preserving enough independent groups to use assigned cores.

Sparse GUID lookup and Parquet decoding do not call BLAS. OpenMP, OpenBLAS, and MKL remain at one thread for these kernels, preventing nested oversubscription. Dense vector reranking and future matrix-based alignment may use a separately measured BLAS/OpenMP profile, but its threads must be subtracted from the Rust budget and fit the pod cpuset.

`nodeSaturationTargetPercent` is 80. CPU and memory HPA resource metrics are both capped at that value; queue delay and bytes in flight may scale earlier. Required pod anti-affinity keeps one query or hydration replica of the same responsibility on a node. Whole-CPU requests equal limits, so each replica receives an exclusive cpuset. RKE2 system and kube reservations remain outside the workload envelope, and the remaining 20% supplies page-cache, networking, compaction, failover, and burst headroom. The validator rejects targets above 80 or non-Guaranteed-QoS resource pairs.

## Correctness boundary

The locator is not an entailment index. It must receive only GUIDs produced by the snapshot-matched semantic query engine. Direct hydration cannot make an entity qualify, repair an incomplete semantic slice, or substitute for OWL 2 DL reasoning.

Phase 18 also does not yet publish the distributed artifact root as the certified online serving root. That cutover requires a durable serving-root manifest, catalog compare-and-swap, object-store staging, reference-versus-sharded hydration equality across the certified query corpus, and operator recovery tests. Until that gate passes, the Phase 13/17 reference compiler remains the semantic and publication oracle.

## Acceptance criteria

1. Binary output is deterministic for identical locator bytes, snapshot, and format version.
2. Input, binary, source-locator, and payload checksums are verified before serving.
3. Duplicate or unsorted locator records, invalid encodings, and partial files are rejected.
4. GUID lookup is logarithmic in locator record count and never scans Parquet.
5. Hydration opens only the partitions and row groups named by locator results.
6. Missing qualified GUIDs, partitions, rows, or snapshot mismatches fail closed.
7. Query ordinals and multiplicities are preserved exactly.
8. One-thread and N-thread hydration return byte-equivalent canonical rows.
9. One-node and N-node deployments return the same hydrated rows.
10. CPU and memory scale targets are at most 80%, one hydration replica lands per responsibility node, and only the RKE2 `parquet-hydration` pool grows.
11. Rust, OpenMP, BLAS, I/O, and control pools fit the assigned cpuset without nested parallelism.
12. Cargo formatting, linting, tests, Helm rendering, RKE2 autoscaling, node-loss, and Parquet corruption tests pass in the pinned qualification environment.
