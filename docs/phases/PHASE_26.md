# Phase 26 — snapshot-safe distributed shuffle-result cache

Phase 26 builds cumulatively on Phase 25 and accelerates recurring certified cross-domain queries without weakening OWL reasoning or SPARQL bag semantics. Fragment workers cache exact logical results for individual hash-owned join partitions on bounded local NVMe. A cache entry is reusable only when the tenant, dataset, published snapshot, exact query, distributed plan, join stage, logical partition, partition count and canonical multisets of both input relations are identical.

## Intent

Phase 25 bounded coordinator memory by spilling partition inputs before exchange. Enterprise hot paths still repeated the same worker-side hash join for identical immutable snapshots. Phase 26 adds a correctness-bound reuse path:

```text
certified Arrow shuffle request
  -> validate snapshot, plan, stage, heads, keys and partition
  -> compute canonical left and right input-bag SHA-256 values
  -> derive tenant/snapshot/plan/partition cache key
  -> acquire one process-local single-flight lease
  -> verify cache header, key digest, file length and payload SHA-256
  -> reparse and revalidate output head, row ceiling and partition ownership
  -> recompute cached output multiset SHA-256
  -> return hit, or execute the exact Rust hash join on miss
  -> atomically publish a bounded cache file
  -> stream Arrow IPC with explicit hit/miss evidence
  -> coordinator revalidates every row and final offline-certified multiset
```

The application endpoint always executes this path for eligible distributed shuffle requests. There is no placeholder cache adapter, mock backend, skeleton handler or smoke-test function acting as production logic.

## Immutable cache identity

The cache key is:

```text
SHA256(
  format || tenant_id || dataset_id || snapshot_id ||
  query_sha256 || plan_sha256 || stage || partition || partition_count ||
  left_input_multiset_sha256 || right_input_multiset_sha256
)
```

Input checksums are canonical SPARQL multiset checksums, so bag duplicates affect identity and row order does not. A different ABox/TBox/RBox snapshot, reasoner closure, distributed plan, input binding, tenant or partition cannot address the same file. The worker still revalidates logical content after the file-level checksum; possession of correctly checksummed bytes is not treated as a semantic proof.

## Disk format, publication and recovery

`ngkg-shuffle-cache` owns a marked directory and manages only lowercase SHA-256 filenames. Each entry contains:

- `NGKGSC26` format magic;
- the 32-byte immutable cache-key digest;
- an unsigned payload length;
- the 32-byte payload SHA-256;
- a versioned logical shuffle-result payload.

Writers use a UUID temporary file, `create_new`, full writes and `sync_all`, then atomically hard-link the completed inode to its no-clobber final name, remove the temporary name and sync the parent directory. This is valid for the required pod-local Linux `emptyDir` filesystem and prevents a concurrent file from being overwritten. Startup removes only structurally valid abandoned temporary names, rejects symlinks and unmanaged entries, validates every managed header without reading the entire cache, and enforces byte and entry ceilings. First use reads and hashes the complete payload. Corruption is removed and becomes a miss; unverified bytes never become bindings.

The cache is disposable acceleration state. PostgreSQL, immutable object artifacts, offline reasoner output, semantic indexes and Parquet payload remain authoritative. A pod replacement may start cold without changing an answer.

## Concurrency and bounded resources

Identical in-flight keys share one Tokio single-flight lease. Different partitions remain independent and execute across the fragment pod's bounded Rust blocking pool. The lease registry removes idle keys, including cancellation paths, so adversarial or abandoned requests cannot create an unbounded lock map.

Four independent limits apply:

- `maxShuffleCacheBytes`: total indexed cache bytes per fragment pod;
- `maxShuffleCacheEntries`: maximum resident files;
- `maxShuffleCacheEntryBytes`: maximum header plus logical result payload;
- `shuffleCacheSizeLimit`: Kubernetes `emptyDir` ceiling.

LRU eviction occurs before atomic publication. Serialization uses a bounded writer and skips caching an oversized result while still returning the independently computed and validated answer. Runtime cache read/write failure cannot authorize cached data; the worker recomputes where safe and reports operational warnings.

## HPC, mmap, Parquet, OpenMP and BLAS

The join remains parallel across stable hash partitions, Rust cores, fragment pods and RKE2 nodes. Each fully bound local partition uses a Rust hash index; the coordinator bag-unions complete partitions and compares the result with the offline certificate. Cache hits eliminate repeated hash-table construction and RDF-term merging for hot immutable partitions.

Local NVMe is appropriate because cache access is whole-entry sequential I/O with strong locality. File mmap is not used for cache payloads: entries are bounded, verified sequentially and then parsed once, while mapping them would not eliminate the logical JSON/Arrow decoding requirement. The cumulative fixed-width GUID locator retains read-only anonymous mmap, and GUID hydration retains direct Parquet row-group access.

`OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS` and `MKL_NUM_THREADS` remain one for sparse RDF joins. This does not serialize the database: Rust owns partition concurrency and Kubernetes owns pod/node distribution. BLAS is reserved for a future measured dense scoring kernel; enabling dense native thread pools for equality joins would oversubscribe the guaranteed cpuset.

## Kubernetes and RKE2

The cache belongs to `sparql-fragment-processing`, so no new responsibility pool is created. Every fragment pod mounts `/var/lib/ngkg/shuffle-cache` as a separately bounded `emptyDir`. Its CPU, memory and ephemeral-storage requests equal limits. The fragment request must cover the immutable artifact cache plus the shuffle-result cache, with remaining capacity for logs and the writable layer.

RKE2 must place kubelet/containerd ephemeral storage for fragment nodes on reviewed local NVMe. The chart cannot manufacture NVMe or verify physical media. Required hostname anti-affinity preserves one fragment pod per node. HPA CPU and memory targets remain at 80 percent or lower; a new pending replica produces responsibility-specific Cluster Autoscaler demand. Cache-hit ratio should be exported through the platform metrics pipeline in a later observability phase; the Phase 26 response already carries exact per-query hit evidence.

## Acceptance criteria

Phase 26 is accepted only when:

1. Rust 1.97.1 format, build, Clippy with warnings denied and all workspace tests pass with a committed lockfile.
2. Cache keys change for every tenant, dataset, snapshot, query, plan, stage, partition, partition-count or input-multiset change.
3. Round-trip, reopen, LRU, byte-bound, entry-bound, oversized-entry, corruption, truncation, extension, wrong-key, symlink, unmanaged-root and abandoned-temp tests pass.
4. Cached logical output is rejected unless its head, row count, partition ownership and canonical multiset are valid.
5. First execution reports misses; an identical second execution reports positive hits and exactly identical bindings, GUIDs and Parquet hydration.
6. Snapshot or plan change produces zero reuse from the previous identity and still equals the independent expected SPARQL result.
7. Concurrent identical requests compute each missing partition once per worker; cancellation, timeout and worker loss leave the lease registry and cache within bounds.
8. Fragment pod ephemeral-storage requests equal limits and cover `cacheSizeLimit + shuffleCacheSizeLimit`; local cache usage plateaus under more unique keys than both configured ceilings.
9. Helm schema, lint, rendering, server-side dry-run, digest-pinned images, NetworkPolicy, mTLS, probes, PDB and RKE2 79/80-percent autoscaling qualification pass.
10. Enterprise qualification measures cold, warm and mixed workloads, cache hit ratio, worker CPU/RSS, NVMe throughput, p50/p95/p99 latency, eviction rate and exact answer equality.

Run deployed qualification:

```bash
NGKG_ONLINE_QUERY_URL=https://ngkg.example \
NGKG_API_TOKEN="${NGKG_API_TOKEN}" \
NGKG_DATASET_ID=4d2e1a82-c2bc-536a-a809-fda7643ef1f7 \
NGKG_CERTIFIED_QUERY_FILE=test-corpus/queries/q01-cross-domain.rq \
NGKG_EXPECTED_RESULTS_FILE=test-corpus/expected/q01-cross-domain.srj \
NGKG_KUBERNETES_NAMESPACE=ngkg \
scripts/qualify_phase26.sh
```

## Intentional boundary

Phase 26 implements bounded pod-local reuse of exact certified shuffle-partition results. It does not implement a cluster-coherent shared cache, durable cache truth, direct worker-to-worker shuffle, out-of-core worker hash tables, spillable final aggregation, adaptive skew splitting, Arrow Flight, distributed property paths, arbitrary SPARQL decomposition, arbitrary OWL 2 DL query completeness, proof-DAG export, continuous updates or a universal 20–50x speedup. The final offline-certificate comparison remains mandatory even when every partition is a cache hit.
