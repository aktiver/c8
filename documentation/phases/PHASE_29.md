# Phase 29 — certified complete-result cache for recurring OWL/SPARQL queries

Phase 29 builds cumulatively on Phase 28 and accelerates the production workload that NGKG is explicitly designed to win: recurring, selective, multi-hop queries over immutable published snapshots. Phase 26 reuses one partitioned shuffle result inside a fragment worker. It does not prevent a query replica from rebuilding the complete route, replaying all distributed joins, validating the final bag and hydrating the same Parquet rows on every identical request. Phase 29 adds a bounded local-NVMe cache for the complete public response after all those steps have succeeded.

This is application code in the real query path. It is not a benchmark shortcut and it does not cache an uncertified partial graph.

## Exact cache identity

One key is the SHA-256 of a domain-separated binary encoding of:

```text
response schema version
+ tenant UUID
+ dataset UUID
+ immutable snapshot UUID
+ snapshot-manifest SHA-256
+ serving-root SHA-256
+ exact certified query-byte SHA-256
+ hydration mode
```

The tenant prevents cross-customer reuse. The immutable snapshot, manifest and serving root prevent stale semantic or physical data reuse. The exact query hash preserves the offline certificate boundary. Hydrated and semantic-only responses are distinct entries. A response schema version prevents old bytes from being interpreted under a new public contract.

Authorization, current publication and certificate lookup still run before cache lookup. The cache never grants permission and never selects a snapshot.

## Production path

```text
authenticated tenant and dataset
  -> active immutable snapshot
  -> exact offline-certified query lookup
  -> exact cache key and per-key single flight
  -> checksum-verified local-NVMe entry
  -> deserialize and revalidate SPARQL multiset, route, GUIDs, bounds and payload
  -> serve the same bounded JSON bytes

cache miss or invalid entry
  -> execute the existing certified local/distributed query path
  -> validate exact final multiset against the offline certificate
  -> optionally hydrate through GUID locator and exact Parquet row groups
  -> serialize the bounded complete response once
  -> fsync and atomically publish the cache entry
  -> serve it with x-ngkg-query-cache: miss
```

Entries carry their cache-key digest, payload byte length and payload SHA-256 in a fixed 80-byte header. Reads verify the current file length, key digest and payload checksum. Verified bytes are copied into a bounded anonymous mmap, converted to a read-only mapping and retained as the Axum response owner. This avoids another response-buffer copy and ensures the returned bytes cannot change if the disposable disk file is evicted later.

The cache root is marker-owned and rejects symlinks, directories, non-UTF-8 names and unmanaged files. Writes use a unique file, `fsync`, an atomic same-filesystem hard-link publication and a directory `fsync`. Entry count, total bytes and individual bytes are independently bounded. Least-recently-used eviction removes only files managed by this cache. Corruption becomes a miss and removes the invalid entry; cache failure never changes query meaning or availability because the certified execution path recomputes the result.

## Logical revalidation on every hit

A checksum proves that bytes did not change. It does not prove that a future binary still interprets those bytes correctly. Before returning a hit, the query service therefore proves all of the following again:

- dataset, snapshot, serving-root and exact query identities match the current request;
- the response is marked complete and remains within row/entity/hydration limits;
- selected named graphs and capability/route hashes equal the offline routing certificate;
- canonical SPARQL result-bag SHA-256 equals the certified multiset;
- the unique URI bindings equal the qualified entity set;
- each GUID is deterministically derived from the published identity namespace;
- a semantic-only entry has no payload;
- every hydrated row refers to a qualified GUID and remains within its bound; and
- execution metadata is a supported certified mode within fragment and partition ceilings.

Unknown JSON fields are rejected. A logical failure increments an invalid-entry metric, invalidates the exact file and recomputes from certified truth.

## HPC, Kubernetes and RKE2 behavior

The query cache is an `emptyDir` mounted only by each query StatefulSet replica. It must be backed by reviewed node-local NVMe in the RKE2 query pool. It is deliberately disposable and per-replica: immutable object storage, the catalog, the offline reasoner certificate and the published serving root remain authoritative. A distributed cache would add a network hop, a new consistency protocol and another shared failure domain to a hot local operation.

Identical concurrent misses on one replica are single-flight. Different keys retain the existing Phase 27/28 global and tenant admission bounds and can run across Rust blocking lanes, query pods and RKE2 nodes. HPA remains the only online replica owner and CPU/memory targets remain at or below 80 percent. Required anti-affinity converts another query replica into demand for the `sparql-query-processing` RKE2 machine pool.

The query pod's equal ephemeral-storage request and limit must cover the immutable semantic cache, shuffle spill and complete-result cache. Helm validates that the result-cache application ceiling fits its volume, that one admitted entry can hold the largest bounded public response plus its header, and that the three mounted cache/spill volumes fit the pod's ephemeral-storage limit.

OpenMP, OpenBLAS and MKL remain fixed at one thread. This cache performs checksumming, sparse RDF/SPARQL validation and NVMe I/O; none is a dense matrix kernel. Rust still parallelizes independent query keys, certified hash partitions and hydration batches across bounded cores, pods and nodes. BLAS may be enabled only for a separately benchmarked dense scoring kernel with one shared cpuset budget.

## Metrics

The query role exports low-cardinality counters for `hit`, `miss`, `invalid` and `error`, plus current entry and byte gauges. The public response includes `x-ngkg-query-cache: hit|miss`. Metrics never label tenant IDs, query hashes, graph IRIs, GUIDs or RDF values.

## Acceptance criteria

1. Cache lookup happens only after authentication, tenant authorization, active-snapshot resolution and exact offline certificate resolution.
2. Every field in the exact identity above changes the key; an invalid lowercase SHA-256 is rejected.
3. A first certified request returns `miss`; an identical request routed to the same pod returns `hit`; their complete JSON bytes and independently expected SPARQL bags are identical.
4. Semantic-only and hydrated requests cannot reuse each other's entry.
5. A different tenant, dataset, snapshot, manifest, serving root or exact query cannot obtain the entry.
6. Truncation, append, header modification and payload modification are detected and never served.
7. A checksum-valid but logically invalid response is rejected, invalidated and recomputed.
8. Entry, byte, file and single-flight counts remain bounded under churn and identical concurrency; RSS and ephemeral storage plateau within the approved pod envelope.
9. Query-cache failure leaves the existing exact certified query path available and never returns a partial result.
10. HPA targets remain no greater than 80 percent, required anti-affinity creates query-pool demand, and the result cache plus prior volumes fit the equal ephemeral-storage request/limit.
11. OpenMP/BLAS do not create nested pools; sparse partitions and independent keys still use bounded Rust and Kubernetes parallelism.
12. Rust format/build/Clippy/tests, Helm schema/lint/render/server dry-run, NVMe behavior, pod restart, node replacement, authorization, service-mesh and RKE2 scaling gates pass.

Run `scripts/qualify_phase29.sh` through a same-pod test endpoint or sticky qualification route. It executes separate semantic-only and hydrated miss→hit pairs, compares each complete pair byte-for-byte, independently checks every certified result bag, proves the hydration modes occupy distinct entries, verifies headers and metrics, and inspects the deployed volume and HPA contract.

## Intentional boundary

Phase 29 is a per-replica complete-result cache for exact certified queries. It is not distributed durable truth, a cache-coherence system, a new reasoner or support for arbitrary OWL 2 DL/SPARQL. New snapshot identities naturally create cold keys; old disposable entries disappear through LRU or pod replacement. This phase does not claim 20–50× for cold, unselective or unsupported queries. It creates a defensible path to the 50× recurring-hot-query target, which still requires the same-hardware exact-result benchmark in `docs/RELEASE_ACCEPTANCE.md`.
