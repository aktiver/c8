# HPC execution on Kubernetes

NGKG distributes work at two levels. Across nodes, immutable envelopes, hash/range reducers, graph fragments, semijoin exchanges, path frontiers, locator shards, and hydration ranges define ownership. Within a pod, bounded Rust pools and selected native kernels use only the CPUs assigned by Kubernetes.

## Core and NUMA rules

- Dedicated pools use static CPU Manager, static Memory Manager where supported, and restricted or single-NUMA-node Topology Manager.
- HPC pods request whole CPUs with equal requests and limits so kubelet can provide an exclusive cpuset.
- Startup reads the effective cgroup cpuset, validates the sum of Rust compute, blocking I/O, OpenMP, BLAS, and control threads, and exports the capability report.
- Default images run OpenMP and BLAS with one thread each and disable dynamic/nested native parallelism. A measured kernel profile may move cores from Rust to OpenMP or BLAS, but total ownership cannot exceed the cpuset.
- BLAS is reserved for dense scoring/alignment kernels. GUID lookup, bitmap intersection, sparse traversal, hash joins, and property paths use integer/SIMD/bitmap/CSR algorithms.
- Local NVMe stores only bounded spill, shuffle, or immutable cache. Every stage commits remotely before advancing.

## Multi-node data flow

Online graph shards and locator replicas use headless Services for ownership-aware direct routing. Phase 23 carries certified columnar bindings as bounded Arrow IPC streams over authenticated internal REST. Phase 24 hashes fully bound join keys into stable logical partitions and executes those exact bag joins across the fragment worker nodes. The coordinator still dispatches and gathers partitions; direct peer shuffle and Arrow Flight remain later transport work. The public API remains REST; bulk uploads, exports, and large results use object artifacts.

Default-deny policy applies to the data plane. DNS, Flight/locator ports, and explicitly configured dependency CIDRs are separate rules. Empty dependency CIDRs intentionally leave external data access denied until a private endpoint or audited egress path is supplied.

## Scaling ownership

- HPA owns query/hydration replica counts from queue-delay and in-flight-work metrics.
- The NGKG operator owns deterministic batch Job creation and parallelism.
- KEDA may own only separately selected independent queue-driven Jobs.
- Kueue owns batch admission and resource flavor.
- Cluster Autoscaler or a provider provisioner owns node capacity.

No two controllers write the same replica or Job-count field. Query scale-in requires ownership drain and checksum-verified replica handoff before Kubernetes termination readiness is removed.

CPU and memory autoscaling targets are capped at 80% of each whole-node worker envelope. Required anti-affinity makes a new query, fragment/shuffle, or hydration replica become new responsibility-pool demand instead of being packed onto an already saturated node. Custom queue and in-flight-byte metrics may scale earlier. RKE2 reservations remain separate; the 20% workload headroom is used for page-cache, network, recovery, and burst tolerance rather than for another long-running worker.

Phase 24 deliberately reuses the fragment pool for shuffle joins. Both are sparse, integer/hash-oriented kernels over the same certified binding cache and have the same trust boundary. A separate shuffle node group would add another transfer and divide idle capacity. Logical `shufflePartitions` can substantially exceed current pod count; Kubernetes may add pods when CPU or memory reaches the 80% target, while the application maps independent partitions round-robin over whatever ready workers exist.

Phase 25 externalizes each stage's partition inputs to bounded query-node local NVMe. Rust consumes rows into hash-owned files sequentially, then replays only `shuffleExchangeConcurrency` partition pairs at once. Join work is still multi-core: each fragment pod accepts concurrent partition requests, builds a Rust hash index for each fully-bound partition and runs exact joins on bounded blocking lanes, while Kubernetes distributes those requests across nodes. Since equal keys have exactly one owner, the coordinator bag-unions complete partition outputs; it does not repeat the join globally. `OMP_NUM_THREADS=1` and the equivalent BLAS settings prevent nested native pools; they do not serialize Rust or cluster execution. The GUID locator continues to use safe read-only anonymous mmap because random fixed-width binary lookup benefits from it. Sequential one-pass spill uses buffered I/O and kernel readahead rather than copying the spill file into RAM-backed mmap.

Phase 26 adds a bounded local-NVMe result cache to each fragment worker. It does not cache by query text alone: tenant, dataset, snapshot, certified plan, stage, partition, partition count and canonical input-bag checksums form the identity. Hits revalidate the file checksum, output partition and output multiset before use. Independent cache misses still occupy Rust blocking lanes and spread across nodes; identical misses within one worker are single-flight. Whole-entry buffered I/O is preferable to mmap for this sequential verified payload, while the fixed-width random GUID locator remains mmap-backed.

Phase 27 bounds work before HTTP body extraction and holds its execution permit through response-body completion or disconnect. This couples Arrow backpressure to real pod capacity instead of allowing accepted streams to escape the concurrency envelope. Separate fragment/shuffle limits share one fragment-worker ceiling; Prometheus counters expose saturation without high-cardinality tenant, GUID or query labels. Rust partitions, pods and nodes remain the sparse parallelism layers; Parquet and mmap retain their cumulative roles, and BLAS/OpenMP remain single-threaded because admission control and RDF hash joins are not dense matrix kernels.

Phase 28 adds a checksum-bound finite tenant envelope inside each global operation envelope. A noisy tenant can fill its own Rust/Arrow/Parquet lanes but cannot reserve every unused pod lane from another configured tenant. This isolation does not add a competing scaler or thread pool: HPA still owns replicas, RKE2 Cluster Autoscaler still owns matching VM-pool capacity, and the existing sparse partition plan still distributes useful work across cores and nodes.

Phase 29 adds a separately bounded complete-result cache on query-node local NVMe. The cache applies only after exact offline certification and optional Parquet hydration, and every hit revalidates the certified SPARQL bag, route, GUID derivation and payload envelope. Verified files are read into safe anonymous mmap storage, made read-only and owned directly by the streamed response. Identical misses are single-flight per replica; independent keys still use bounded Rust lanes and distribute across query pods and RKE2 nodes. OpenMP/BLAS stay at one because hashing, sparse RDF validation, checksumming and NVMe reads are not dense matrix operations.

Phase 30 adds a second, domain-separated worker-local Grace partition only when a primary owner partition's right side exceeds the in-memory threshold. Fragment pods process different primary partitions concurrently across bounded Rust blocking lanes and RKE2 nodes. Within one request, sequential local-NVMe bucket replay limits each Rust hash index to `maxWorkerJoinBuildRows` and each probe batch to `maxWorkerJoinProbeRows`; rescanning a left bucket trades I/O for a hard hot-key memory bound. Each transient file is identity- and checksum-bound. Buffered sequential I/O is used instead of mmap because these files are disposable streams rather than random fixed-width indexes. OpenMP/BLAS remain one thread for the sparse equality kernel, avoiding nested oversubscription while pod/task parallelism uses the assigned cores.

Phase 31 applies the same bounded-I/O discipline before the Grace operator. HTTP chunks are written directly to a process-budgeted local-NVMe request spool; an incremental Arrow decoder validates and hashes rows without retaining complete relations, then large inputs flow directly into secondary buckets. This exploits pod/core/node concurrency without an unbounded per-request heap and provides HPA-visible CPU, memory and ephemeral-storage pressure. It intentionally uses buffered sequential I/O, not mmap, for disposable attacker-sized streams.

Phase 32 makes coordinator transmission bounded as well. Each query task reads one checksum-verified spill record at a time, builds at most one configured Arrow batch, and queues only the configured number of HTTP chunks. Backpressure prevents a slow fragment worker from turning a distributed partition fan-out into unbounded query-pod memory. Partitions still run concurrently across Rust tasks, anti-affine pods and RKE2 nodes; OpenMP/BLAS remain single-threaded for this sparse serialization path.

Phase 33 makes initial fragment ingress out-of-core. Query pods receive fragment responses concurrently but write chunks directly to a dedicated, process-budgeted local-NVMe `emptyDir`. SHA-256 verification and incremental Arrow decoding occur from the file with one record batch resident. This is sequential I/O, so buffered NVMe reads are preferable to mmap; mmap remains reserved for immutable random-access indexes and Parquet remains the columnar enterprise payload store.

Phase 34 sends those verified fragment rows directly into stable primary hash owners. Validation retains only stream identity, row count, head and always-bound-variable summaries; the production shuffle path does not create a complete owned row vector for any initial fragment. Each join stage opens all partition writers once, consumes one row at a time and writes to the owning NVMe file. Later partition requests remain parallel across bounded Rust tasks, fragment pods and RKE2 nodes. OpenMP/BLAS remain at one thread because the kernel is sparse RDF parsing and hashing, not a dense matrix operation; mmap remains for immutable random-access indexes, not disposable sequential streams.

Phase 35 makes the reverse exchange out-of-core as well. Shuffle-worker response chunks land in the process-budgeted response spool, one partition at a time is exactly verified, and lazy ordered spool replay feeds the next primary hash stage without a stage-wide row vector. Concurrent network partitions and fragment workers use the cluster; sequential coordinator verification keeps the owned bag to one bounded partition. Disposable streams use buffered NVMe I/O, immutable random-access indexes retain mmap, Parquet retains enterprise payload, and native dense-library threads remain one.

## Phase 15 distributed build mapping

Phase 15 uses Kubernetes Indexed Jobs because completion indexes are compact scheduling coordinates, not semantic identities. PostgreSQL maps each index to a stable work ID, exact input key, and checksum. A replacement pod on another node resolves the same immutable work.

| Stage | Cross-node unit | In-pod execution | Exchange shape |
| --- | --- | --- | --- |
| Plan | one syntax-safe source plan | complete standards parse and FactID bucketing | checksum-addressed N-Quads shards |
| Projection | one logical source partition | validation, canonical sorting, term extraction | fact and term runs |
| Reducer | one modulo-owned partition range | bounded k-way merge | reduced fact and term runs |
| Finalize | one reducer-root barrier | coverage proof, final merge, dense dictionary | canonical source and root manifest |
| Reasoner | one certified snapshot attempt | Phase 13 Parquet/locator build and HermiT | immutable snapshot artifacts |

Projection and reducer parallelism are bounded by the operator and may be lower than completions. The slowest admitted completion dominates each barrier, so production tuning must measure skew by fact count and bytes per logical partition. A future layout profile may use sampled range boundaries, but changing the profile requires the same one-versus-N equivalence gate and a new immutable profile identifier.

Reducers never download every projection run. They receive only the successful runs whose partition index maps to their reducer. Finalization reads all reducer roots, which is intentionally a small fan-in controlled by `reducerCount`. No worker lists object storage.

The current compute kernels are RDF parsing, sorting, hashing, external merging, Parquet/Arrow generation, and OWL reasoning. BLAS is not used for sparse graph work merely to claim HPC. OpenMP/BLAS threads are pinned to one in the distributed worker Jobs to avoid multiplying pod-level concurrency by native-library concurrency. HermiT receives its own whole-core and memory-heavy responsibility pool.

## Failure and backpressure behavior

- Kueue admission limits the number of whole pod shapes admitted at once.

Phase 40.13.11 adds whole-TriG decode work. Complete objects are largest-first balanced into stable
Indexed completions, and each pod runs a bounded number of parser/upload lanes. OpenMP, BLAS, and
MKL remain single-threaded because RDF tokenization is not a dense numeric kernel; Kubernetes supplies
inter-pod and inter-node parallelism, and exact CPU/memory/ephemeral-storage requests create safe
Cluster Autoscaler demand without oversubscribing cgroups.
- Kubernetes Job retry handles node and process loss; catalog compare-and-swap rejects divergent duplicate outputs.
- Object data is committed before its manifest; the catalog commit occurs after the manifest is verified remotely.
- A completed Job with an uncommitted expected barrier is a protocol failure, not success.
- Finalizer completion is checked against the immutable distributed root because the catalog remains `INDEXED` before and after root publication.
- Each pod has a bounded scratch volume. Exhaustion fails that completion and cannot partially advance the catalog.
- Phase barriers prevent reducers from consuming incomplete projection sets and prevent the reasoner from consuming an incomplete root.

This structure maximizes safe distribution without pretending the TriG grammar or full OWL 2 DL reasoning can be divided at arbitrary byte or graph boundaries.

## Phase 40.13.19 storage recovery mapping

Recovery uses one stable logical task per immutable snapshot artifact and destination. Kubernetes
Indexed Jobs distribute those tasks across whole-core pods and dedicated autoscaled nodes; the
logical plan does not change when pod count, node count, retry count, or task parallelism changes.
Rendezvous hashing provides deterministic placement across failure domains. Bounded scratch,
multipart buffers, task byte ceilings, Kueue quotas, and an operation-wide in-flight byte ceiling
prevent a large backup or relocation from exhausting cluster RAM or local storage.

The transfer kernel is streaming object I/O plus SHA-256, so nested OpenMP/BLAS/MKL pools remain at
one thread. Useful parallelism comes from independent Rust tasks, Indexed Job completions, pods,
failure-domain node pools, and Cluster Autoscaler capacity. A separate all-results barrier prevents
autoscaling, eviction, duplicate delivery, or a missing node from changing the certified outcome.
