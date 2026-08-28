# RKE2 autoscaling for NGKG

RKE2 runs Kubernetes; it does not create or delete machines in response to pending NGKG pods. The validated NGKG path is Rancher-provisioned RKE2 plus one upstream Cluster Autoscaler instance configured with the Rancher provider. Standalone RKE2 requires another capacity provider that can really resize its machines; otherwise select `provisioner: existing`.

## Capacity chain

1. The NGKG operator, HPA, or KEDA changes only the workload it owns.
2. Kueue admits batch work against a ResourceFlavor and quota.
3. Admitted pods carry a responsibility label/taint, complete requests, and become unschedulable when their pool lacks capacity.
4. Cluster Autoscaler changes the matching Rancher machine-pool quantity.
5. RKE2 joins a worker that already has the required label, taint, reservation, CPU Manager, Topology Manager, CNI, and storage configuration.
6. NGKG admits the pod only after node and dependency readiness.

## 80% saturation policy

NGKG uses `hpcRuntime.nodeSaturationTargetPercent: 80` as a hard upper configuration boundary. Phase 20 query and hydration HPAs evaluate CPU and memory utilization from Metrics Server, and neither target may be configured above 80%. Queue delay and bytes-in-flight values are reserved for a later phase that exports and qualifies those custom metrics; the current chart does not create HPA dependencies on metrics the service does not publish.

Query and hydration replicas have required anti-affinity by responsibility, so one replica of a given worker type occupies one node. Their whole-core requests equal limits and should match the measured allocatable shape after RKE2 `system-reserved` and `kube-reserved` deductions. At sustained 80% utilization the HPA adds a replica; that replica is unschedulable when no matching responsibility node has room; Cluster Autoscaler then grows only that Rancher pool. The unused 20% of the workload envelope protects page cache, CNI traffic, control work, recovery, and short bursts. It is not permission to omit RKE2 system reservations.

Validate this with real node metrics. Artificially drive one worker to 79%, confirm it remains stable, then sustain at least 80% and confirm a new replica and matching node appear. Repeat for memory. Scale-down must wait for ownership drain and must not evict the last certified locator or query replica.

## Responsibility-specific Rancher pools

Create separate worker machine pools for:

- `semantic-projection`
- `semantic-artifact-build`
- `reasoning`
- `index-build`
- `sparql-query-processing`
- `sparql-fragment-processing`
- `parquet-hydration`
- `maintenance-export`
- `storage-recovery`

Each pool must have Rancher Cluster Autoscaler min/max annotations and scale-from-zero resource annotations based on measured allocatable CPU, memory, and ephemeral storage. Control-plane and etcd pools are never part of NGKG scaling.

For Phase 20, four batch pools and two online pools are active. Start with measured warm minima, not these example quantities as universal sizing:

| Rancher machine pool | Node label and taint value | Phase 15 work | Example min/max |
| --- | --- | --- | --- |
| `ngkg-semantic-projection` | `semantic-projection` | safe planner and Indexed projection completions | 0/24 |
| `ngkg-semantic-artifact-build` | `semantic-artifact-build` | Indexed Arrow/Parquet and semantic-sidecar completions | 0/30 |
| `ngkg-index-build` | `index-build` | range reducers, artifact finalizer and immutable serving-root barrier | 0/12 |
| `ngkg-reasoning` | `reasoning` | HermiT, reference certification and sharded-hydration equivalence | 0/4 |
| `ngkg-sparql-query-processing` | `sparql-query-processing` | certified semantic query and mmap locator replicas | 3/30 |
| `ngkg-sparql-fragment-processing` | `sparql-fragment-processing` | checksum-bound named-graph fragment execution | 3/60 |
| `ngkg-parquet-hydration` | `parquet-hydration` | exact GUID-directed Parquet row-group hydration | 2/20 |
| `ngkg-storage-recovery` | `storage-recovery` | checksum-bound replica, relocation, backup, and restore partitions | 0/32 |

Keep a nonzero minimum for any pool whose cold-start time would violate the ingestion SLO. A zero minimum is valid only when the Rancher provider has accurate scale-from-zero CPU, memory, and ephemeral-storage annotations and the Kueue flavor has enough quota for one complete pod.

Every RKE2 agent template must join with its matching label and taint. A projection profile has this configuration intent:

```yaml
node-label:
  - ngkg.io/workload=semantic-projection
node-taint:
  - ngkg.io/workload=semantic-projection:NoSchedule
kubelet-arg:
  - cpu-manager-policy=static
  - topology-manager-policy=restricted
  - system-reserved=cpu=2,memory=4Gi,ephemeral-storage=20Gi
  - kube-reserved=cpu=1,memory=2Gi,ephemeral-storage=10Gi
```

Reservations and Memory Manager settings must be generated from the real machine topology. Update an immutable machine template, drain the old pool, and requalify cpuset/NUMA behavior; do not hand-edit autoscaled nodes.

Rancher must put the corresponding annotations on each scalable machine pool. Exact annotation keys depend on the supported Rancher/Cluster Autoscaler release, so obtain them from that version's Rancher provider documentation and verify them by inspecting the Cluster Autoscaler discovery logs. The intended values are:

```text
cluster-autoscaler enabled = true
cluster-autoscaler min size = operator-approved minimum
cluster-autoscaler max size = operator-approved maximum
scale-from-zero cpu = measured allocatable whole cores
scale-from-zero memory = measured allocatable bytes
scale-from-zero ephemeral storage = measured allocatable bytes
```

Do not place these machine-pool credentials or identifiers in the NGKG Helm release. The chart describes pod demand; the separately administered Rancher autoscaler maps unschedulable demand to machine-pool quantity.

## Cluster Autoscaler installation boundary

Install a Kubernetes-minor-compatible, digest-pinned Cluster Autoscaler outside the NGKG application release with:

```text
--cloud-provider=rancher
--cloud-config=/etc/cluster-autoscaler/cloud-config
--leader-elect=true
```

The cloud-config Secret comes from the platform's external secret manager and contains the Rancher URL, restricted token, downstream cluster name, and cluster namespace. NGKG values contain only the Secret name. The Rancher identity is limited to the documented target cluster and machine resources.

A representative autoscaler Deployment argument set is:

```yaml
args:
  - --cloud-provider=rancher
  - --cloud-config=/etc/cluster-autoscaler/cloud-config
  - --leader-elect=true
  - --balance-similar-node-groups=false
  - --expander=least-waste
  - --skip-nodes-with-local-storage=false
  - --scale-down-enabled=true
  - --scale-down-unneeded-time=15m
```

`balance-similar-node-groups=false` is intentional because the responsibility pools are not interchangeable. Validate every flag against the pinned autoscaler release; unsupported flags must fail installation rather than be silently removed. NGKG scratch is disposable `emptyDir`, but a worker must commit its immutable output before completion, so node loss is recovered by the Job retry and catalog CAS.

## RKE2 prerequisites

- keep `rke2-metrics-server` healthy for CPU/memory metrics;
- install a custom/external metrics adapter only when a later qualified NGKG release enables queue and latency metrics;
- install Kueue and KEDA before selecting those owners;
- use Canal, Calico, or Cilium in a tested NetworkPolicy-enforcing configuration;
- ensure new nodes can reach RKE2 registration/API, registry, object store, catalog, DNS, telemetry, and certificate endpoints;
- make Kueue quota large enough to admit at least one complete pod shape, or scale-from-zero cannot start;
- keep nonzero warm minima for query and locator capacity unless the product SLO explicitly accepts cold starts.

## Install sequence

```bash
python3 scripts/validate_helm_values.py charts/ngkg-workloads/values.yaml --overlay charts/ngkg-workloads/profiles/rke2.yaml
helm upgrade --install ngkg-crds charts/ngkg-crds --namespace data-platform
helm upgrade --install ngkg-platform charts/ngkg-platform --namespace data-platform --values approved-platform-values.yaml --wait --rollback-on-failure
helm upgrade --install ngkg-workloads charts/ngkg-workloads --namespace data-platform --values charts/ngkg-workloads/profiles/rke2.yaml --wait --rollback-on-failure
```

The approved platform values must supply digest-pinned images and existing dependency Secret references. CRD upgrades are reviewed separately because Helm does not manage CRD schema upgrades as ordinary templates.

For Phase 20, the approved platform overlay must explicitly size logical work separately from instantaneous pods:

```yaml
api:
  allowedResourceProfiles: [reference-balanced, distributed-hpc-v1]

distributedOperator:
  logicalPartitions: 256
  reducerCount: 16
  artifactRowGroupRows: '1048576'
  stages:
    plan:       {queue: ngkg-batch, cpu: '1',  memory: 128Gi, scratch: 512Gi, maxParallelism: 1,  activeDeadlineSeconds: 7200}
    projection: {queue: ngkg-batch, cpu: '1',  memory: 16Gi,  scratch: 256Gi, maxParallelism: 96, activeDeadlineSeconds: 3600}
    reducer:    {queue: ngkg-batch, cpu: '1',  memory: 32Gi,  scratch: 512Gi, maxParallelism: 24, activeDeadlineSeconds: 7200}
    finalize:   {queue: ngkg-batch, cpu: '1',  memory: 128Gi, scratch: 512Gi, maxParallelism: 1,  activeDeadlineSeconds: 7200}
    artifact_plan: {queue: ngkg-batch, cpu: '1', memory: 32Gi, scratch: 64Gi, maxParallelism: 1, activeDeadlineSeconds: 1800}
    artifact: {queue: ngkg-batch, cpu: '1', memory: 32Gi, scratch: 256Gi, maxParallelism: 96, activeDeadlineSeconds: 7200}
    artifact_finalize: {queue: ngkg-batch, cpu: '1', memory: 128Gi, scratch: 512Gi, maxParallelism: 1, activeDeadlineSeconds: 7200}
    serving_root: {queue: ngkg-batch, cpu: '1', memory: 128Gi, scratch: 512Gi, maxParallelism: 1, activeDeadlineSeconds: 7200}
    reasoner:   {queue: ngkg-batch, cpu: '32', memory: 512Gi, scratch: 1024Gi, maxParallelism: 1, activeDeadlineSeconds: 14400}
```

`logicalPartitions`, `reducerCount`, and `artifactRowGroupRows` affect immutable work identity and must be versioned. `maxParallelism` changes only how many completions Kubernetes may run at once and can be tuned without changing result bytes. Kueue cohort quota must cover the desired concurrent stage, while each Rancher pool maximum must be large enough to realize that quota.

Phase 40.13.11 applies the same rule to existing cloud TriG. `whole-trig-lpt-v1` fixes each
completion's complete-object set in the checksum-bound decode plan; `cloudCompiler.maxParallelism`
only caps simultaneous Indexed completions. Kueue admission and pending `source-ingestion` pods drive
the external node autoscaler, while `objectConcurrency` uses bounded CPU lanes inside each pod.
Changing pod or node count therefore cannot split a grammar stream or alter the compiler handoff.

The projection, merge, artifact and serving-root kernels are single-threaded inside each completion, so their pod shape requests one exclusive core and obtains parallelism from many independent completions across cores and nodes. The serving root is one global sorted publication barrier and truthfully stays at one core. Requesting 16 or 32 cores for a single-threaded worker would waste the pool and slow admission. The reasoner remains a larger measured JVM shape; after reasoning, its equivalence step uses the configured bounded Rust hydration lanes over independent Parquet row groups. Sparse stages keep OpenMP, OpenBLAS and MKL at one thread. Raise other per-pod CPU only after a cgroup-aware multi-threaded kernel is implemented and benchmarked.

After installation, verify the full ownership chain:

```bash
kubectl -n data-platform get deploy ngkg-distributed-operator
kubectl get resourceflavors.kueue.x-k8s.io
kubectl -n data-platform get localqueues.kueue.x-k8s.io
kubectl get nodes -L ngkg.io/workload
kubectl -n kube-system logs deploy/cluster-autoscaler --since=10m
```

An admitted projection pod should select and tolerate only `semantic-projection`; an artifact completion only `semantic-artifact-build`; a reducer/finalizer/serving-root barrier only `index-build`; and the reasoner/equivalence Job only `reasoning`. A pod that can land on a general-purpose or wrong-responsibility node is a failed configuration.

Phase 15 batch pods carry `ngkg.io/network-plane=batch` and are default-denied. Populate `networking.dependencyCidrs` with the private PostgreSQL and S3/MinIO endpoint CIDRs before qualification; the chart opens only DNS and the reviewed dependency ports. An empty list intentionally makes catalog/object access fail closed. Prefer private endpoints and narrow CIDRs rather than general internet egress.

## Phase 20 online serving overlay

Create the two online Rancher machine pools before installing the workload chart. Each `sparql-query-processing` node must fit one complete query pod plus the RKE2 reservations; each `parquet-hydration` node must fit one complete hydration pod. Required pod anti-affinity deliberately limits each role to one replica per node. Do not reduce requests to force bin-packing: HPA must create a pending pod so Cluster Autoscaler receives an unambiguous responsibility-specific demand signal.

An approved values overlay has this shape:

```yaml
images:
  query: {repository: registry.example/ngkg-online-serving, digest: sha256:<reviewed>}
  fragment: {repository: registry.example/ngkg-online-serving, digest: sha256:<reviewed>}
  locator: {repository: registry.example/ngkg-online-serving, digest: sha256:<reviewed>}
  hydration: {repository: registry.example/ngkg-online-serving, digest: sha256:<reviewed>}

onlineServing:
  databaseSecret: ngkg-online-database
  authTokensSecret: ngkg-online-auth
  objectStoreCredentialsSecret: ngkg-online-object-store
  artifactStoreBaseUrl: s3://ngkg-artifacts/production
  cacheSizeLimit: 128Gi
  maxObjectBytes: '68719476736'
  maxPayloadCacheBytes: '107374182400'
  maxResidentQueryRoutes: '64'
  maxResidentFragmentRuntimes: '16'
  maxRequestBytes: '67108864'
  maxQueryBytes: '1048576'
  maxQueryResponseBytes: '536870912'
  maxQualifiedEntities: '100000'
  maxHydrationRows: '1000000'
  maxHydrationResponseBytes: '268435456'
  hydrationWorkerThreads: '6'
  maxDistributedFragments: '64'
  maxDistributedIntermediateRows: '1000000'
  maxDistributedExchangeBytes: '268435456'
  maxFragmentResponseBytes: '67108864'
  fragmentArrowBatchRows: '8192'
  fragmentArrowHttpChunkBytes: '1048576'
  fragmentArrowChannelCapacity: '4'
  fragmentExchangeConcurrency: '16'
  shufflePartitions: '64'
  maxShuffleRequestBytes: '67108864'
  maxShuffleResponseBytes: '67108864'
  maxShuffleExchangeBytes: '536870912'
  shuffleExchangeConcurrency: '16'
  shuffleSpillSizeLimit: 256Gi
  maxShuffleSpillBytes: '214748364800'
  maxShuffleOpenFiles: '256'
  shuffleCacheSizeLimit: 128Gi
  maxShuffleCacheBytes: '107374182400'
  maxShuffleCacheEntries: '8192'
  maxShuffleCacheEntryBytes: '134217728'
  maxQueryInFlight: '12'
  maxFragmentWorkerInFlight: '16'
  maxFragmentInFlight: '8'
  maxShuffleInFlight: '16'
  maxLocatorInFlight: '128'
  maxHydrationInFlight: '1'
  maxQueryPending: '24'
  maxFragmentPending: '16'
  maxShufflePending: '32'
  maxLocatorPending: '256'
  maxHydrationPending: '8'
  admissionWaitMilliseconds: '250'
  fragmentTimeoutSeconds: '30'
  hydrationTimeoutSeconds: '60'

resources:
  query:
    requests: {cpu: '16', memory: 64Gi, ephemeral-storage: 512Gi}
    limits: {cpu: '16', memory: 64Gi, ephemeral-storage: 512Gi}
  fragment:
    requests: {cpu: '16', memory: 64Gi, ephemeral-storage: 384Gi}
    limits: {cpu: '16', memory: 64Gi, ephemeral-storage: 384Gi}

hpcNodeGroups:
  sparql_query_processing_num_of_nodes: 3
  sparql_fragment_processing_num_of_nodes: 3
  parquet_hydration_num_of_nodes: 2
autoscaling:
  sparqlQueryProcessing: {owner: hpa, minNodes: 3, maxNodes: 30}
  sparqlFragmentProcessing: {owner: hpa, minNodes: 3, maxNodes: 60}
  parquetHydration: {owner: hpa, minNodes: 2, maxNodes: 20}
metrics:
  cpuUtilizationTargetPercent: 80
  memoryUtilizationTargetPercent: 80
```

`ngkg-online-database` must contain `database-url`. `ngkg-online-auth` must contain `tokens.json`; every allowed online identity has a lowercase SHA-256 token hash, tenant UUID, principal ID, and `queries:execute` scope. The optional object-store Secret is exposed only as SDK environment variables and must use the restricted read-only identity for the configured artifact prefix. The chart never writes credentials into command arguments or generated manifests.

The same digest-built `ngkg-online-serving` image runs query, fragment, locator, and hydration roles. Query and fragment roles have bounded Tokio control threads and synchronous SPARQL blocking lanes. Locator and hydration roles load the checksum-verified fixed-width index into read-only mmap virtual memory. Hydration divides independently addressed Parquet row groups across `hydrationWorkerThreads`. Sparse kernels set `OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1`, and `MKL_NUM_THREADS=1`; this is intentional. The fully-bound RDF join fast path builds a Rust hash index inside each hash-owned partition, and independent partitions run concurrently across blocking lanes, pods and nodes. A future dense reranker may use BLAS only in a separately sized pod profile where Rust and native threads share one explicit cpuset budget.

Phase 21 query replicas load a checksum-bound asserted named-graph route per exact certified query rather than the complete asserted dataset. Set `onlineServing.maxResidentQueryRoutes` from measured p95 route memory, the closure size, concurrent query lanes, and the query pod limit. The service applies an LRU count bound, coordinated local-file eviction, and single-flight route construction. Verify memory and `emptyDir` usage with more unique certified queries than the bound; both must stabilize after eviction. HPA remains a replica scaler, not a substitute for these in-process bounds.

Phase 22 adds a separate `sparql-fragment-processing` Rancher pool. The offline compiler accepts a distributed plan only after executing every graph-local fragment, performing the exact bag join, and proving equality with both the independent expected result and the complete Phase 21 route result. Query coordinators resolve ready pod addresses from the headless `ngkg-fragments` Service and require at least two distinct worker identities. Each worker downloads only manifest-listed fragment, query, plan, and closure artifacts, verifies hashes and sizes, and executes the exact certified fragment. Response, total exchange, fragment count, intermediate row count, concurrency, request time, and resident runtime counts are all operator-bounded. A worker cache eviction removes its verified local fragment copy; immutable objects remain the recovery source.

Phase 23 changes only that internal binding transport to `certified-arrow-ipc-rest`. Set `onlineServing.fragmentArrowBatchRows` from measured binding width and the fragment pod's memory envelope; it must remain positive and no larger than `maxDistributedIntermediateRows`. `fragmentArrowHttpChunkBytes` and `fragmentArrowChannelCapacity` bound the back-pressured encoder-to-socket queue, and their product cannot exceed `maxFragmentResponseBytes`. Smaller batches reduce temporary column allocation, while larger batches reduce IPC framing overhead. Qualification must measure both rather than assuming the default 8,192 rows is optimal. The query response reports `arrow_ipc_stream_v1`, so operators can prove the new image is not silently using the former JSON path.

Phase 24 uses the same fragment pool for certified partitioned hash joins. `shufflePartitions` is the logical parallelism per join stage and must be at least two; it does not request that many Kubernetes nodes. `shuffleExchangeConcurrency` limits the simultaneous Arrow requests and cannot exceed the partition count. Per-request and per-response ceilings must fit below `maxShuffleExchangeBytes`, which counts both directions across every stage. Start with at least twice as many partitions as the maximum expected ready workers, then benchmark key skew and coordinator memory. Increasing logical partitions cannot correct a single hot key; adaptive skew splitting is not implemented and must not be claimed.

Phase 25 adds a dedicated `shuffle-spill` `emptyDir` to query pods. Configure the RKE2 query pool so kubelet/containerd ephemeral storage is physically backed by the reviewed local NVMe device; the chart cannot turn a slow system disk into NVMe. Query `ephemeral-storage` requests equal limits and must cover both `cacheSizeLimit` and `shuffleSpillSizeLimit`. `maxShuffleSpillBytes` is the application ceiling for one stage and must not exceed the volume size. `maxShuffleOpenFiles` must be at least twice `shufflePartitions` because each partition has one left and one right writer. Disk exhaustion, corruption or cleanup failure stops the query.

Phase 26 adds a separately bounded `shuffle-cache` `emptyDir` to fragment pods. Fragment `ephemeral-storage` requests equal limits and must cover `cacheSizeLimit + shuffleCacheSizeLimit`, plus an operator-reviewed margin for logs and writable layers. `maxShuffleCacheBytes` cannot exceed the volume, `maxShuffleCacheEntryBytes` cannot exceed the application cache ceiling, and `maxShuffleCacheEntries` independently bounds inode and lock pressure. The cache is disposable: pod replacement creates a cold worker, while all semantic truth remains in the catalog and immutable object storage.

Phase 27 makes each replica reject excess work before request-body extraction. Set the five operation limits from measured per-request CPU, RSS, Arrow buffer and Parquet decode envelopes; `maxFragmentWorkerInFlight` is a hard parent ceiling shared by fragment evaluation and shuffle joins. Keep `admissionWaitMilliseconds` short enough that callers can retry another ready replica while HPA and Cluster Autoscaler react. Label Prometheus scraper pods `ngkg.io/metrics-client=true`; the default-deny policy otherwise blocks `/metrics`. Alert on rejection rate and admission wait, but retain HPA as the only online replica owner unless a reviewed migration explicitly replaces it with one custom-metric controller.

Phase 28 requires a tenant admission Secret with the key `tenant-policy.json`. Compute lowercase SHA-256 values for that file and the existing `tokens.json`, set `onlineServing.authTokensFileSha256`, `onlineServing.tenantAdmissionSecret`, `onlineServing.tenantAdmissionPolicySha256` and `onlineServing.maxAdmissionTenants`, then perform a Helm upgrade. Both checksums are copied into every online pod template, which forces query, fragment, locator and hydration replicas to roll together. Each policy limit must fit beneath its matching Phase 27 global limit; with multiple tenants, every individual limit must be strictly smaller so at least one peer lane remains. Size the global envelope from measured pod resources first; divide that capacity into tenant lanes second. HPA remains the sole online replica owner at the 80-percent ceiling, while tenant rejection protects peers during scaling lag.

Phase 29 adds `query-result-cache` only to query replicas. Configure `onlineServing.queryResultCacheSizeLimit`, `maxQueryResultCacheBytes`, `maxQueryResultCacheEntries` and `maxQueryResultCacheEntryBytes`. The entry limit must cover `maxQueryResponseBytes + 80`, while the cache byte ceiling must fit its `emptyDir`. Query ephemeral storage must cover `cacheSizeLimit + shuffleSpillSizeLimit + queryResultCacheSizeLimit`; the chart's cross-field validator rejects an undersized pod. Back `/var/lib/ngkg/query-result-cache` with reviewed local NVMe through the RKE2 query-node filesystem and kubelet/containerd placement. The volume is disposable, so do not use a network PersistentVolume or treat it as semantic truth. Required anti-affinity and the existing 80-percent HPA continue to create one new `sparql-query-processing` pool demand per added query replica.

For deterministic cache qualification, port-forward one freshly started query pod rather than the load-balanced Service. Confirm semantic-only and hydrated requests each report `miss` then `hit`, each pair is byte-identical, both independently expected SPARQL bags match, the two hydration modes occupy separate keys, and entry/byte gauges remain within the configured ceiling. Then repeat with churn, maximum response sizes, pod deletion and node replacement. Cache loss must produce a cold recomputation, never an unavailable or partial semantic result.

Phase 30 adds `worker-join-spill` only to fragment replicas. Configure `workerJoinSpillSizeLimit`, total and per-request spill bytes, bucket/open-file counts, build/probe chunks, maximum row bytes and the in-memory threshold. The chart rejects a total limit larger than the `emptyDir`, a per-request limit larger than the process total, fewer than two buckets, fewer than two file slots per bucket, row chunks above the distributed ceiling, or fragment ephemeral storage below `cacheSizeLimit + shuffleCacheSizeLimit + workerJoinSpillSizeLimit`. Back `/var/lib/ngkg/worker-join` with reviewed node-local NVMe on the `sparql-fragment-processing` RKE2 pool. The spill is disposable and must not be placed on a shared network PersistentVolume.

No Phase 30 node group is added. Grace join is the bounded physical implementation of the existing fragment/shuffle responsibility. Independent primary partitions run across the current anti-affine fragment pods and nodes; the fragment HPA still requests replicas at no more than 80-percent CPU or memory, and pending replicas still drive only the `sparql-fragment-processing` Rancher pool. Validate with a certified skew dataset large enough to exceed `inMemoryJoinBuildRows`, confirm positive `workerJoinSpillBytes`, and confirm `ngkg_worker_join_active_spill_bytes` returns to zero before allowing scale-down.

Phase 31 adds `streaming-request-spool` to the same fragment replicas; it is not a new responsibility node group. Set `streamingRequestSpoolSizeLimit` for the node-local-NVMe `emptyDir` and `maxStreamingRequestSpoolBytes` for process-wide admitted request bytes. The chart requires the latter to fit the former, requires one `maxShuffleRequestBytes` request to fit the process budget, and adds the volume to fragment ephemeral-storage arithmetic. Continue scaling the anti-affine `sparql-fragment-processing` pool at the existing HPA target of at most 80 percent. Before scale-down, require both `ngkg_streaming_request_spool_active_bytes` and `ngkg_worker_join_active_spill_bytes` to return to zero.

Phase 32 adds no node group or volume. It uses the existing `sparql-query-processing` pool and query `shuffle-spill` NVMe volume, with RAM bounded by `fragmentArrowBatchRows` plus `fragmentArrowHttpChunkBytes × fragmentArrowChannelCapacity` per concurrent exchange. The existing chart validator requires this channel bound to fit the shuffle request/response ceiling. Query HPA targets stay at or below 80 percent, and anti-affine pending replicas continue driving the matching RKE2 node pool.

Phase 33 adds `fragment-response-spool` to the existing `sparql-query-processing` pool; it is not a new responsibility node group. `fragmentResponseSpoolSizeLimit` bounds the pod volume and `maxFragmentResponseSpoolBytes` bounds live process reservations. The chart requires the process budget to cover the admitted distributed exchange and the query pod's Guaranteed-QoS ephemeral-storage request to cover cache, shuffle spill, query-result cache and fragment responses together. Require `ngkg_fragment_response_spool_active_bytes` to reach zero before voluntary query-pod scale-down.

Phase 34 adds no node group, volume or autoscaler. It reuses the query pool's fragment-response and shuffle-spill local-NVMe volumes and converts validated rows directly between them. The query HPA remains capped at 80-percent CPU and memory; required anti-affinity turns a new replica into one additional `sparql-query-processing` Rancher node request when the pool is full. Before scale-down, require the fragment-response gauge to be zero and verify no stage directories remain below the shuffle-spill marker. Size ephemeral storage for both volume ceilings because direct partitioning temporarily retains the source spool and destination stage together.

Phase 35 also adds no node group or volume. The existing fragment-response spool now owns stage-result leases as well as initial fragments, so `maxFragmentResponseSpoolBytes` must cover `maxShuffleExchangeBytes`; Helm rejects a smaller budget. Lazy replay drops each source lease at EOF, and the query HPA remains capped at 80-percent CPU and memory. Before voluntary scale-down, require zero active response/request/Grace bytes and no unmanaged stage entries on the node-local NVMe volumes.

```bash
policy_sha256="$(sha256sum tenant-policy.json | awk '{print $1}')"
tokens_sha256="$(sha256sum tokens.json | awk '{print $1}')"
kubectl --namespace ngkg create secret generic ngkg-tenant-admission \
  --from-file=tenant-policy.json=tenant-policy.json \
  --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install ngkg-workloads charts/ngkg-workloads \
  --namespace ngkg --values approved-rke2-values.yaml \
  --set-string onlineServing.authTokensFileSha256="${tokens_sha256}" \
  --set-string onlineServing.tenantAdmissionSecret=ngkg-tenant-admission \
  --set-string onlineServing.tenantAdmissionPolicySha256="${policy_sha256}"
kubectl --namespace ngkg rollout status statefulset/ngkg-query-shard
kubectl --namespace ngkg rollout status statefulset/ngkg-fragment-worker
kubectl --namespace ngkg rollout status statefulset/ngkg-locator
kubectl --namespace ngkg rollout status deployment/ngkg-hydration
```

OpenMP/BLAS settings of one apply only to native dependency pools. The Rust query runtime still schedules independent spill reads, Arrow encodes and partition requests concurrently, fragment pods process independent requests across their bounded blocking threads, and Kubernetes spreads pods across nodes. Do not increase OpenMP or BLAS thread counts for sparse joins; benchmark a separate cpuset-budgeted profile only if a future dense reranker or native join kernel actually calls those libraries.

No new Rancher machine pool is required. Fragment evaluation and hash joining are the same sparse responsibility, use the same immutable artifacts, and are scheduled on `sparql-fragment-processing`. This lets the fragment HPA and Cluster Autoscaler apply existing idle capacity to either operation. The query response must report both `shufflePartitionCount` and at least two distinct `shuffleWorkerCount` identities before deployed qualification accepts the distributed path.

## Phase 40.13.20 production qualification overlay

Phase 40.13.20 requires the complete production control chain. Apply
`profiles/phase40.13.20-production.yaml` together with
`profiles/phase40.13.20-rke2.yaml` only after the externally managed Rancher Cluster Autoscaler,
Metrics Server, custom-metrics adapter, and Kueue are healthy. The RKE2 overlay selects the Rancher
provider and references the approved cloud-config Secret; it does not install or own Rancher
credentials. Other Kubernetes targets use the provider overlays documented in
`docs/phases/PHASE_40_13_20.md`.

Every online HPA uses both CPU and memory targets of exactly 80 percent. Batch operators create
additional Indexed Job completions from immutable plans, and Kueue admits their resource flavors.
Accurate Guaranteed-QoS requests plus responsibility selectors turn additional pod demand into
pending pods when the existing pool has exhausted its 20-percent reserve. Rancher Cluster
Autoscaler then grows only the matching machine pool. Do not deploy a second controller that writes
the same HPA target or directly races Rancher pool size.

Before voluntary drain, require all applicable checkpoint and spill gauges to be zero. A node-loss
test must restart the same immutable completion indexes, recover from the last checksum-bound
checkpoint, and reproduce the baseline result and artifact-root hashes. Run the read-only live
collector documented in `docs/phases/PHASE_40_13_20.md` after the approved load and chaos sequence.

The fragment headless Service is deliberately single-stack so one DNS address represents one ready pod; set `networking.fragmentIpFamily` to the RKE2 cluster's `IPv4` or `IPv6` service family. The coordinator still verifies distinct worker IDs returned by the pods, so duplicate DNS identity cannot be mistaken for cross-node execution.

One fragment worker is required per dedicated node. Its CPU and memory requests equal limits, and the matching RKE2 machine pool must expose an allocatable shape that fits exactly one pod after system reservations. `averageUtilization: 80` therefore creates another replica when the existing requested envelope sustains 80 percent use; required anti-affinity makes that replica pending when the pool is full, which gives Cluster Autoscaler an unambiguous `sparql-fragment-processing` demand. Sparse Oxigraph kernels do not call BLAS, so OpenMP, OpenBLAS, and MKL stay at one thread. Independent fragment requests use bounded Rust blocking lanes across the exclusive cpuset; enabling nested native threads would oversubscribe the node.

The data plane is default-denied. Query callers require the label `ngkg.io/query-client=true`; internal query-to-hydration and query-to-fragment traffic is explicitly permitted. Populate `networking.dependencyCidrs` with PostgreSQL and object-store private CIDRs or the pods will remain unable to load snapshots. The application protocol is authenticated HTTP inside that policy boundary, and `networking.tlsMode: external-service-mesh-required` is an explicit deployment prerequisite rather than a claim that the Rust listener terminates TLS. Production confidentiality requires the approved RKE2 service-mesh/CNI mTLS path or an equivalent reviewed internal TLS layer; mounting a TLS Secret alone is not evidence that mTLS is active.

Prove the 80% chain rather than inferring it from YAML:

1. At healthy queue pressure, hold a replica near 79% of its requested CPU and memory and verify no resource-driven scale-up.
2. Sustain at least 80% and verify HPA increments the matching workload.
3. Verify required anti-affinity leaves the new replica pending when the responsibility pool is full.
4. Verify Rancher Cluster Autoscaler grows only the matching machine pool and the node joins with the correct label, taint, reservations, and CPU-manager policy.
5. Verify the pod receives exclusive whole cores, becomes ready, and reconstructs its cache only from checksum-addressed objects.
6. Remove load and verify HPA and node scale-down stabilization respect the PDB and do not interrupt the final available replica.

## Acceptance probe

For each responsibility pool, submit one real admitted envelope whose pod can fit only that pool. Verify that only the intended Rancher quantity changes, the new node joins with exact labels/taints and measured allocatable resources, NetworkPolicy and mTLS probes pass, the work commits one checksummed result, and scale-down respects PDB, drain, cache ownership, and local-spill policy.

For the distributed build, also force projection and reducer node loss, restart the distributed operator, and vary Indexed Job parallelism. Catalog work must remain single-terminal, the canonical source and dictionary must stay byte-identical, and HermiT/query certification must match the one-node reference result. NGKG uninstall must leave RKE2, machine pools, Metrics Server, Kueue/KEDA, and Cluster Autoscaler intact.
