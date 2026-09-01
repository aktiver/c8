# Phase 16 — Distributed columnar semantic-artifact kernel

Phase 16 removes the next measured architectural bottleneck after deterministic RDF reduction: constructing the semantic spine, payload Parquet, query-visible RDF, reasoner-visible RDF, and GUID locator data on only one worker. It adds a real partition kernel that can run once per Phase 15 logical source shard and a bounded-memory finalizer whose result is independent of Kubernetes completion order.

## Data path

```text
Phase 15 source-plan + exact source shards + global dictionary
  → one artifact completion per logical partition
      ├── semantic-spine.parquet
      ├── payload.parquet
      ├── queryable.nq
      ├── reasoner-core.nt
      └── locator-run.tsv
  → exact partition barrier
  → k-way locator merge
  → distributed-artifact-root.json
```

The global dictionary is loaded once by each worker and maps canonical identifier terms (`I`) and canonical literal terms (`L`) to topology-stable 64-bit IDs. Subject, predicate, object, and named-graph joins therefore use integers. The partition worker reparses only one syntax-safe N-Quads shard; it never reparses the uploaded TriG document and never reads another worker's shard.

Every Parquet row retains the 128-bit FactID, the full 256-bit collision fingerprint, snapshot identity, source identity, treatment, and reasoning/query visibility. Payload rows preserve exact lexical value, datatype, language, graph ID, and predicate ID. Empty partitions still emit valid empty Parquet files and a manifest, so the barrier can prove exhaustive coverage without treating absence as success.

## Direct locator behavior

Each artifact worker produces a strictly sorted locator run keyed by the entity GUID and followed by logical payload partition, row group, row-in-group, graph term ID, and predicate term ID. The finalizer performs an external k-way merge with one resident row per input run. It rejects unsorted input and duplicate physical addresses.

The final locator is a direct sorted directory. A later serving process can memory-map or range-index it and binary-search a GUID without scanning Parquet files. The locator includes the logical payload partition, so hydration can resolve the exact payload object after the object-store manifest supplies that partition's URI and checksum.

## Determinism

Logical partition ownership still comes from Phase 15's full fact hash and immutable `logicalPartitionCount`. Phase 16 does not repartition facts according to the number of live pods. The same source plan, dictionary, mapping policy, row-group size, and worker binary must produce identical partition manifests regardless of pod scheduling or completion order.

The local acceptance exercise deliberately materializes partitions in forward order and reverse order. It then requires equality of:

- every partition manifest checksum;
- the global locator checksum;
- fact, semantic, payload, and locator counts; and
- the topology-independent semantic content root.

Run it with:

```bash
scripts/run_distributed_artifact_slice.sh
```

The script requires the pinned Rust toolchain and a reviewed `Cargo.lock`. Missing build prerequisites block the test.

## HPC and Kubernetes execution model

`materialize-artifact-partition` is an Indexed-Job-compatible command. The Kubernetes completion index maps directly to one immutable Phase 15 partition index. A production operator can set a large completion count with a smaller `parallelism` ceiling; Kueue and Cluster Autoscaler may change admitted pods and RKE2 node count without changing work ownership.

The worker itself is vectorized through Arrow and Parquet. Parallelism should primarily come from many exclusive-core partition pods. `OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, and `MKL_NUM_THREADS` remain one for this sparse encoding kernel; assigning multiple BLAS threads would oversubscribe CPUs without accelerating RDF encoding. Object downloads and uploads should use bounded asynchronous concurrency around the CPU stage.

Recommended responsibility mapping:

```yaml
semanticArtifactBuild:
  responsibility: semantic-artifact-build
  completions: 256
  maxParallelism: 64
  cpuPerCompletion: "1"
  memoryPerCompletion: 4Gi
  scratchPerCompletion: 16Gi
```

The production object-store/operator adapter must register every completion index and output manifest in PostgreSQL before this stage is admitted. It must upload Parquet and sidecar data first, upload the partition manifest last, and use exact keys and checksums rather than bucket listing. That control-plane integration is not silently assumed by this phase's local kernel.

## Intentional boundary

Phase 16 implements and tests the deterministic distributed artifact kernel. It does **not** yet replace the Phase 15 reference worker's single-node artifact production in the certified service path. The existing reference compiler and HermiT adapter remain the semantic oracle while the new artifact layout is qualified for byte/content equivalence, object-store publication, restart safety, and hydration compatibility.

This phase also does not claim that HermiT reasoning is distributed, that arbitrary OWL 2 DL SPARQL is certified, or that a TSV locator is the final serving format. A later phase must wire these artifact completions into the catalog/operator DAG, publish their object keys atomically, add the mmap locator service, and make the certifier consume the shard root only after equality with the reference snapshot is proven.

## Acceptance gate

Phase 16 passes only when:

1. each completion verifies the source plan, source shard, global dictionary, source identity, mapping policy, and resource ceilings before writing output;
2. every logical partition emits exactly five data artifacts and one manifest, including empty partitions;
3. semantic and payload row counts sum to the source-plan fact count;
4. payload locator count equals payload row count;
5. missing, duplicate, corrupt, unplanned, or checksum-mismatched partitions fail closed;
6. the global dictionary is dense, canonical, unique, and complete for every encoded term;
7. locator inputs are strictly sorted and the bounded-memory merge rejects duplicate physical rows;
8. forward, reverse, retried, and reordered completion matrices produce identical partition hashes, locator bytes, counts, and semantic root;
9. Parquet schemas and row-group limits match the reviewed contract; and
10. a supported environment compiles, lints, tests, validates Parquet metadata, and records one-node/N-node resource and throughput evidence.

Static source inspection is useful but does not satisfy the runtime gate. The phase verification report remains blocked when Rust, Parquet inspection tooling, and the external qualification environment are absent.
