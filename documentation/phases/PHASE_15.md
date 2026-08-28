# Phase 15 — Deterministic distributed compilation foundation

Phase 15 is the first multi-node compilation increment. It keeps the proven Phase 13/14 compiler and reasoner as the certification boundary, but moves syntax-safe normalization, logical partition validation, cross-node reduction, and canonical-source construction into a durable distributed DAG.

## What is real in this phase

The API accepts the existing checksum-bound compilation bundle with `resourceProfile: distributed-hpc-v1`. A dedicated operator then reconciles catalog state into five idempotent Kubernetes Jobs:

```text
plan (one complete TriG parse)
  → projection (Indexed Job, one completion per logical partition)
  → reducer (Indexed Job, disjoint ranges of projection partitions)
  → finalize (one global coverage and canonical-byte commit)
  → reasoner/certifier (the existing Phase 14 worker and HermiT adapter)
```

The implementation is cumulative. It reuses the Phase 14 tenant, API, object-store, snapshot, publication, cancellation, and reasoner contracts. It adds:

- `ngkg-distributed-build`, a deterministic partition/reduction library;
- `ngkg-distributed-worker`, with local and catalog/object-store execution modes;
- `ngkg-distributed-operator`, a level-based Kubernetes controller;
- migration 3, which stores immutable plans, exact work indexes, output manifests, and the global root under forced row-level security;
- source-plan, run-manifest, and equivalence-matrix JSON contracts;
- Helm resources for the new operator, workers, Kueue flavors, and responsibility-specific nodes; and
- an API projection of distributed counts at `GET /v1/jobs/{operationId}`.

## Determinism and topology invariance

The planner parses the entire uploaded TriG document with a standards parser. It never cuts arbitrary byte ranges. Blank nodes are source-scoped before data is divided. Every normalized RDF fact has its existing full 256-bit FactID collision fingerprint, and logical partition ownership is:

```text
partition = first_u64_be(fact_hash) mod logical_partition_count
```

`logical_partition_count` is part of the immutable source plan. Kubernetes `parallelism` is not. For example, 256 logical partitions can run through 1, 16, or 96 simultaneous pods without changing any partition ID or output bytes.

Each projection completion validates exactly one planned N-Quads shard and writes a strictly sorted canonical fact run and term run. Each reducer owns only partitions where:

```text
partition_index mod reducer_count = reducer_index
```

Reducers download only their assigned successful runs and use bounded k-way external merge. Finalization rejects missing, duplicate, overlapping, corrupt, unsorted, or unplanned input. It commits:

- one canonical sorted N-Quads source;
- one deterministic dense dictionary whose IDs are sorted-line ordinals;
- a topology-independent semantic content hash; and
- a checksum-bound distributed root manifest.

Only then may the reference worker replace the original source with the canonical source and run the existing Parquet, locator, HermiT, exact-query, snapshot, and publication pipeline. The compiler distinguishes the canonical file checksum from `sourceIdentitySha256`, so physical verification uses the reducer output hash while snapshot provenance and blank-node identity retain the original uploaded TriG hash. The final worker does not download the large original TriG again.

The restricted runtime database role now also needs `SELECT` and `INSERT` on `distributed_plan`, `distributed_work`, and `distributed_root`, plus `UPDATE` on `distributed_work`. It still must not own tables, bypass RLS, migrate schemas, delete rows, truncate tables, or update immutable plans/roots. The migration identity remains separate.

## Durable recovery contract

The planner uploads every source shard, uploads `source-plan.json` last, and registers the complete plan and all completion indexes in one catalog transaction. Projection and reducer workers:

1. resolve their Kubernetes completion index through PostgreSQL;
2. read only exact object keys and checksums;
3. upload data runs before the run manifest;
4. upload the manifest last; and
5. commit success with a catalog compare-and-swap.

Retries are idempotent only when the same key, checksum, and manifest are observed. The operator never uses object listing as a source of truth. The last projection advances `PARTITIONED → PROJECTED`; the last reducer advances through the existing identity/spine/index audit states to `INDEXED`; the finalizer adds the immutable root without changing that state; and the existing reasoner worker advances `INDEXED → REASONED → CERTIFIED`.

## Local deterministic exercise

With the pinned Rust toolchain installed, run:

```bash
scripts/run_distributed_reference_slice.sh
```

The script creates a one-partition/one-reducer baseline and an eight-partition/three-reducer build from the checked-in cross-domain TriG corpus, then calls `compare-builds`. The canonical source bytes, dictionary bytes, fact count, term count, and semantic content hash must match exactly.

This local exercise intentionally tests the pure deterministic data path. `scripts/qualify_phase15.sh` is the external integration gate for PostgreSQL, object storage, Kubernetes, Kueue, RKE2 autoscaling, HermiT, restart, retry, and node-loss evidence.

## Kubernetes and HPC behavior

- Planning and projection select `ngkg.io/workload=semantic-projection`.
- Reduction and finalization select `ngkg.io/workload=index-build`.
- HermiT selects `ngkg.io/workload=reasoning`.
- Projection and reducer stages use Kubernetes Indexed Jobs. `completions` is the durable logical count; `parallelism` is an operator ceiling and may be lower.
- Indexed stages use a per-index retry budget and permit zero failed indexes, so transient failures are isolated without allowing a partially successful barrier.
- Jobs request whole CPUs with equal requests and limits and use bounded `emptyDir` scratch.
- OpenMP, OpenBLAS, and MKL are set to one thread per pod in this implementation because each projection/reducer completion requests one exclusive core and parallelism comes from many completions across nodes. This prevents nested oversubscription and avoids pretending a single-threaded sparse merge uses a multi-core request. A later measured native-kernel profile may assign a fixed subset of a larger pod cpuset to BLAS/OpenMP.
- Kueue controls admission. Responsibility-specific Rancher/RKE2 pools provide capacity. Cluster Autoscaler changes machine-pool quantity; it does not change logical work ownership.
- Worker service accounts are separate from operator identities. Token automount remains off unless provider workload identity requires it.

## Intentional boundary

Phase 15 does **not** yet distribute every operation inside the final reference compiler. In particular:

- the initial complete TriG parse is one syntax-correctness boundary and currently retains normalized facts in memory;
- the final Phase 13 Parquet/spine/locator build is repeated from the canonical N-Quads source by one reference worker;
- the HermiT adapter still executes as one reasoner Job; and
- only the finite, named-entity materialization and exact pre-certified SPARQL query hashes provided by Phase 13 are certified.

Those limits are deliberate. This phase establishes deterministic work ownership, exact barriers, retry safety, and topology equivalence before sharding Parquet/index production or OWL modules. It does not claim arbitrary OWL 2 DL SPARQL or linearly scalable parsing/reasoning.

## Acceptance gate

Phase 15 passes only when all of the following are recorded against digest-built images and the same immutable input bundle:

1. migration 3 applies once, readiness requires version 3, and RLS blocks cross-tenant plan/work/root reads;
2. one-partition/one-reducer and N-partition/N-reducer builds have byte-identical canonical sources and dictionaries and equal semantic hashes;
3. semantic snapshot artifacts (spine, payload, locator, graph capability, reasoner inputs/closure, and certified result multisets) are identical across the topology matrix in `test-corpus/distributed/build-equivalence-v1.json`; operation-bound audit/compile-request files are compared after removing approved runtime paths and operation identifiers rather than falsely required to be byte-identical;
4. projection/reducer completion order, parallelism, retry, duplicate pod, operator restart, node loss, and autoscaling do not change the result or produce two successful outputs for one work index;
5. omitted, duplicate, corrupt, overlapping, or unsorted shards/runs fail closed;
6. bucket listing permission is denied throughout the build;
7. every stage reads and writes exact checksum-addressed objects, and manifests are published last;
8. each responsibility lands only on its intended RKE2 node pool, Kueue admits the complete shape, and scale-up/down preserves catalog truth;
9. CPU sets do not oversubscribe Rust/OpenMP/BLAS threads and scratch stays within its declared limit; and
10. the final HermiT report, query certification, GUID hydration, and snapshot publication remain identical to the Phase 14 reference path.

The archive verification report must mark this gate `blocked` when the required Rust, PostgreSQL, S3, Helm, Kubernetes, Kueue, RKE2, Java, Maven, or reasoner environment is unavailable. Static inspection is not a substitute for the distributed equivalence and fault matrix.
