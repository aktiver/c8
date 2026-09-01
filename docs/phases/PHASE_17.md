# Phase 17 — Durable distributed semantic-artifact service path

Phase 17 connects the Phase 16 columnar artifact kernel to the real PostgreSQL catalog, immutable object store, Kubernetes operator, REST status model, reference certifier, and Helm/RKE2 capacity topology. It does not introduce a second ingestion workflow: it extends the Phase 15 compilation operation with a durable artifact sub-DAG after the canonical source and dictionary barrier.

## Executed DAG

```text
syntax-safe source plan
  → Indexed semantic projections
  → deterministic reducers
  → canonical source + global dictionary
  → immutable artifact plan
  → Indexed artifact partitions
       ├── semantic-spine.parquet
       ├── payload.parquet
       ├── queryable.nq
       ├── reasoner-core.nt
       └── locator-run.tsv
  → exact completion barrier
  → bounded global locator merge
  → distributed-artifact-root.json
  → reference HermiT/query certification gate
  → atomic snapshot certification/publication
```

The artifact plan is uploaded before its catalog row and completion indexes are registered. Each worker downloads only the exact plan, source shard, dictionary, bundle, and work index assigned by the catalog. Data objects are uploaded first, the partition manifest is uploaded last, and the catalog compare-and-swap is the only completion signal. The finalizer reads successful catalog outputs, never lists a bucket, downloads only partition manifests and locator runs, and publishes the global locator before its root manifest.

## Durable truth and restart behavior

Migration 4 adds an `ARTIFACT` work kind, immutable artifact plan, immutable artifact root, forced tenant RLS, count invariants, and versioned object-key/checksum roots. The Kubernetes controller is level-based. After restart it observes catalog truth and selects exactly one missing barrier:

1. distributed root finalization;
2. artifact-plan registration;
3. pending Indexed artifact completions;
4. global artifact finalization; or
5. reference certification.

Kubernetes Job status is not accepted as durable success. A completed Job without the corresponding catalog commit fails the operation. Retried completions must commit the same manifest key and checksum or fail as an immutable conflict.

## Certification handoff

The reasoner Job is not admitted until the catalog contains the exact distributed source root and distributed artifact root. The reference worker verifies both argument pairs against catalog truth, downloads both manifests and the global locator by checksum, proves that every root partition matches a successful catalog completion index, and compares artifact fact/semantic/payload/locator counts with the independently rebuilt reference snapshot.

The published snapshot still comes from the established reference compiler and HermiT boundary. Therefore a damaged or inconsistent distributed artifact tree cannot become query-serving truth in Phase 17. Count equivalence is qualification evidence, not a claim of byte-level Parquet equivalence. Serving directly from the sharded artifact root requires the later logical-row, certified-query, and hydration equivalence gate.

## HPC and RKE2 topology

Artifact completions use a dedicated `semantic-artifact-build` responsibility pool. They request one whole CPU per pod and obtain throughput from many Indexed completions across nodes. Arrow/Parquet supply vectorized in-process encoding; OpenMP and BLAS thread counts remain one because sparse RDF encoding is not a dense linear-algebra kernel. This avoids nested oversubscription while Kubernetes, Kueue, and the external Rancher Cluster Autoscaler provide inter-pod and inter-node parallelism.

The chart separates immutable logical work from instantaneous capacity:

```yaml
distributedOperator:
  artifactRowGroupRows: '1048576'
  stages:
    artifact_plan: {maxParallelism: 1}
    artifact: {maxParallelism: 96}
    artifact_finalize: {maxParallelism: 1}
```

`partitionCount` and `rowGroupRows` are part of immutable output identity. `maxParallelism` can change without changing result bytes. RKE2 itself does not provision machines; the chart emits schedulable demand and Kueue flavors, while a separately installed Cluster Autoscaler resizes the labelled and tainted Rancher machine pool.

## REST visibility

`GET /v1/jobs/{operationId}` now reports the source/reducer build, artifact-plan completion counts, and optional global artifact root. This lets automation distinguish pending partitions from finalization and certification without inspecting Kubernetes pods or object storage.

## Acceptance gate

Phase 17 requires all of the following in a supported environment:

1. migration 4 applies on PostgreSQL and forced-RLS tenant isolation passes;
2. plan registration is immutable and creates one dense artifact completion index per logical partition;
3. every artifact worker verifies exact plan, work, source-shard, dictionary, bundle, policy, snapshot, and resource-ceiling identity;
4. no worker or finalizer uses bucket listing;
5. data-before-manifest and locator-before-root publication order survives pod and node loss;
6. partial, duplicate, corrupt, mismatched, or completed-without-CAS work fails closed;
7. the operator resumes at every catalog barrier after restart and never admits the reasoner early;
8. artifact root partitions match all successful catalog completion indexes and reference counts exactly;
9. REST/OpenAPI status, Helm values/schema, Kueue flavor, node selector, taint, and RKE2 scale-from-zero chain agree;
10. one-node and N-node executions preserve partition manifests, global locator, semantic root, published snapshot, certified query answers, and hydrated rows; and
11. Cargo formatting, linting, tests, PostgreSQL/S3 integration, Helm rendering, RKE2 autoscaling, HermiT, and fault-injection gates all pass with recorded evidence.

## Intentional boundary

Phase 17 wires real durable artifact production and a fail-closed qualification handoff. It does not yet make the online query or hydration services consume sharded Parquet directly, distribute HermiT itself, certify arbitrary SPARQL under all OWL 2 DL constructs, or prove the 20–50× performance target. Those claims remain blocked until the exact serving path consumes this root and passes semantic, proof, authorization, provenance, hydration, and benchmark equality.
