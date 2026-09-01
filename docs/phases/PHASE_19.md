# Phase 19 — Immutable distributed serving root and hydration equivalence gate

Phase 19 moves the Phase 18 locator/hydration kernel into the durable compilation DAG. It creates a checksum-addressed serving bill of materials, proves its results against the existing reference compiler for every certified query, and prevents snapshot certification or automatic publication unless that evidence matches the exact reference snapshot manifest.

## Executed DAG

```text
Phase 17 artifact root
  → serving-root Job on index-build capacity
      → verify artifact root, completion indexes, partition manifests and payload references
      → compile sorted TSV locator into snapshot-bound fixed-width binary
      → upload locator.bin
      → upload serving-root.json last
      → immutable catalog compare-and-swap
  → reference/reasoner Job on reasoning capacity
      → compile the independent monolithic reference snapshot
      → execute every certified query
      → derive every bound entity GUID
      → mmap binary locator and fetch only referenced Parquet partitions/row groups
      → compare canonical reference and sharded hydration multisets
      → upload equivalence report and snapshot manifest
      → commit serving certification
      → commit reference certification and optional publication
```

`serving-root.json` binds the dataset and snapshot IDs, Phase 17 artifact root, dense dictionary, source locator, binary locator, semantic content root, row-group size, record count, and every exact payload object key/checksum/byte count. Consumers never list object storage. The catalog rows are tenant-isolated, immutable, and idempotent under exact retries.

## Correctness boundary

The sharded path does not decide which entities satisfy SPARQL or OWL semantics. The independently certified reference query produces the complete bound IRI set. NGKG converts those IRIs with the snapshot's identity namespace, uses the mmap locator only for physical routing, and compares the resulting sharded payload multiset with reference hydration.

The comparison includes bound entities with no payload. This matters because deriving candidates only from rows that already hydrated would fail to detect an erroneous extra sharded row. Each query report records exact query hash, row counts, and a length-delimited SHA-256 of sorted canonical rows. The immutable catalog certificate also repeats the exact serving-root and binary-locator hashes, preventing report replay across physical roots.

Publication fails closed when a distributed serving root exists but its serving certification is absent, or when the certificate names a different reference manifest key/hash. A checksum-valid but semantically different root cannot become published truth.

This is a serving-root **admission** gate, not yet the horizontally scaled online SPARQL service. The Phase 20 data-plane work must load only a catalog-certified serving root and preserve this oracle equivalence.

## HPC and Kubernetes intent

Artifact production remains chunked into many Indexed Jobs across `semantic-artifact-build` nodes. The serving-root compiler is intentionally a one-core global sorted barrier because it converts one canonical locator stream and publishes one root; assigning unimplemented threads would waste capacity. The large reference/reasoner Job uses its measured JVM shape, then runs sharded hydration across `hydrationWorkerThreads` bounded Rust lanes. Independent Parquet partitions and row groups are the CPU work units.

OpenMP, OpenBLAS and MKL are fixed at one thread for sparse dictionary, locator and Parquet kernels. BLAS is reserved for measured dense kernels and is not used as a graph-traversal substitute. Requests equal limits, enabling static CPU-manager placement and avoiding nested oversubscription. Scratch is disposable; durable progress lives in PostgreSQL and immutable object storage.

Online query/hydration HPAs retain the 80% CPU and memory ceiling. Batch stages scale differently: Kueue admits a bounded number of complete pods and Rancher Cluster Autoscaler adds matching RKE2 nodes for unschedulable demand. Batch work must not wait for a partially used node to reach an HPA percentage; its resource requests already reserve the full measured pod shape. The 20% operational headroom remains outside that workload envelope for page cache, CNI, recovery and system services.

## REST and operational evidence

`GET /v1/jobs/{operationId}` now exposes nullable `distributedServingRoot` and `distributedServingCertification` objects. A successful distributed operation must show both; their checksums, partition/row-group counts, report key, certified query count and exact reference manifest binding are available through the authenticated Swagger contract.

## Acceptance gate

1. The serving root is deterministic for identical artifact inputs and contains every partition exactly once in dense index order.
2. Locator binary checksum, source-locator checksum, snapshot ID and record count match the serving root and catalog.
3. Every payload reference matches a checksum-verified Phase 17 partition manifest; no bucket listing or broad Parquet scan occurs.
4. Every certified query executes on the independent reference snapshot and supplies all bound entity IRIs, including zero-payload entities.
5. Reference and sharded canonical hydration multisets, counts and hashes are identical for every query.
6. Corrupt, missing, version-mismatched, over-budget or extra rows fail the Job and prevent certification.
7. Serving root and serving certificate rows are immutable, forced-RLS protected and exact-retry idempotent.
8. Reference certification and publication cannot commit without a matching serving certificate when a serving root exists.
9. Operator restart or node loss recreates only the missing stage and cannot overwrite a different immutable object or catalog root.
10. One-thread and N-thread hydration, one-node and N-node artifact production, and different Indexed Job parallelism produce equivalent certified rows.
11. RKE2 placement, Kueue quota and Cluster Autoscaler grow only the responsibility pool requested by each stage.
12. CPU/memory requests equal limits; sparse kernels keep OpenMP/BLAS at one; Rust hydration lanes fit the pod cpuset; online HPA targets remain at or below 80%.
13. Cargo formatting, Clippy, all Rust tests, PostgreSQL/S3 integration, Helm render, RKE2 recovery and corruption qualification pass in the pinned release environment.

## Intentional boundary

Phase 19 proves that a durable distributed physical representation reconstructs the same payload context for the repository's certified OWL-aware queries. It does not claim arbitrary SPARQL support, a complete finite closure for every OWL 2 DL consequence, universal 20–50× speedup, or an already deployed online distributed query coordinator. Those claims remain blocked until later phases implement and benchmark the online read path against this gate.
