# Phase 35 — out-of-core multi-stage shuffle results

Phase 35 builds cumulatively on Phase 34 and removes stage-wide coordinator assembly between certified distributed joins. Shuffle-worker Arrow responses now stream into process-budgeted, checksum-verified local-NVMe leases. Each partition is validated independently and the ordered leases become the left relation for the next join stage through a lazy replay iterator.

## Implemented data path

1. The coordinator streams every successful shuffle-worker response through `FragmentResponseSpool::receive`; no complete response-byte buffer is created.
2. Admission enforces the per-response byte ceiling, cumulative query exchange ceiling and process-wide response-spool ceiling. Arrow EOS, file type, length, SHA-256, flush and `fsync` remain mandatory.
3. One partition at a time is decoded for exact response-multiset and stable partition-ownership verification. Its temporary owned verification rows are dropped before the validated lease is retained.
4. Stage row counts are summed with checked arithmetic and cannot exceed the certified intermediate-row ceiling.
5. Partitions are ordered by stable partition ID and stored as `ValidatedFragmentSpool` values rather than merged into a stage-wide row vector.
6. `ValidatedFragmentSpoolSequence` opens one lease at a time, verifies immutable metadata and exact replay row count, releases it at EOF, and feeds rows directly into the next primary partition writer.
7. Missing, duplicate, foreign, corrupt, oversized or incomplete partitions fail the query. Dropping a lease removes its file and releases the process reservation.
8. Only the final bounded stage result is materialized for SPARQL projection, entity extraction, exact offline multiset comparison, JSON serialization and the complete-response cache.

The multi-stage path is:

`worker Arrow chunks → verified response leases → per-partition exact validation → ordered lazy spool sequence → next primary partition stage`

It is no longer:

`all worker response bytes → all decoded partition vectors → one assembled intermediate Vec → next stage`.

## Public and operational evidence

For the partitioned fast path:

- `shuffleResultIngressMode=streamed_nvme_spool_v1`;
- `shuffleResultIngressBytes` is the exact sum of admitted encoded worker-result bytes across all stages;
- `intermediateResultMode=partition_spool_sequence_v1`;
- `assembledIntermediateOwnedRows=0`.

Prometheus counters expose admitted shuffle-result responses and bytes. Existing active response-spool accounting covers initial fragments and stage outputs and must return to zero after success, rejection, cancellation or failure. Query-cache hits revalidate these mode fields before reusing a complete response.

## Kubernetes and HPC intent

Phase 35 adds no node group or volume. It reuses the query role's node-local `fragment-response-spool` and `shuffle-spill` volumes. Helm validation requires `maxShuffleExchangeBytes` to fit the process-wide response-spool budget, while Guaranteed-QoS query ephemeral storage already covers both volumes.

Independent network exchanges remain concurrent across stable partitions, fragment pods and anti-affine RKE2 nodes. Exact partition verification is intentionally sequential at the coordinator so only one decoded partition bag is owned at once; the next stage lazily replays one file at a time while Rust partition workers execute across cores and nodes. Buffered NVMe I/O is appropriate for disposable sequential streams. Read-only mmap remains for immutable random-access locator and result-cache indexes; Parquet remains the columnar payload store. OpenMP/OpenBLAS/MKL remain one-threaded because sparse RDF decode, hashing and checksum work are not dense BLAS kernels. HPA CPU and memory targets remain capped at 80 percent.

## Acceptance criteria

1. Pinned Rust format, build, Clippy and all workspace tests pass with the checked-in lock file.
2. A sequence of multiple validated spool partitions replays the exact RDF bag in deterministic partition order and releases every byte reservation.
3. Shuffle-result validation proves dataset, snapshot, query, stage/partition identity, head, exact multiset, stable hash ownership and row ceilings.
4. Production source inspection proves shuffle responses call the process-budgeted spool and do not call `read_bounded_response` or assemble `stage_left`.
5. A fresh certified query with at least three fragments returns the independent expected bag, reports both Phase 35 modes, reports zero assembled intermediate rows and positive exact ingress bytes.
6. The direct response/byte counters cover every stage partition and all active request, response and Grace-spill gauges drain to zero.
7. Truncation, append, checksum change, foreign partition, duplicate response, disk full, cancellation, timeout, worker loss and query-pod loss never expose a partial answer.
8. Maximum-concurrency RSS and combined source/destination ephemeral storage plateau beneath approved pod limits.
9. Helm schema/lint/render, server-side dry run, node-local-NVMe inspection, required anti-affinity and sustained RKE2 79/80-percent scale tests pass.

## Honest boundary

Phase 35 prevents stage-wide intermediate assembly but temporarily owns one bounded partition during exact canonical multiset verification. The final stage result, final projection, REST JSON and complete-result cache remain bounded in memory. A future phase can implement external canonical multiset verification and a streamed final response, but this phase does not claim those capabilities, arbitrary OWL 2 DL/SPARQL completeness, unsuitable BLAS acceleration, production qualification without the required cluster, or a universal 20–50× speedup.
