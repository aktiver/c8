# Phase 34 — direct fragment-spool primary partitioning

Phase 34 builds cumulatively on Phase 33 and removes complete decoded fragment vectors from the certified partitioned-shuffle fast path. Worker Arrow responses remain checksum-bound local-NVMe leases. The coordinator validates each stream incrementally, records only certificate metadata and a compact binding summary, and later replays each row directly into its stable primary hash partition.

## Implemented data path

1. `ValidatedFragmentSpool` opens the immutable response file, validates every Arrow batch and RDF term, counts exact rows, and computes the set of variables bound in every solution.
2. The coordinator compares dataset, snapshot, exact query, fragment identity, head, row count, worker identity and offline fragment multiset identity with the published distributed plan.
3. `shuffle_plan_is_eligible` uses the compact always-bound summaries. It cannot select primary hash ownership when a required key is unbound in any participating fragment.
4. Eligible plans reopen the checksum-verified source leases and pass incremental iterators to `ShuffleSpillStage::create_iter`. Each row is decoded once and immediately appended to its stable hash-owned partition file.
5. Source-iterator, row, ownership, byte, open-file or I/O failure removes the incomplete stage directory. Source leases release their process-wide byte reservation when the stage finishes or fails.
6. Existing Phase 32 transmission streams each partition file to the selected fragment worker; Phase 31/30 worker paths retain bounded request and Grace-join memory.
7. Plans that cannot prove fully bound primary keys use `bounded_owned_fallback_v1`. That fallback materializes one certified fragment at a time and joins under the existing intermediate-row ceiling; it is explicit in public evidence.
8. The final SPARQL bag is still compared with the offline certified multiset before any answer is visible.

The fast path is:

`worker chunks → certified response spool → incremental validation summary → incremental replay → primary partition files → cross-node joins → final certificate`

It is not:

`all fragment streams → all decoded fragment Vecs → primary partition files`.

## Public and operational evidence

`Execution.fragmentMaterializationMode` is one of:

- `none` for a local certified route;
- `direct_spool_to_primary_partition_v1` for the Phase 34 fast path;
- `bounded_owned_fallback_v1` when SPARQL unbound-key behavior prevents stable primary ownership.

`fragmentOwnedRows` is zero on the direct path and reports the number of source rows decoded into owned vectors on the fallback. Prometheus counters report the fragments and rows replayed directly into primary partitions. Existing active-spool gauges must return to zero after completion, cancellation and failure.

## Kubernetes and HPC intent

Phase 34 adds no responsibility pool. Query pods use the existing `sparql-query-processing` nodes and both local-NVMe volumes already accounted by the Helm chart. Fragment joins continue across independent hash partitions, Rust tasks, anti-affine fragment pods and RKE2 nodes. HPA CPU and memory targets remain at or below 80 percent so a saturated pod causes a new responsibility-specific replica and, when necessary, a new Rancher node.

Sequential disposable response and partition files use buffered I/O and kernel readahead. Independent fragment validation passes run across the bounded fragment-exchange blocking lanes, while each stream preserves sequential access. Read-only mmap remains for immutable random-access locator/cache indexes; Parquet remains the enterprise payload store. OpenMP, OpenBLAS and MKL remain one-threaded because RDF decode and sparse hash ownership do not call dense matrix kernels. This avoids nested oversubscription while Rust and Kubernetes use the assigned cores and nodes.

## Acceptance criteria

1. Pinned Rust format, build, Clippy and all workspace tests pass with the checked-in lock file.
2. Incremental and owned primary partitioning produce exactly the same left and right RDF bags for every partition count and key distribution in the test matrix.
3. Empty, duplicate, unbound, malformed, oversized and source-error streams fail closed; incomplete stage directories and response leases are removed.
4. Production source inspection proves eligible initial fragments reach `ShuffleSpillStage::create_iter` without `FragmentBindingStream::into_batch` or a complete fragment `Vec`.
5. A fresh certified multi-domain shuffle returns the independent expected bag, reports `direct_spool_to_primary_partition_v1`, reports `fragmentOwnedRows=0`, increments direct fragment/row counters and drains all active-spool gauges.
6. A certified plan containing an unbound join key reports the bounded fallback, returns the exact bag, respects the row ceiling and never claims the direct mode.
7. Helm schema/lint/render, server-side dry run, Guaranteed QoS, node-local-NVMe inspection, required anti-affinity and sustained RKE2 79/80-percent scale tests pass.
8. Maximum-concurrency RSS and ephemeral storage plateau beneath the approved pod limits during slow-worker, cancellation, node-loss, disk-full and restart tests.

## Honest boundary

Phase 34 removes owned initial-fragment rows only from eligible partitioned joins. A completed shuffle stage is still assembled into a bounded coordinator vector before the next stage; final projection, JSON response and complete-result cache retain explicit memory/byte ceilings. Spillable stage-result aggregation is the next boundary. This phase does not expand supported OWL 2 DL or SPARQL semantics, use BLAS for unsuitable sparse work, prove deployment on an unavailable RKE2 cluster, or claim a universal 20–50× speedup.
