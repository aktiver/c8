# Phase 32 — coordinator-side streaming shuffle transmission

Phase 32 builds cumulatively on Phase 31 and removes the coordinator request-memory boundary recorded there. A query coordinator no longer reconstructs a complete left/right partition pair and no longer serializes a complete Arrow request into `BoundedBuffer` before contacting a fragment worker.

## Implemented data path

1. Phase 25’s coordinator spill remains the immutable source of each primary partition. Files retain stage identity, owner partition, exact row count, byte length and SHA-256.
2. `SpillPartitionReader` opens one partition file, verifies its real-file type, length and identity header, then yields one JSON binding at a time. It validates record framing, owner partition and the complete file checksum before successful iterator termination.
3. `write_shuffle_join_stream_iter` accepts incremental left/right iterators and retains at most `fragmentArrowBatchRows` bindings. It verifies declared counts, RDF term structure and primary ownership while producing Arrow IPC v2 batches.
4. `ArrowRequestWriter` applies the per-request byte ceiling and cumulative stage exchange ceiling as bytes are encoded. It computes SHA-256 online and publishes only bounded `fragmentArrowHttpChunkBytes` chunks through a bounded Tokio channel.
5. Reqwest sends that channel as a chunked request body. Backpressure reaches the blocking encoder: a slow worker permits only `fragmentArrowChannelCapacity × fragmentArrowHttpChunkBytes` queued bytes.
6. Producer completion returns the exact byte count and SHA-256. The worker independently reports what it durably spooled, and the coordinator rejects any mismatch before accepting results.
7. Worker cache, incremental worker decode, direct Grace partitioning, output-owner validation and the final offline OWL/SPARQL multiset certificate remain mandatory.

The resulting coordinator hot path is:

`verified NVMe spill record → bounded Arrow batch → bounded HTTP chunk → worker spool`

It is not:

`complete spill partition → complete relation vectors → complete Arrow Vec → HTTP body`.

## Kubernetes and HPC intent

Independent owner partitions are streamed concurrently across the configured Rust tasks, query pods, fragment pods and RKE2 nodes. Each stream has two explicit RAM bounds: one Arrow record batch and one bounded channel. Existing query `shuffle-spill` storage should be node-local NVMe; it is read sequentially and therefore is intentionally not mmaped. Mmap remains appropriate for immutable random-access locator/cache indexes, and Parquet remains the columnar payload store.

OpenMP, OpenBLAS and MKL remain fixed to one thread for the sparse equality/serialization path. Dense BLAS kernels would add nested oversubscription without accelerating RDF hash ownership, JSON record validation or Arrow IPC encoding. Kubernetes provides node/core parallelism through concurrent partitions and anti-affine replicas. Query and fragment HPA targets remain no greater than 80 percent so RKE2 Cluster Autoscaler can add responsibility-specific nodes before sustained saturation.

## Acceptance criteria

1. Pinned Rust format, build, Clippy and all workspace tests pass with `Cargo.lock` present.
2. Incremental and owned Arrow writers decode to the same exact partition bags; source failure or count mismatch never writes a valid EOS.
3. Incremental spill replay rejects changed headers, wrong owners, truncation, appended bytes, invalid JSON and checksum changes.
4. Static and runtime inspection proves the production coordinator path contains neither `read_pair` nor a complete request `BoundedBuffer`.
5. A fresh enterprise skew qualification returns the independent certified bag, reports `streamed_from_spill_v1`, and shows coordinator streamed-request and byte counters increasing.
6. Coordinator bytes equal worker-spooled bytes across every successful partition; any evidence mismatch fails closed.
7. Slow consumers, client cancellation, worker 429/503, disk-full, pod loss and node loss leave bounded memory/storage and no partial accepted result.
8. Helm schema/lint/render, server-side dry run, Guaranteed QoS, anti-affinity, node selectors and RKE2 79/80-percent scale tests pass.

## Honest boundary

Phase 32 bounds coordinator partition replay, Arrow encoding and HTTP request transmission. Initial fragment responses and final stage aggregation are still subject to existing explicit row/byte ceilings and remain memory-resident. Automatic replay after ambiguous network failure is not introduced because exact retry semantics require a separately reviewed idempotency and deterministic-byte contract. Phase 32 does not expand OWL 2 DL/SPARQL language coverage or claim a universal speedup.
