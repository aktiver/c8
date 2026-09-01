# Phase 31 — streamed, bounded worker shuffle ingestion

Phase 31 builds cumulatively on Phase 30 and removes the worker request-memory boundary explicitly recorded there. A fragment worker no longer asks Axum to materialize a shuffle body as one `Bytes` value and no longer decodes a large left and right relation into complete vectors before the Grace operator can spill them.

## Implemented data path

1. Authentication, Arrow media type, query identity and declared length are checked before accepting data.
2. The HTTP body is copied chunk-by-chunk into an exclusively created file under a marker-owned local-NVMe `emptyDir`. Per-request and process byte ceilings are enforced while bytes arrive, including requests without `Content-Length`.
3. The worker computes the exact body SHA-256, retains only the final eight bytes needed to verify the Arrow EOS marker, flushes and `sync_all`s the file, and rejects truncated streams.
4. Shuffle IPC advances to `ngkg.shuffle-join.v2`. Its schema declares exact left and right row counts. The incremental decoder validates schema, RDF terms, relation order, primary partition ownership and observed counts before yielding each row.
5. A validation pass computes complete relation-stream digests without retaining either relation. Cache and Grace identity fields are explicitly named `left_input_sha256` and `right_input_sha256`, and their domains advance to v2 so they cannot be confused with or collide with the earlier canonical-multiset key contract.
6. Cache hits are still logically revalidated. On a miss, a row-count-bounded small partition uses the existing in-memory fast path. A larger partition reopens the spool and feeds each decoded row directly into `GraceJoinEngine::join_stream`; it never creates complete left/right vectors.
7. The Grace stage guard deletes partial buckets and releases byte accounting if Arrow decoding, disk I/O, limits or execution fail. Existing bucket headers, record framing and SHA-256 trailers remain mandatory.
8. The worker reports input mode, byte count and SHA-256. The coordinator compares these to the exact request it sent before accepting join output. The final result still requires the offline OWL/SPARQL multiset certificate.

## Kubernetes/HPC intent

The fragment StatefulSet receives a dedicated `streaming-request-spool` `emptyDir`, intended for node-local NVMe. `maxStreamingRequestSpoolBytes` is a process-wide concurrency budget and must fit `streamingRequestSpoolSizeLimit`; one `maxShuffleRequestBytes` request must fit that process budget. Fragment ephemeral-storage requests cover immutable cache, shuffle cache, Grace spill and request spool together. Existing Guaranteed QoS, anti-affinity, responsibility node selector, HPA targets at or below 80 percent and RKE2 Cluster Autoscaler behavior are preserved.

Parallelism remains across admitted requests, primary partitions, fragment pods, cores and RKE2 nodes. Within one large sparse join, sequential Arrow decode and bucket writes prevent nested oversubscription and make memory use predictable. OpenMP/OpenBLAS/MKL remain one thread for sparse RDF equality hashing; BLAS is not falsely inserted into this kernel. Mmap remains used for stable fixed-width locator/cache artifacts, Parquet for payload storage, and buffered sequential I/O for disposable request and Grace files.

## Acceptance criteria

1. Pinned format, Clippy and workspace tests pass with a committed `Cargo.lock`.
2. The incremental Arrow test proves declared counts and exact row yield; truncated, malformed, wrong-owner and relation-order inputs fail closed.
3. The streamed Grace test matches an independent SPARQL bag join and proves a decoder failure cleans every stage and returns active spill bytes to zero.
4. HTTP qualification sends a chunked body larger than the reviewed RAM threshold, observes bounded RSS, positive spool activity, `streamed_spool_v1`, exact byte/SHA evidence, and exact certified output.
5. Missing EOS, false `Content-Length`, body overrun, process-spool exhaustion, disk-full, cancellation and pod termination leave no accepted partial result; startup removes only valid crash debris under its marker-owned root.
6. Cache hit and miss paths use identical request identity and answer validation. A changed byte, snapshot, plan, partition or relation digest cannot reuse an entry.
7. Helm values validation, lint, server-side dry run and the approved RKE2 profile prove volume/resource arithmetic and 80-percent autoscaling behavior.
8. Sustained concurrent enterprise qualification demonstrates bounded RSS and ephemeral storage, fair admission, cleanup gauges returning to zero and exact offline-certified answers.

## Honest boundary

Phase 31 bounds the fragment worker's request body and decoded input relations. The query coordinator still reads one already bounded primary partition pair and serializes that request into memory before transmission, and final stage aggregation remains bounded in memory. Those are separate future out-of-core boundaries. Phase 31 does not add OWL 2 DL language coverage and makes no universal performance claim.
