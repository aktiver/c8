# Phase 33 — out-of-core coordinator fragment ingress

Phase 33 builds cumulatively on Phase 32 and removes the complete encoded fragment-response buffers from the query coordinator. Worker responses now flow directly from HTTP into checksum-verified, process-budgeted local-NVMe files. The coordinator subsequently opens each immutable file, verifies its length and SHA-256, and decodes one Arrow record batch at a time.

## Implemented data path

1. A query pod creates a marker-owned `fragment-response-spool` on its node-local spill volume and removes only correctly named stale response files at startup.
2. Every successful fragment HTTP response is consumed as chunks. Content length, actual bytes, per-response size, process-wide active bytes and Arrow EOS are enforced while the file is written.
3. The file is flushed and `fsync`ed before it becomes a lease. Cancellation, timeout, truncated streams, capacity exhaustion and I/O failure drop the lease, remove the partial file and release its reservation.
4. Before decoding, the coordinator rejects symlinks, non-files, changed lengths and SHA-256 changes. The verified file is rewound and passed to `FragmentBindingStream`.
5. `FragmentBindingStream` validates schema and certificate metadata once, then retains only the current Arrow batch while decoding RDF terms and enforcing the row ceiling.
6. Fragment identity, head, exact row count, worker diversity and offline fragment multiset certificate remain mandatory. The final distributed answer is still compared with its offline certified multiset.
7. Public execution evidence reports `fragmentIngressMode=streamed_nvme_spool_v1` and the exact admitted encoded byte count. Prometheus exposes the currently reserved spool bytes.

The ingress path is:

`worker Arrow chunk → bounded NVMe spool → SHA-256 verification → bounded Arrow batch → certified rows`

It is no longer:

`worker response → complete byte Vec → concurrent full decode`.

## Kubernetes and HPC intent

The query StatefulSet receives a dedicated `fragment-response-spool` `emptyDir`, configured independently from immutable cache, primary shuffle spill and query-result cache. Helm cross-field validation requires the process budget to fit the volume and cover the complete admitted distributed exchange. The query pod's Guaranteed-QoS ephemeral-storage request covers all four volumes.

Independent fragment transfers remain concurrent across fragment workers, query pods and RKE2 nodes. Each transfer performs sequential asynchronous writes; later decoding is a sequential NVMe read with one Arrow batch in memory. Mmap is deliberately not used for disposable sequential streams. Existing immutable locator and cache indexes continue to use mmap, Parquet remains the payload/hydration store, and OpenMP/OpenBLAS/MKL remain one-threaded because this sparse RDF decoding path is not a dense matrix kernel. Kubernetes and Rust task concurrency provide the useful node/core parallelism.

## Acceptance criteria

1. Pinned Rust format, build, Clippy and all workspace tests pass with `Cargo.lock` present.
2. Incremental fragment decoding exactly matches the owned decoder and fails at the configured row limit.
3. Spool tests prove exact row recovery, checksum rejection, cleanup and active-byte release.
4. Production source inspection proves the coordinator calls `FragmentResponseSpool::receive` and does not call `read_bounded_response` for initial fragment execution responses.
5. A fresh certified cross-domain workload returns the independent expected bag, reports `streamed_nvme_spool_v1`, reports positive ingress bytes and drains the active spool gauge to zero.
6. Truncation, append, checksum change, disk full, cancellation, timeout, worker loss and query-pod loss never produce a partial accepted answer.
7. Helm schema/lint/render, server-side dry run, Guaranteed QoS, dedicated volume, anti-affinity, node selectors and RKE2 sustained 79/80-percent scaling tests pass.

## Honest boundary

Phase 33 externalizes encoded fragment ingress and limits decode working memory to one Arrow batch, but the decoded fragment row vectors, assembled shuffle-stage rows, final projection, JSON response and query-cache entry still have explicit in-memory row/byte ceilings. Direct fragment-to-primary-partition routing and spillable final aggregation remain future boundaries. Phase 33 does not expand OWL 2 DL/SPARQL language coverage, use BLAS for unsuitable sparse work or claim a universal performance improvement.
