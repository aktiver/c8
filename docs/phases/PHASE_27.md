# Phase 27 — bounded admission, streaming lifetime control and operational metrics

Phase 27 builds cumulatively on Phase 26 and closes an enterprise overload gap. Every expensive REST data operation must first obtain one of a bounded number of pending slots and then a role-specific execution permit before Axum extracts and buffers its request body. The permit remains owned until the complete response body is transmitted or dropped, including streamed Arrow IPC responses. Requests that cannot obtain a pending slot immediately or an execution lane within the operator-controlled deadline receive `429 Too Many Requests` and `Retry-After: 1`; they cannot create an unbounded internal queue.

## Production path

```text
HTTP request
  -> classify query / fragment / shuffle / locator / hydration
  -> authenticate before consuming pending or execution capacity
  -> reserve a bounded per-class pending slot or reject immediately
  -> wait at most admissionWaitMilliseconds
  -> acquire role lane
  -> fragment and shuffle also acquire the shared fragment-worker lane
  -> extract bounded request body
  -> execute the existing certified semantic path
  -> retain permit while JSON or Arrow body is consumed
  -> release on completion, client disconnect, error or cancellation
```

Fragment evaluation and shuffle joins have independent ceilings plus `maxFragmentWorkerInFlight`. This prevents their combined admitted load from exceeding the fragment pod envelope while allowing operators to reserve capacity for certified fragment work. Five `max*Pending` values bound semaphore waiters independently. The implementation uses Tokio semaphores and monotonic deadlines; a request without a pending slot is rejected before body extraction.

## Metrics

`GET /metrics` emits Prometheus text for:

- configured limits and current in-flight requests by operation class;
- configured pending limits and current pending waiters;
- admitted, rejected, completed and failed totals;
- cumulative admission wait and full response-lifetime service seconds;
- shuffle-cache hit, miss, invalid and error events;
- current fragment-local shuffle-cache entries and bytes.

Metrics contain role and operation labels only. Tenant IDs, principals, query hashes, graph IRIs, GUIDs and RDF values are deliberately excluded to prevent cardinality and information leakage. The endpoint is unauthenticated inside the data-plane listener. Default-deny policy admits data-plane peers and pods explicitly labelled `ngkg.io/metrics-client=true`; approved query-client pods can also reach it on the query service because Kubernetes NetworkPolicy cannot filter HTTP paths. Production gateway/service-mesh authorization should restrict that path if even low-cardinality operational load is confidential.

## HPC and Kubernetes behavior

Admission limits are capacity envelopes, not thread counts. Rust still distributes independent hash partitions across blocking lanes, fragment pods and RKE2 nodes. Hydration continues to read exact Parquet row groups across its bounded worker lanes, and locator lookups continue through the checksum-verified read-only mmap index. OpenMP, OpenBLAS and MKL remain at one thread because none of these sparse request-control or equality-join kernels calls dense BLAS. A future measured dense kernel must share the same cpuset budget rather than nesting an uncontrolled native pool.

HPA CPU and memory targets remain capped at 80 percent. Sustained admitted work raises resource utilization and creates replicas; bounded rejections expose the period where demand exceeded existing lanes before those replicas became ready. Required hostname anti-affinity turns additional replicas into responsibility-specific pending pods so RKE2 Cluster Autoscaler can add the matching node pool. Prometheus may alert on rejection rate and admission wait, but Phase 27 does not configure an unreviewed custom-metrics adapter or allow two independent controllers to own the same replica count.

## Acceptance criteria

1. Authentication and admission happen before request-body extraction and apply to every `/v1/` data operation; unauthenticated traffic consumes no execution capacity.
2. A permit survives until the response body completes or is dropped; Arrow encoder/socket backpressure therefore consumes capacity.
3. Fragment and shuffle traffic cannot exceed their individual ceilings or their shared worker ceiling.
4. Pending waiters are independently bounded for every class, and the monotonic wait deadline is capped at five seconds by Helm validation.
5. Overload returns JSON error code `ADMISSION_CAPACITY_EXHAUSTED`, HTTP 429 and `Retry-After: 1` without invoking semantic execution.
6. Cancellation, body-extraction failure, handler error, encoder failure and client disconnect release all permits.
7. Metrics return to zero in-flight after the workload drains and counters remain monotonic under concurrency.
8. A load test above every configured ceiling produces bounded 429 responses, no unbounded RSS growth, exact results for admitted requests and no stale or partial answers.
9. HPA remains at or below 80 percent CPU and memory, and RKE2 grows only the responsibility pool selected by the pending pod.
10. Rust format/build/Clippy/tests, Helm lint/render/server dry-run, NetworkPolicy, service-mesh, Prometheus scrape and enterprise p50/p95/p99 qualification all pass.

Run the deployed gate with `scripts/qualify_phase27.sh`. Its successful requests are compared with the independent expected SPARQL multiset; overload responses are evidence of bounded control, not substituted application results.

## Intentional boundary

Phase 27 does not implement priority classes, tenant-weighted fair queuing, distributed global concurrency, KEDA/Prometheus HPA ownership, direct peer shuffle, out-of-core worker joins, adaptive skew splitting, arbitrary OWL 2 DL query coverage or a universal speedup claim. Admission is pod-local by design; Kubernetes replica/node scaling supplies cluster-wide capacity.
