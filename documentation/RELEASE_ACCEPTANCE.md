# Release acceptance

The phase archives are cumulative engineering deliverables. They are not evidence that an exact OWL 2 DL database has already met production correctness, durability, security, or 20–50× performance targets.

## Required release evidence

- 100% SPARQL multiset equality against the independent corpus, including named-graph dataset semantics, duplicates, ordering when defined, proofs, provenance, and authorization.
- Every exact query returns with a plan-bound coverage certificate or version-matched exact reasoner completion.
- One-node and N-node builds have identical GUIDs, FactIDs, logical facts, extents, routing, proofs, locators, and public answers across partition/retry matrices.
- Phase 16 forward, reverse, retried, and rescheduled artifact completions have identical per-partition manifests, Parquet schemas, locator bytes, logical counts, and semantic roots before the certified service path may consume the sharded layout.
- Phase 17 catalog/object-store executions prove dense completion coverage, data-before-manifest ordering, locator-before-root ordering, operator restart recovery, reference-count equivalence, and no reasoner admission before the artifact root exists.
- Projection satisfies source RDF equals `core ∪ virtual` for the declared scope; hydration fields never change answer eligibility.
- Ordinary query/hydration succeeds while object-store listing is denied; every opened object verifies snapshot/schema/hash.
- Fault injection never exposes a partial snapshot, partial stream, stale index, missing qualified locator key, or unauthorized dependency as success.
- Phase 27 overload qualification proves hard pending and execution bounds, permit release on cancellation/slow-reader disconnect, retryable 429 behavior, exact admitted answers, low-cardinality metrics and stable RSS above every configured concurrency ceiling.
- Phase 28 multi-tenant qualification proves checksum-bound policy rollout, exact authorized-tenant coverage, tenant/global permit release, tenant-scoped saturation, peer-tenant forward progress, exact result equality and absence of tenant identifiers in metrics.
- Phase 29 recurring-query qualification proves a cold certified miss and same-pod hot hit have byte-identical complete responses and independent expected bags, while corruption, cross-identity reuse, cache churn, RSS, ephemeral storage and RKE2 scaling remain bounded.
- Phase 30 skew qualification forces the worker Grace path, proves exact bag equality against an independent expected result, bounds build/probe chunks and total/per-request spill, rejects corrupt files/evidence, drains active spill accounting, and plateaus RSS and ephemeral storage under admitted concurrency.
- Phase 31 streamed-input qualification sends chunked Arrow requests through the marker-owned local-NVMe spool, proves exact byte/SHA evidence at the coordinator, rejects truncation/overrun/corruption, feeds large decoded relations directly into Grace buckets, drains both request and join gauges, and preserves the independently certified SPARQL bag.
- Phase 32 coordinator-streaming qualification proves production shuffle requests are encoded from incremental checksum-valid spill readers, remain bounded to one Arrow batch plus the configured channel, report byte-identical worker evidence, increase streamed-request metrics, and preserve the independently certified SPARQL bag.
- Phase 33 fragment-ingress qualification proves initial distributed Arrow responses are written to a process-budgeted NVMe spool, decoded incrementally after checksum validation, report positive public ingress evidence, drain active bytes and preserve an independently certified SPARQL bag.
- Phase 34 direct-partition qualification proves an eligible certified shuffle reports `direct_spool_to_primary_partition_v1`, owns zero complete ingress rows, increments direct fragment/row counters, rejects iterator failures without orphan spill files, drains all spool gauges and preserves the independent expected bag.
- Phase 35 multi-stage qualification proves every shuffle result is streamed to verified NVMe, exact partitions are retained as a lazy ordered spool sequence, no stage-wide intermediate rows are assembled, response counters cover all stage partitions, gauges drain and the independent expected bag remains exact.
- Helm install, no-op/forward upgrade, failed rollback, and uninstall pass on supported Kubernetes/RKE2 minors without altering the active dataset snapshot.
- RKE2 capacity probes resize only the intended Rancher worker pool, and joined nodes pass label/taint, CNI, mTLS, object-store, catalog, cpuset/NUMA, and spill checks.
- Default-deny and explicit data-plane flows pass with an enforcing CNI; mTLS rejects untrusted identities.
- Raw benchmark artifacts retain hardware, images, compiler flags, workload/snapshot/ontology/mapping hashes, cache state, concurrency, errors, and exact comparison evidence.

## Performance claim

For the approved selective production workload, the exact target is geometric-mean speedup at least 20× versus the same-hardware Jena plus certified DL baseline, and hot-query median at least 50×. Failed or unequal queries are failures, not omitted samples. The claim does not apply to unselective scans, arbitrary broad aggregates, unsupported SPARQL, or necessarily uncached global OWL 2 DL reasoning.

Run `scripts/benchmark_exact.py` against real, authenticated NGKG and certified-baseline endpoints. It resets declared cache states through controlled endpoints, runs every query at every concurrency, compares both systems to checked-in expected multisets, and exits nonzero if correctness or either speed target fails.

## Current artifact status

The creation workspace has Python and Git but does not have Rust/Cargo, Helm, kubectl, a PostgreSQL/object-store integration environment, a certified reasoner, or an RKE2 qualification cluster. Consequently the checked-in reports mark those gates `blocked`. Only structural parsing, cross-field value checks, Git ancestry, and archive integrity are asserted locally.
