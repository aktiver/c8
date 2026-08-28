# Phase 28 — checksum-bound tenant isolation and noisy-neighbor control

Phase 28 builds cumulatively on Phase 27 and closes a multi-tenant enterprise gap. Global pod limits prevent unbounded work, but they do not stop one authenticated tenant from occupying every available query, fragment, shuffle, locator or hydration lane. Every data request now obtains both a tenant-specific permit and the existing global permit before Axum extracts its body. A saturated tenant receives an explicit retryable response without consuming another tenant's reserved capacity.

## Production path

```text
Bearer token
  -> fixed token hash resolves the authenticated tenant
  -> checksum-bound tenant policy lookup
  -> reserve global and tenant pending slots or reject before body extraction
  -> acquire tenant operation lane
  -> fragment/shuffle also acquire the tenant fragment-worker parent lane
  -> acquire the existing global operation and parent lanes
  -> execute the certified RDF/SPARQL path
  -> retain all permits through the complete JSON or Arrow response body
  -> release on completion, failure, cancellation or disconnect
```

The tenant policy is finite, immutable for a process lifetime and validated at startup. Its tenant set must equal exactly the tenants that have `queries:execute` access in `tokens.json`. Duplicate, missing, stale and extra tenant records fail startup. Each tenant ceiling must be positive, must fit Tokio's semaphore boundary and must not exceed its corresponding global ceiling. Fragment and shuffle each have a tenant class limit and share a tenant `fragmentWorkerMaxInFlight` parent, matching the global worker envelope.

For a policy with multiple tenants, every tenant's class and shared fragment-worker ceiling must be strictly smaller than the corresponding global ceiling; every pending ceiling must also leave at least one global pending slot. This makes the isolation guarantee structural rather than dependent on an operator remembering to reserve a peer lane.

The policy file follows `contracts/tenant-admission-policy.schema.json`, is limited to a non-empty 1 MiB regular file and is mounted from an operator-owned Secret at `/var/run/ngkg/admission/tenant-policy.json`. The token file has the same 1 MiB regular-file bound. Both file SHA-256 values are supplied independently through Helm and verified before the listener starts. Both checksums are pod-template annotations, so a Helm upgrade performs one controlled rollout of authorization and admission state rather than allowing replicas to serve different tenant sets or policies indefinitely.

## Why this is database work, not transport theory

NGKG remains an RDF graph database. OWL-certified graph routes, named-graph fragments, exact SPARQL bag semantics, distributed RDF hash joins, the mmap GUID locator and Parquet hydration determine query meaning and execution. HTTP and Arrow IPC are only the inter-node carrier for already typed RDF bindings. Tenant admission surrounds those database operations so enterprise capacity remains isolated; it does not infer facts, choose ontology consequences or alter results.

## Metrics and privacy

`ngkg_admission_rejections_by_scope_total` distinguishes global saturation from tenant saturation using only `role`, `class` and `scope` labels. `ngkg_tenant_admission_configured` exposes the finite policy count. Tenant UUIDs, principals, query hashes, graph IRIs, GUIDs and RDF values are never Prometheus labels. Existing admitted, pending, in-flight, service-time and cache metrics remain intact.

## HPC and RKE2 behavior

Tenant ceilings reserve access to the existing sparse parallel engine; they do not create nested native thread pools. Hash partitions continue across Rust lanes, fragment pods and RKE2 nodes. The fixed-width locator remains checksum-verified and read-only mmap-backed. Hydration still performs direct, bounded Parquet row-group reads. OpenMP, OpenBLAS and MKL remain at one thread because neither admission nor sparse RDF equality joins call dense matrix kernels. A separately measured dense ranking kernel may use BLAS only under one mutually budgeted cpuset.

HPA remains the only online replica owner and stays at or below 80 percent CPU and memory. Tenant saturation does not independently change replica count: it protects peer tenants while ordinary resource utilization drives pod scaling and required anti-affinity creates responsibility-specific RKE2 node-pool demand. Raising a tenant limit without raising measured pod/node capacity is prohibited operationally even if it remains below the global maximum.

## Acceptance criteria

1. Every authenticated `/v1/` operation acquires tenant and global capacity before request-body extraction.
2. The token-file and policy SHA-256 values, formats, finite counts, unique tenant IDs and exact equality with authorized query tenants are verified before the listener binds.
3. Every tenant execution and pending limit is positive and bounded by its global limit; a multi-tenant policy must leave at least one global execution and pending lane outside each individual tenant envelope.
4. Fragment and shuffle traffic share both tenant-level and global fragment-worker parent ceilings.
5. A saturated tenant receives HTTP 429, `Retry-After: 1` and `TENANT_ADMISSION_CAPACITY_EXHAUSTED`; another tenant can still obtain an unused global lane.
6. Global saturation continues to return `ADMISSION_CAPACITY_EXHAUSTED` with the same retry contract.
7. Both tenant and global permits remain held until the complete response body ends or is dropped, including Arrow backpressure and disconnects.
8. Metrics distinguish tenant versus global rejection without exposing high-cardinality or customer-identifying labels.
9. A multi-tenant load test proves exact certified results for every admitted request, bounded RSS, no cross-tenant data access and forward progress for a conforming tenant while another is saturated.
10. Rust format/build/Clippy/tests, JSON Schema, Helm lint/render/server dry-run, Secret rollout, default-deny networking, service-mesh and RKE2 80-percent scaling gates pass.

Run `scripts/qualify_phase28.sh` against two independently provisioned tenants. The test deliberately overloads tenant A while repeatedly executing a certified query for tenant B, compares all admitted result bags with their independent expected files and verifies tenant-scoped rejection metrics.

## Intentional boundary

Phase 28 implements deterministic per-replica tenant isolation, not a distributed global quota ledger, weighted priority scheduler or billing system. Kubernetes load balancing may place requests unevenly, so customer-wide concurrency across all replicas is the sum of the participating replica limits. Globally coordinated tenant quotas require a separate low-latency lease service and failure model. This phase also does not add arbitrary OWL 2 DL query coverage, a distributed property-path frontier, adaptive skew splitting, direct peer shuffle, out-of-core worker joins or a universal 20–50× speed claim.
