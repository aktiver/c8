# Enterprise Stabilization Phase 4

Phase 4 closes runtime-correctness and durable-orchestration defects identified by the 2026-08-29 audit. It is source implemented, but it is not production-qualified until the Phase 3 controlled workflow and the Phase 4 live regression matrix produce signed certificates.

## Runtime contracts

- Kubernetes status writers use field-owned server-side apply. Operator and worker field sets do not overlap, and apply conflicts are retried by controller reconciliation rather than forced.
- `orchestration_stage` is the completion authority. A Job is only an attempt. The immutable stage-spec hash is reserved before Job creation; terminal Job evidence is committed before TTL garbage collection; `SUCCEEDED` prevents recreation after restart.
- Source upload identity is reserved in PostgreSQL before reading or publishing the body. The source and metadata objects are conditional/checksum-equal publications. Large uploads use a checksum-derived staging object followed by atomic copy-if-absent.
- Source mount capacity equals the admitted source-byte ceiling rather than an unmanaged 1-PiB claim. Static mount handles use explicit reclaim ownership.
- Grace-join and shuffle executors remove only their own UUID-scoped directories. They never sweep another live execution at startup.
- Online whole-file hashing runs in Tokio's blocking pool. Request tasks do not perform synchronous whole-artifact reads.
- SPARQL result negotiation is completed before query admission, execution, cache lookup, or audit-log creation. Unsupported formats return `406` without consuming query resources.
- `/v1/query_logs` distinguishes requested resources, Kubernetes allocation, and measured use. `COORDINATOR_CGROUP_INTERVAL` means CPU is a cgroup interval delta and peak RSS is the observed coordinator cgroup peak; it does not claim worker-wide attribution. Pod/node UID lists and GPU/autoscaling evidence are empty until a measuring participant supplies them.
- All query lanes use graph-IRI-scoped blank nodes. All OWL qualification/offline/exact lanes require the same checksum-bound `ngkg-owl2-direct-datatype-policy-v1` artifact.
- Federation validates every DNS resolution, rejects mapped IPv4, NAT64, 6to4, Teredo, private/link-local/loopback addresses, disables redirects, and reuses only clients keyed by both endpoint and pinned socket address.
- Artifact, checkpoint, spill, backup, and recovery storage accepts file, S3, Azure Blob, and Google Cloud Storage roots through workload-identity-aware native backends.
- Locator indexes are checksum-verified immutable files mapped read-only. Replacement must publish a new path and atomically switch the manifest; a mapped file is never mutated in place.

## Required live evidence

Run migration 0010 against a disposable PostgreSQL HA cluster, execute concurrent writer and retry tests, force operator restarts and Job TTL deletion, run parallel spill users, issue invalid `Accept` requests under load, rotate federation DNS, and exercise artifact publication against S3, Azure Blob, and GCS. Repeat the Phase 3 RKE2/EKS/AKS/GKE, OCI, SBOM, signing, recovery, tenant-isolation, autoscaling, HermiT, and GPU gates. No release label may say production-qualified without both signed certificates.
