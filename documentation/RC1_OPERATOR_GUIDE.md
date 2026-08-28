# NGKG 1.0.0-RC1 operator guide

The RC1 operator surface is frozen by `release/1.0.0-rc1/freeze-manifest.json`. Deploy only digest-pinned images and signed Helm packages whose hashes occur in the signed artifact manifest. Do not deploy from an unqualified source archive or substitute a mutable image tag.

Installation begins with provider workload identity, encrypted PostgreSQL and object storage, a compatible CNI enforcing default-deny policy, Metrics Server, Prometheus, KEDA, Kueue, HPA, and one provider-specific node autoscaler. Install CRDs before platform and workload charts. Configure API, SPARQL, ingestion, compilation, reasoning, query, federation, and recovery identities separately. Bucket mounts and object API access must be read-only for ingestion and use S3, Azure Blob, Google Cloud Storage, or the approved RKE/RKE2 S3-compatible path.

The control and online OpenAPI documents are authoritative for `/sparql`, `/query`, `/query_logs`, ingestion, snapshot, backup, restore, recovery, and administration. Tenant identity comes from authentication, never request bodies. Use `/query_logs` to correlate a principal and query execution with start/end epoch time, human duration, activated nodes, CPU, RAM, outcome, and request identifiers.

Autoscaling retains the frozen 80-percent CPU-or-memory threshold. HPA/KEDA own pod demand, Kueue owns batch admission, and the cluster autoscaler owns node capacity. Heavy pools may scale from zero. Checkpoints, spill, incomplete property-path frontiers, recovery work, and response spools block unsafe scale-down.

Before an upgrade, verify a recoverable backup, freeze publication changes, and validate version-skew policy. Apply migrations in numeric order and never expose a snapshot until every migration, compiler/reasoner partition, checksum, and publication barrier completes. On failure, stop admission, roll back the Helm release where supported, restore the catalog/snapshot when required, and prove query plus artifact identity before reopening traffic.

Operational response procedures remain fail closed: checksum mismatch quarantines an object; missing reasoning/query partitions prevent answers; federation endpoints remain allowlisted; expired identity denies access; restore failure cannot publish; and an uncertified partial result is never returned as complete. Export audit chains, metrics, traces, query logs, recovery evidence, and exact release digests with every support case.
