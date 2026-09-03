# Optional large context-slice broker

Phase 10 adds an optional, separately deployed broker for reasoned context graphs that are too large for an MCP response or model context window. It does not change NGKG semantic authority: the slice is derived from an already authorized, snapshot-pinned result and is bound to the dataset ID, snapshot ID, authorized graph-set SHA-256, and `semanticResultSha256`. Models receive only verified bytes and the redacted manifest; they never receive cloud object keys.

The feature is disabled by default. When disabled, the Phase 9 inline context-envelope behavior is unchanged.

## Trust and data boundaries

- Only `ngkg-context-slice-broker` and its lifecycle GC worker use the `ngkg-context-slice` service account. The MCP gateway has no context-slice bucket permission.
- Use an IAM/KMS policy limited to one tenant-rooted context-slice prefix. Do not reuse the prompt-input bucket role.
- S3, Azure Blob, and GCS credentials come from EKS Pod Identity/IRSA, Azure Workload Identity, or GKE Workload Identity. RKE2 uses the approved external workload-identity/Vault integration.
- Bucket-level encryption with the declared KMS key is mandatory. Helm stores only the SHA-256 of the key identifier as evidence; it never stores key material.
- PostgreSQL forced RLS derives the tenant from verified authentication. Request JSON cannot select a tenant.

## Lifecycle

1. `POST /v1/context-slices` creates an `UPLOADING` slice with frozen semantic bindings, TTL, chunk size, total bytes, and deletion recovery window.
2. `PUT /v1/context-slices/{sliceId}/chunks/{ordinal}` verifies `X-NGKG-Content-SHA256` before an immutable content-addressed write. A retry succeeds only if every byte and range matches.
3. `POST /v1/context-slices/{sliceId}/finalize` reads every chunk in ordinal order, verifies each digest, verifies the caller's full-content digest, builds the fixed-width locator index, creates the canonical manifest, and atomically marks the slice `ACTIVE` in PostgreSQL.
4. `POST /v1/context-slices/{sliceId}/capabilities` issues a short-lived HS256 capability bound to tenant, subject, slice, immutable manifest, policy version, exact byte range, exact audience, expiry, and a unique nonce. The signing key is a checksum-bound mounted Secret used only by broker pods.
5. `GET /v1/context-slices/{sliceId}/content` requires the capability and audience headers. The broker rechecks the nonce and active slice in PostgreSQL, verifies the index, verifies every selected chunk, and returns exactly the authorized range as HTTP 206.
6. Expiry prevents reads immediately. Deletion waits through the configured recovery window. GC leases make retries and node loss safe; successful removal creates an immutable checksum evidence tombstone.

## Verified mmap index

The locator ABI is deliberately small and fixed width. Its 96-byte header contains magic `NGKGSIDX`, version, record width, count, total content bytes, content hash, and record-array hash. Each 56-byte record contains only chunk SHA-256, ordinal, byte start, and exclusive byte end.

Before use the broker enforces the configured maximum bytes and records, then checks regular-file type, no symlink, exact owner UID, exact length, full index SHA-256, magic, version, record width, count arithmetic, reserved bytes, record-array hash, strict `(hash, ordinal)` ordering, unique ordinals, contiguous offsets, and exact total length. Verified bytes are copied into an anonymous mapping and converted read-only. The staged file is removed; raw RDF, JSON, prompts, model output, remote bodies, mutable spill, and attachments are never mmap'd.

`ngkg_context_mapped_bytes` reports the mapping size. `resident_estimate_bytes` conservatively treats every mapped byte as resident so memory-cgroup budgeting remains safe during cold-cache page faults. Maximum mapped bytes, in-flight range reads, chunk bytes, and file descriptors are bounded by configuration and pod limits.

## Kubernetes enablement

Create a random signing key and its checksum without putting the key in Helm values:

```bash
openssl rand 64 > context-capability.key
kubectl -n ngkg create secret generic ngkg-context-capability-signer \
  --from-file=signing-key=context-capability.key
sha256sum context-capability.key
```

Set `contextSlice.enabled=true`, the returned `contextSlice.capabilityKeySha256`, a SHA-256 of the provider KMS key identifier, the provider bucket/container, and workload-identity annotations. Install or upgrade with the existing immutable image digest:

```bash
helm upgrade --install ngkg-agents ./charts/ngkg-agents -n ngkg --create-namespace \
  --values values-production.yaml \
  --set contextSlice.enabled=true \
  --wait --timeout 20m
```

Broker replicas spread across nodes and zones, have a disruption budget, and scale on either CPU or RAM at exactly 80%. Their CPU/memory requests make pending pods visible to Karpenter, Cluster Autoscaler, or the provider node provisioner. The GC replicas use `FOR UPDATE SKIP LOCKED` leases, so work distributes safely across nodes without duplicate completion. Hashing and index construction run on bounded blocking CPU work; Tokio owns network and control flow. OpenMP, OpenBLAS, and MKL threads are fixed at one because this Rust path uses no nested OpenMP region.

## REST and Swagger

The full standalone contract is `contracts/context-slice-openapi.yaml`. Each broker serves it at `/openapi.yaml` and serves interactive Swagger at `/swagger-ui`. Every slice-management feature is REST driven. The binary content route is capability driven and deliberately does not accept an OAuth token as a substitute.

## Required production qualification

Source validation is not production qualification. Before enabling the feature, execute live tests against each supported cloud: concurrent multipart upload, repeated chunks, content collision, index bit flip, object bit flip, truncated object, wrong tenant, wrong subject/policy, wrong audience, expanded range, expired/revoked capability, slice expiry during a read, broker loss, PostgreSQL failover, bucket throttling, GC worker loss, duplicate deletion, recovery-window restoration, cold cache, memory pressure, file-descriptor pressure, HPA at 80% CPU and RAM, node scale-out, zone loss, and restore from backup. No corruption or partial range may be returned as successful content.
