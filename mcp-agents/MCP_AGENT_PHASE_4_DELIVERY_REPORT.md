# NGKG MCP/Agent Phase 4 Delivery Report

## Outcome

Phase 4 implements the long-input ingestion and deterministic context compiler
milestone on the unchanged Phase 3 authentication/catalog baseline. It accepts
large prompts and arbitrary attachments without embedding them in MCP JSON,
preserves exact source bytes, and compiles supported text into a permanent,
evidence-linked constraint ledger.

## Delivered implementation

1. Six authenticated REST operations create an input, upload checksum-bound
   parts, finalize the exact source manifest, inspect status, read a redacted
   manifest, and list extracted requirements. Tenant, subject, actor, and
   scopes come only from the Phase 3 verifier.
2. The `ngkg-agent-input` Rust crate provides cloud-neutral storage, immutable
   repository transitions, structural compilation, stable domain-separated
   identifiers, deterministic roots, and extractive context reduction.
3. S3, Azure Blob, GCS, and a local-test backend use the Apache Arrow
   `object_store` interface. Provider SDK configuration comes from workload
   identity/environment; Helm never accepts access keys.
4. Part writes are idempotent only when ordinal, length, digest, and object
   reference all match. Finalization verifies contiguous ordinals, byte count,
   configured limits, and the caller's source root in one PostgreSQL lock.
5. Migration `0003` adds forced-RLS input, part, shard, chunk, requirement,
   compiled-context, and coverage tables. Immutable evidence rejects updates
   and deletes; prompt and shard state machines are database enforced.
6. A narrow opaque dispatcher queue lets workers claim across tenants without
   a `BYPASSRLS` runtime role. Security-definer functions expose only a single
   lease and install its tenant before touching forced-RLS rows. Direct queue
   privileges are revoked.
7. The compiler streams and verifies an object, then uses a cgroup-aware Rayon
   pool. Independent extraction runs in parallel, while part ordinal, source
   byte offset, and stable-ID sorts make one-core, many-core, and multinode
   results identical.
8. UTF-8 and Markdown inputs produce bounded structural chunks, heading paths,
   byte spans, instructions, prohibitions, acceptance criteria, required
   outputs, and identifiers. Binary files are preserved and compiled as opaque
   zero-derived-content parts until an approved parser is added.
9. Context reduction always selects mandatory requirement source chunks before
   optional context. It rejects an insufficient budget instead of dropping a
   constraint. Summaries and model output remain derived, non-authoritative
   artifacts.
10. Helm 0.4.0 adds cloud workload-identity service accounts, a hardened
    compiler Deployment, topology spreading, default-deny egress, resource
    limits, and an HPA that scales on either 80% CPU or 80% memory. Pending pods
    can trigger the configured RKE/RKE2, EKS, AKS, or GKE node provisioner.

## Trust and scale boundaries

Exact source objects are authoritative. PostgreSQL stores state, offsets,
hashes, object references, requirements, leases, and evidence roots, not cloud
credentials or raw source blobs. The gateway never returns internal bucket
keys. Unsupported files remain usable as attached evidence but do not silently
produce guessed text.

OpenMP is not used: this phase is safe Rust string/hash work, for which Rayon is
the appropriate in-process data-parallel runtime. `mmap` is not used against
remote object storage. A later context-slice broker may use checksum-verified,
read-only memory maps for measured hot immutable artifacts.

## Qualification status

- Supplied Phase 3 archive SHA-256 and internal manifests passed before edits.
- The frozen NGKG 1.0 GA sibling's static gates passed unchanged.
- JSON syntax, shell syntax, Phase 3 authentication gates, Phase 4 SQL/API/
  compiler/storage/Helm structural gates, and regenerated source manifests
  pass.
- Native Rust format/check/test/clippy is blocked because Rust/Cargo and a
  controlled dependency mirror are unavailable. A `Cargo.lock` was not
  fabricated.
- Helm lint/template and live PostgreSQL, object-store, RKE/RKE2, EKS, AKS,
  GKE, node-loss, lease-recovery, and autoscaling tests require those
  toolchains and clusters.

This is a source-implemented candidate, not a compiled, signed, scanned, or
live production-qualified release.

## Next phase

Phase 5 is the managed agent orchestrator and model-provider boundary. It will
consume `inputId`, build bounded contexts from the Phase 4 ledger plus certified
NGKG graph slices, drive Claude/OpenAI-compatible/Hugging Face/vLLM adapters,
validate claims, and issue reasoning-bound answer certificates. GPU node pools
and vLLM queue autoscaling enter that phase.

After Phase 4, nine planned source phases remain: Phase 5 through Phase 13.
