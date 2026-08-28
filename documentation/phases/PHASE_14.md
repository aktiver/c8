# Phase 14 — Atomic catalog, object-store, REST, and Kubernetes slice

Phase 14 implements the tutorial's second integration milestone on top of the exact Phase 13 compiler. It turns the local reference executable into an authenticated asynchronous service whose durable truth survives API, operator, worker, and node restarts.

## Executable request path

```text
checksum-addressed input objects + compilation-bundle.json
  → authenticated REST ingestion request
  → tenant-scoped PostgreSQL operation + immutable audit revision 0
  → deterministic NgkgCompilation custom resource
  → restart-safe operator reconciliation
  → Kueue-labelled, digest-pinned Kubernetes Job
  → bounded parallel object downloads to disposable scratch
  → unchanged Phase 13 TriG/Parquet/HermiT/certification compiler
  → bounded parallel upload of every manifest-listed artifact
  → snapshot-manifest.json uploaded last
  → tenant-scoped all-or-nothing certification transaction
  → optional active-snapshot compare-and-swap publication
```

The API never accepts bulk TriG, ontology, Parquet, or result bytes. The caller places immutable objects below the operator-owned `file://` or `s3://` root and submits a relative object key plus SHA-256. `scripts/stage_reference_bundle.py` demonstrates the local filesystem form without weakening the production object contract.

## Correctness and recovery boundary

- PostgreSQL row-level security is initialized with `set_config('ngkg.tenant_id', ..., true)` inside every tenant transaction. Tenant identity comes from the bearer-token mapping, not the JSON body.
- The API commits the operation before creating Kubernetes desired state. If Kubernetes is unavailable after the catalog commit, the caller receives `503` and safely retries the same body and idempotency key; the original operation is returned and the same CR name is ensured.
- The API and operator share one generated Rust CR type. The operator compares every CR field with catalog truth before scheduling work.
- A Job is named from the operation ID. Reconciliation is level-based; an operator restart observes the existing Job instead of creating logical duplicates.
- The worker downloads exact object keys. It never lists the bucket. Every stream is bounded and checksum-verified before compilation.
- The worker rejects a bundle unless its dataset ID, snapshot ID, parent snapshot, identity namespace, and projection-policy ID exactly match durable catalog truth. This prevents a correctly hashed but semantically misbound bundle from creating GUIDs under the wrong namespace or policy.
- Snapshot artifacts are selected from the compiler's verified manifest rather than discovered by listing scratch. The snapshot manifest is the last uploaded object and the catalog points to it only after remote verification succeeds.
- A storage, network, PostgreSQL, or pod failure is retried by the Kubernetes Job while the operation remains `REGISTERED`. Once the Job's bounded retry budget is exhausted, the operator records `INFRASTRUCTURE_RETRY_EXHAUSTED` as a terminal `FAILED` transition; a new request is required. A deterministic bundle or semantic compiler failure is recorded as `FAILED` immediately.
- A Kubernetes Job that reports `Complete` while catalog truth remains `REGISTERED` is a protocol violation and becomes `JOB_COMPLETED_WITHOUT_CERTIFICATION`; it is never mistaken for a successful build.
- The atomic reference worker makes all logical stage/audit revisions visible in one PostgreSQL transaction only after the complete Phase 13 pipeline succeeds. Readers cannot observe a half-certified stage sequence.
- Automatic publication is a second guarded transaction. A lost parent-snapshot CAS leaves the new snapshot `CERTIFIED` and the old active snapshot unchanged.
- Cancellation uses an operation revision CAS. A worker that finishes after cancellation cannot commit certification; uploaded unreferenced objects are harmless retention candidates.

## HPC and Kubernetes behavior in this phase

One Phase 14 compilation remains one large reference Job because the tutorial requires atomic single-node correctness before partitioning a compilation across nodes. The implementation still uses cluster and node parallelism where it is already safe:

- independent ingestion operations run on different semantic-projection nodes through Kueue;
- a Job requests equal CPU/memory requests and limits for Guaranteed QoS eligibility;
- input materialization and immutable artifact publication use bounded concurrent object streams;
- Parquet/Arrow and HermiT use the whole CPU/memory allocation sequentially rather than oversubscribing nested OpenMP/BLAS pools;
- scratch is a bounded `emptyDir`, never durable truth;
- node labels and taints route the Job to the RKE2 semantic-projection worker pool; and
- pending Kueue-admitted Jobs can drive the existing RKE2 Cluster Autoscaler capacity chain.

Phase 15 is the distributed-build milestone: syntax-safe TriG shards, independent projection Jobs, deterministic identity/dictionary/index reducers, distributed reasoner modules, and one-node-versus-N-node equality. Phase 14 deliberately does not fake that capability by splitting arbitrary TriG byte offsets.

## Deployment identities and least privilege

The `migration-database-url` identity owns schema migration. The `database-url` identity must be a separate `NOINHERIT`, non-superuser role without `BYPASSRLS` and must not own the tables. After migrations, grant it only `CONNECT`, schema/type `USAGE`, sequence `USAGE` if future migrations add sequences, `SELECT`/`INSERT` on the Phase 14 tables, and `UPDATE` on `dataset`, `operation`, and `snapshot`. Do not grant `DELETE`, `TRUNCATE`, `REFERENCES`, `TRIGGER`, table ownership, or migration-table writes. `FORCE ROW LEVEL SECURITY` is not a defense against a superuser or `BYPASSRLS` role.

The object-store identity requires exact-key `GetObject` for the immutable input prefix and `GetObject` plus create/write access for its tenant snapshot prefix. It does not require `ListBucket` or object deletion. Production policy must prevent overwriting immutable content-addressed objects. When workload identity is used, set `serviceAccounts.referenceWorker.automountToken=true` and provide the provider-specific service-account annotations; otherwise leave token automount disabled and inject only the operator-selected object-store credential Secret.

## Public API implemented

The running API serves the reviewed contract at `GET /openapi.yaml` and implements:

- `PUT /v1/datasets/{datasetId}`
- `POST /v1/datasets/{datasetId}/ingestions`
- `GET /v1/jobs/{operationId}`
- `POST /v1/jobs/{operationId}/cancel`
- `GET /v1/datasets/{datasetId}/snapshots/{snapshotId}`
- `POST /v1/datasets/{datasetId}/snapshots/{snapshotId}/publish`

Each protected operation declares a scope in OpenAPI. Phase 14 uses an operator-mounted file containing only high-entropy token hashes and their tenant/principal/scope mappings. A production OIDC/JWKS adapter may replace this implementation later without changing catalog tenant derivation or endpoint authorization semantics.

## Acceptance gate

The Phase 14 gate requires a real PostgreSQL, S3-compatible store (and local-store conformance), Kubernetes/RKE2, Kueue, the pinned Rust/Java/Maven toolchains, and digest-built images. It must prove:

1. migrations 1 and 2 apply once and readiness rejects an older schema;
2. authentication and scope checks happen before catalog or Kubernetes disclosure;
3. identical ingestion retries return one operation and one CR; changed bytes under the same key return `409`;
4. API and operator restarts do not duplicate logical work;
5. object listing permission can be denied throughout compilation and publication;
6. corrupt bundle, source, ontology, query, expected result, reasoner JAR, uploaded artifact, or snapshot manifest fails closed;
7. storage failure before catalog certification is retryable and leaves no active snapshot change;
8. deterministic semantic failure becomes terminal with an immutable audit transition;
9. cancellation races cannot publish;
10. automatic and manual publication preserve parent-snapshot CAS semantics;
11. the downloaded snapshot reproduces the Phase 13 exact query and direct GUID hydration result; and
12. RKE2 placement, Kueue admission, node scale-up, node loss, retry, and scale-down preserve the same catalog result.

This archive does not claim those external-system gates passed unless `verification/phase-14.json` records their actual execution. Missing Cargo, Maven, PostgreSQL, S3, Helm, Kubernetes, Kueue, or RKE2 remains a blocked gate, never a simulated pass.
