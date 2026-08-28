# Reference worker image

The Dockerfile builds the Rust worker and HermiT adapter into one Kubernetes Job image. All three base-image arguments are required and must be supplied as immutable digest references; there are intentionally no mutable defaults.

```bash
test -n "${RUST_BUILDER_IMAGE:?required digest-qualified reference}"
test -n "${MAVEN_BUILDER_IMAGE:?required digest-qualified reference}"
test -n "${RUNTIME_IMAGE:?required digest-qualified reference}"
test -n "${NGKG_REFERENCE_IMAGE:?required release reference}"
docker build \
  --build-arg RUST_BUILDER_IMAGE="${RUST_BUILDER_IMAGE}" \
  --build-arg MAVEN_BUILDER_IMAGE="${MAVEN_BUILDER_IMAGE}" \
  --build-arg RUNTIME_IMAGE="${RUNTIME_IMAGE}" \
  -f deploy/reference-worker/Dockerfile \
  -t "${NGKG_REFERENCE_IMAGE}" .
```

Release automation must validate that every builder/runtime value contains `@sha256:` and record the produced image digest in provenance. The runtime base must contain a Java 17-compatible executable at the operator-configured path. The build uses `cargo --locked`, so a reviewed `Cargo.lock` is a hard prerequisite rather than an optional fallback.

In Kubernetes this binary runs as an immutable Job work unit. The operator supplies the adapter checksum and identity, storage root, resource ceilings, transfer concurrency, and one catalog-bound bundle key/checksum. The user-provided bundle never selects the executable JAR, database, bucket, endpoint, output prefix, or resource ceiling.

The `compile-object-store` command downloads exact manifest keys in parallel, stages them below bounded ephemeral scratch, executes the Phase 13 compiler, and uploads only the files named by its verified snapshot manifest. It uploads `snapshot-manifest.json` last and commits the catalog only after every remote object verifies. S3 credentials come from an optional operator-selected Secret or from workload identity when `serviceAccounts.referenceWorker.automountToken=true` and the matching service-account annotations are configured; they never appear in the compilation resource or CLI arguments.

Large-object multipart writes require an immutable-prefix storage policy that denies overwrite after creation. Object keys include tenant, dataset and snapshot identity, and the worker detects an existing object with different bytes as a hard conflict. Bucket lifecycle policy should remove abandoned multipart uploads and unreferenced unpublished objects only after catalog-aware retention review.

Phase 14 schedules one full reference compilation per pod while Kueue and the RKE2 node autoscaler distribute independent operations across nodes. Distributed Indexed Jobs in the next phase will reuse the same object, catalog, checksum, identity, and publication rules for syntax-safe partitions and deterministic reducers.
