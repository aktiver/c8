# NGKG Phase 8 staging deployment and semantic smoke-test runbook

Run all commands from the extracted candidate root. This is a fail-fast staging path, not a production qualification.

## 1. Select a supported target and install tools

Use a current Kubernetes 1.35-1.37 patch release. Install Docker with Buildx, Python 3.11+, Helm 3 or 4, kubectl within one minor version of the API server, curl, jq, OpenSSL, and uuidgen. Install the locked Python validation dependencies in an isolated environment:

```bash
python3 -m venv .venv
.venv/bin/pip install -r NGKG_1_0_0_GA/conformance/python-requirements.lock
.venv/bin/python scripts/deployment_static_preflight.py
```

Before a full workload deployment, install Metrics Server and Kueue. Install MPI Operator only for `hpc.enabled=true`; install KEDA and Prometheus only for a profile that selects KEDA or vLLM scale-to-zero.

## 2. Build and push all 13 images

Copy `docker_repos/example.env` to an ignored local file and replace every registry and `<64-hex>` placeholder. The registry address must resolve from every Kubernetes node; `localhost:5000` normally points at the node itself, not the build host.

For the first build, use `NGKG_BUILD_OFFLINE=false`. The Rust base must contain Rust 1.97.1; the Maven/Java bases must support Java 17; the MPI bases must contain compatible compilers, rank transport, `mpirun`, and shared libraries.

```bash
set -a
. .ngkg/build.env
set +a
./build_all_images.sh
jq -e '.images | length == 13' docker_repos/generated/image-lock.json
```

This must create three generated Helm values files whose image references are registry digests. Do not continue if any image build, push, digest lookup, or parity check fails.

## 3. Prove cluster-side registry access

Create the namespace and a pull Secret when required:

```bash
export NGKG_NAMESPACE=ngkg
kubectl create namespace "$NGKG_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NGKG_NAMESPACE" create secret docker-registry ngkg-registry \
  --docker-server="$NGKG_LOCAL_REGISTRY" \
  --docker-username="$NGKG_LOCAL_REGISTRY_USERNAME" \
  --docker-password="$NGKG_LOCAL_REGISTRY_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Put this value in every site values file when the registry is private:

```yaml
imagePullSecrets:
  - name: ngkg-registry
```

Any separately created cloud-import `identityRef` ServiceAccount must also reference that Secret. Registry login on the build host does not authenticate Kubernetes nodes.

## 4. Create runtime configuration

Follow `NGKG_1_0_0_GA/QUICKSTART.md` to create separate migration/runtime PostgreSQL credentials, the control bearer-token file and checksum, object-store configuration, and `.ngkg/platform-values.yaml`. Use HA PostgreSQL and S3-compatible object storage reachable from the pods.

For a later online query smoke test, the token needs at least:

```json
{
  "tokenSha256": "<sha256-of-bearer-token>",
  "tenantId": "<tenant-uuid>",
  "principalId": "staging-admin",
  "scopes": [
    "datasets:write",
    "sources:write",
    "ingestions:create",
    "jobs:read",
    "snapshots:read",
    "snapshots:publish",
    "queries:execute"
  ],
  "graphAuthorizationLabels": ["<label-issued-for-authorized-graphs>"]
}
```

The workload chart additionally requires its TLS, database, auth, tenant-admission, object-store, and reasoner values. A minimal tenant admission entry has bounded positive limits for all roles:

```json
{
  "formatVersion": 1,
  "tenants": [{
    "tenantId": "<tenant-uuid>",
    "query": {"maxInFlight": 2, "maxPending": 4},
    "fragment": {"maxInFlight": 2, "maxPending": 4},
    "shuffle": {"maxInFlight": 2, "maxPending": 4},
    "locator": {"maxInFlight": 4, "maxPending": 8},
    "hydration": {"maxInFlight": 1, "maxPending": 2},
    "fragmentWorkerMaxInFlight": 2
  }]
}
```

Compute lowercase SHA-256 values after writing the files, create namespaced Secrets with the exact keys expected by the chart, and put those Secret names/checksums in the site values. Keep credentials out of Helm command-line arguments and source control.

## 5. Lint, render, and server-dry-run

The generated image file must be the last values file so it wins over site placeholders:

```bash
helm lint NGKG_1_0_0_GA/charts/ngkg-crds
helm lint NGKG_1_0_0_GA/charts/ngkg-platform \
  -f .ngkg/platform-values.yaml \
  -f docker_repos/generated/platform-local-registry-values.yaml

helm template ngkg-platform NGKG_1_0_0_GA/charts/ngkg-platform \
  --namespace "$NGKG_NAMESPACE" \
  --kube-version "$(kubectl version -o json | jq -r '.serverVersion.gitVersion | ltrimstr("v")')" \
  -f .ngkg/platform-values.yaml \
  -f docker_repos/generated/platform-local-registry-values.yaml \
  > .ngkg/platform-rendered.yaml

kubectl apply --dry-run=server -f .ngkg/platform-rendered.yaml
```

Repeat lint/template/server-dry-run for workloads and agents before installing them. A default workload render requires Kueue CRDs. Enabled KEDA, MPIJob, ServiceMonitor, or HTTPRoute resources require their corresponding CRDs to exist before server-side validation.

## 6. Install the upload-capable control plane

Install CRDs first. Keep the generated image file last:

```bash
helm upgrade --install ngkg-crds NGKG_1_0_0_GA/charts/ngkg-crds \
  --namespace "$NGKG_NAMESPACE" --atomic --wait --timeout 10m

helm upgrade --install ngkg-platform NGKG_1_0_0_GA/charts/ngkg-platform \
  --namespace "$NGKG_NAMESPACE" \
  -f .ngkg/platform-values.yaml \
  -f docker_repos/generated/platform-local-registry-values.yaml \
  --atomic --wait --wait-for-jobs --timeout 30m

kubectl -n "$NGKG_NAMESPACE" get pods -o wide
kubectl -n "$NGKG_NAMESPACE" get events --sort-by=.lastTimestamp
kubectl -n "$NGKG_NAMESPACE" rollout status deployment/ngkg-api
```

Every pod must be Ready and every `imageID` must match the generated digest lock before API testing.

## 7. Test Swagger and the direct TriG upload route

```bash
kubectl -n "$NGKG_NAMESPACE" port-forward service/ngkg-api 8080:80
export NGKG_API_URL=http://127.0.0.1:8080
curl --fail-with-body "$NGKG_API_URL/health/ready"
curl --fail-with-body "$NGKG_API_URL/openapi.yaml" -o .ngkg/control-openapi.yaml
```

Open `http://127.0.0.1:8080/docs`. Then use the dataset and upload commands in the quickstart. The upload request must use `Content-Type: application/trig; charset=utf-8` and `X-NGKG-Content-SHA256` for the exact bytes. The input is expected to have already passed the upstream alignment and OWL-certification automation. Treat `201` as confirmation that the database accepted and immutably stored the certified TriG source; ingestion and snapshot activation remain explicit database operations.

## 8. Ingest and publish the certified TriG as a queryable snapshot

The source upload and ingestion routes are intentionally separate. For the checked corpus, stage the already-aligned and certified input bundle with `NGKG_1_0_0_GA/scripts/stage_reference_bundle.py` using the exact example in `NGKG_1_0_0_GA/README.md`. The script produces a local content-addressed tree and prints `bundleObjectKey` and `bundleSha256`; for a cluster object store, upload that tree below the configured `artifactStore.baseUrl` without changing relative keys. NGKG then runs its existing RDF partitioning, indexing, snapshot-integrity and distributed-query preparation logic; it does not replace the upstream alignment or certification automation.

Create the explicit dataset identity used in the bundle, then start ingestion:

```bash
export NGKG_DATASET_ID=<bundle-dataset-uuid>
export NGKG_DATASET_NAMESPACE=<bundle-dataset-namespace-uuid>
export NGKG_TARGET_SNAPSHOT_ID=<bundle-target-snapshot-uuid>
export NGKG_BUNDLE_OBJECT_KEY=<printed-bundle-object-key>
export NGKG_BUNDLE_SHA256=<printed-bundle-sha256>

curl --fail-with-body -X PUT "$NGKG_API_URL/v1/datasets/$NGKG_DATASET_ID" \
  -H "Authorization: Bearer $NGKG_BEARER_TOKEN" \
  -H 'Content-Type: application/json' \
  --data "{\"identityNamespace\":\"$NGKG_DATASET_NAMESPACE\",\"policyVersion\":\"urn:ngkg:projection-policy:reference-v1\"}"

export NGKG_IDEMPOTENCY_KEY="staging-ingestion-$(uuidgen)"
INGESTION_RESPONSE="$(curl --fail-with-body --silent --show-error \
  -X POST "$NGKG_API_URL/v1/datasets/$NGKG_DATASET_ID/ingestions" \
  -H "Authorization: Bearer $NGKG_BEARER_TOKEN" \
  -H "Idempotency-Key: $NGKG_IDEMPOTENCY_KEY" \
  -H 'Content-Type: application/json' \
  --data "{\"bundleObjectKey\":\"$NGKG_BUNDLE_OBJECT_KEY\",\"bundleSha256\":\"$NGKG_BUNDLE_SHA256\",\"parentSnapshotId\":null,\"targetSnapshotId\":\"$NGKG_TARGET_SNAPSHOT_ID\",\"publicationPolicy\":\"automatic-after-certification\",\"resourceProfile\":\"reference-balanced\"}")"
export NGKG_OPERATION_ID="$(printf '%s' "$INGESTION_RESPONSE" | jq -r .operationId)"
printf '%s\n' "$INGESTION_RESPONSE" | jq .
```

Poll `GET /v1/jobs/$NGKG_OPERATION_ID` until it is `PUBLISHED`, or stop and inspect the error artifact on `FAILED`. The API's existing `publicationPolicy` enum retains its compatibility name, but this runbook assumes the input was certified upstream. Do not query the online plane until NGKG has completed ingestion and activated the published snapshot.

## 9. Install and test the online SPARQL plane

Label/taint capacity exactly as the workload chart requires, create the workload Secrets, and install Kueue before the chart. Then lint, server-dry-run, and install with the generated values last:

```bash
helm upgrade --install ngkg-workloads NGKG_1_0_0_GA/charts/ngkg-workloads \
  --namespace "$NGKG_NAMESPACE" \
  -f .ngkg/workloads-values.yaml \
  -f docker_repos/generated/workloads-local-registry-values.yaml \
  --atomic --wait --timeout 30m

kubectl -n "$NGKG_NAMESPACE" port-forward service/ngkg-query 32010:32010
export NGKG_QUERY_URL=http://127.0.0.1:32010
curl --fail-with-body "$NGKG_QUERY_URL/health/ready"
curl --fail-with-body "$NGKG_QUERY_URL/openapi.yaml" -o .ngkg/online-openapi.yaml
```

Open `http://127.0.0.1:32010/docs`. Execute a standards-shaped SPARQL POST:

```bash
curl --fail-with-body --silent --show-error \
  -X POST "$NGKG_QUERY_URL/v1/datasets/$NGKG_DATASET_ID/sparql" \
  -H "Authorization: Bearer $NGKG_BEARER_TOKEN" \
  -H 'Content-Type: application/sparql-query; charset=utf-8' \
  -H 'Accept: application/sparql-results+json' \
  --data-binary @NGKG_1_0_0_GA/test-corpus/queries/q01-cross-domain.rq | jq .
```

The test passes only if the response is HTTP 200, identifies the published snapshot, matches the checked expected result, and the corresponding query log is finalized. A route existing in Swagger is not evidence that compilation, OWL qualification, authorization, distributed execution, or hydration is correct.

The repository also includes an executable database-boundary smoke test. It performs no alignment or OWL certification:

```bash
python3 scripts/database_api_smoke_test.py \
  --control-url "$NGKG_API_URL" \
  --online-url "$NGKG_QUERY_URL" \
  --token "$NGKG_BEARER_TOKEN" \
  --dataset-id "$NGKG_DATASET_ID" \
  --query-published-dataset
```

## Stop conditions

Stop immediately on a Rust warning/error, mutable or missing image digest, registry pull failure, Helm schema/render error, unknown CRD, failed migration hook, non-Ready dependency, failed checksum, unpublished snapshot, cross-tenant visibility, or SPARQL result mismatch. Preserve rendered manifests, pod events/logs, operation JSON, object checksums, and the image lock for diagnosis.
