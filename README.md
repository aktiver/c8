# c8 (NGKG)

c8, implemented in the code as NGKG, is an ontology-native distributed RDF database for governed knowledge graphs. It accepts RDF 1.1 TriG, compiles named graphs and OWL 2 DL semantics into immutable, checksum-bound snapshots, and serves exact SPARQL 1.1 queries through a horizontally scalable Kubernetes data plane. PostgreSQL stores durable control metadata, object storage holds immutable bulk artifacts, Rust services perform compilation and query execution, and a pinned Java/HermiT adapter supplies the exact OWL reasoning boundary. The system is deliberately fail-closed: an unauthorized graph, stale snapshot, missing partition, invalid checksum, or incomplete proof cannot be returned as a successful answer.

This repository is a **1.0.0 GA source candidate**, not a published production distribution. Its static gates pass, but the checked-in [build status](dev_docs/BUILD_STATUS_1_0_0_GA.md) records that live multi-provider qualification, native image publication, security scans, signatures, and a publishable GA certificate are still external release requirements. Build and qualify the exact images and configuration you deploy.

- [Project documentation](https://aktiver-team.github.io/c8/)
- [Quickstart](QUICKSTART.md)
- [Control-plane OpenAPI](api/openapi.yaml)
- [Online OpenAPI](api/online-openapi.yaml)
- [GA operations guide](docs/GA_RELEASE_AND_OPERATIONS.md)
- [Apache 2.0 license](LICENSE)

## Architecture and repository map

```text
TriG upload/cloud import
        |
control API -> PostgreSQL catalog -> operators -> batch/Indexed Jobs
        |                                      |
        +------------------------------> immutable object artifacts
                                               |
client -> query service -> fragment workers -> locator -> Parquet hydration
                              |
                              +-> exact HermiT reasoning when required
```

| Path | Responsibility |
| --- | --- |
| `services/api` | Dataset, source, ingestion, snapshot, and recovery API |
| `services/operator` | Reference compilation and source-import reconciliation |
| `services/distributed-operator` | Distributed compilation and artifact orchestration |
| `services/online-serving` | Query, fragment, locator, and hydration process roles |
| `services/direct-reasoner-worker` | Online exact OWL Direct-Semantics worker |
| `services/reference-worker` | Reference compiler/query slice with HermiT |
| `services/storage-recovery-*` | Replication, backup, restore, repair, and relocation |
| `crates/` | Catalog, RDF identity, compiler, planner, execution, cache, and storage libraries |
| `adapters/hermit-reasoner` | Java OWLAPI/HermiT boundary |
| `contracts/` | Closed schemas for plans, manifests, policies, proofs, and certificates |
| `charts/` | CRD, platform, and workload Helm charts |
| `scripts/`, `acceptance/`, `verification/` | Validation, conformance, qualification, and release evidence |

Durable truth lives in PostgreSQL and immutable object storage; pods and local caches/spill are disposable. Public control traffic goes to `ngkg-api`, and public query traffic goes to `ngkg-query`. Fragment, shuffle, locator, hydration, and reasoner services are internal.

## Local development

The pinned toolchain is Rust 1.97.1 with edition 2024. A full build also needs Maven/JDK, Python 3, Helm, and Kubernetes tooling.

```bash
git clone https://github.com/aktiver-team/c8.git
cd c8
rustup show
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
python3 scripts/structural_validate.py --root .
mvn --batch-mode --no-transfer-progress -f adapters/hermit-reasoner/pom.xml clean test package
```

Some qualification scripts require packages in `conformance/python-requirements.lock`, live PostgreSQL and object storage, Docker, Helm, or a real cluster. A missing external dependency is not a passing release gate.

## Deploy to an existing HA Kubernetes cluster

The charts declare Kubernetes `>=1.33`; production support is determined by the live-qualified version matrix for your release. Use a currently supported release and test the exact c8 images and values you deploy.

### 1. Prepare the cluster

Use at least three workers across two or more failure zones. The cluster must already provide:

- HA PostgreSQL and supported object storage reachable from the c8 namespace;
- a registry containing digest-pinned c8 images;
- network-policy-capable CNI, DNS, CSI, and a `ReadWriteMany` storage class;
- Metrics Server (`metrics.k8s.io`) and a Prometheus custom-metrics adapter (`custom.metrics.k8s.io`);
- KEDA for hydration scaling and Kueue for batch admission;
- exactly one provider/node autoscaler configured for scale-from-zero pools;
- Gateway/Ingress and service mesh or equivalent TLS.

The charts do not install those dependencies. Check them first:

```bash
kubectl get nodes -L topology.kubernetes.io/zone
kubectl get apiservice v1beta1.metrics.k8s.io
kubectl get apiservice v1beta1.custom.metrics.k8s.io
kubectl api-resources | grep -E 'scaledobjects|clusterqueues|localqueues'
```

Configure autoscaled pools with these `ngkg.io/workload` labels and matching `NoSchedule` taints where the templates require them:

```text
source-ingestion              semantic-projection
semantic-artifact-build       index-build
reasoning                     storage-recovery
sparql-query-processing       sparql-fragment-processing
parquet-hydration             online-reasoning
```

The defaults are HPC-sized: query and fragment pods request 16 CPU, 64 GiB memory, and hundreds of GiB of ephemeral storage. Review `charts/ngkg-workloads/values.yaml` against actual node shapes. Keep requests equal to limits for Guaranteed QoS and ensure ephemeral requests cover all configured caches, spools, and spills.

### 2. Build and publish images

No production image coordinates are bundled. The build script requires immutable builder/runtime images whose caches support its network-disabled builds.

```bash
export NGKG_RUST_BUILDER_IMAGE='<registry>/rust-builder@sha256:<digest>'
export NGKG_RUNTIME_IMAGE='<registry>/runtime@sha256:<digest>'
export NGKG_MAVEN_BUILDER_IMAGE='<registry>/maven-builder@sha256:<digest>'
export NGKG_JAVA_RUNTIME_IMAGE='<registry>/java-runtime@sha256:<digest>'
export NGKG_IMAGE_REGISTRY='<registry>/<project>'
export NGKG_IMAGE_TAG="$(git rev-parse HEAD)"
./scripts/build_images.sh
```

Push all generated tags, record registry-reported digests, and put `repository` plus `sha256:...` in Helm values. Query, fragment, locator, and hydration use the same `ngkg-online-serving` image. Keep `hpc.enabled: false` unless you separately build and qualify an MPI-capable image containing an MPI runtime, `ngkg-mpi-exec`, and `ngkg-distributed-worker`.

```bash
sha256sum adapters/hermit-reasoner/target/ngkg-hermit-adapter.jar
```

### 3. Create identities and Secrets

```bash
export NGKG_NAMESPACE=ngkg
export NGKG_TENANT_ID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
export NGKG_BEARER_TOKEN="$(openssl rand -hex 32)"
export NGKG_TOKEN_SHA256="$(printf %s "$NGKG_BEARER_TOKEN" | openssl dgst -sha256 -r | awk '{print $1}')"
export NGKG_REASONER_TOKEN="$(openssl rand -hex 32)"
export NGKG_REASONER_TOKEN_SHA256="$(printf %s "$NGKG_REASONER_TOKEN" | openssl dgst -sha256 -r | awk '{print $1}')"
mkdir -p .ngkg
kubectl create namespace "$NGKG_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
```

Create `.ngkg/tokens.json`, replacing placeholders with the exported values:

```json
{
  "formatVersion": 1,
  "tokens": [{
    "tokenSha256": "<NGKG_TOKEN_SHA256>",
    "tenantId": "<NGKG_TENANT_ID>",
    "principalId": "platform-admin",
    "scopes": ["datasets:write", "sources:write", "ingestions:create", "imports:create", "imports:read", "jobs:read", "jobs:cancel", "snapshots:read", "snapshots:publish", "storage:read", "storage:write", "storage:restore", "queries:execute", "query-logs:read", "query-logs:read:text"],
    "graphAuthorizationLabels": ["domain:production"]
  }]
}
```

Create `.ngkg/tenant-policy.json`. Its tenant set must exactly match the tenants with `queries:execute`:

```json
{
  "formatVersion": 1,
  "tenants": [{
    "tenantId": "<NGKG_TENANT_ID>",
    "query": {"maxInFlight": 12, "maxPending": 24},
    "fragment": {"maxInFlight": 8, "maxPending": 16},
    "shuffle": {"maxInFlight": 16, "maxPending": 32},
    "locator": {"maxInFlight": 128, "maxPending": 256},
    "hydration": {"maxInFlight": 2, "maxPending": 8},
    "fragmentWorkerMaxInFlight": 16
  }]
}
```

After replacing the placeholders:

```bash
export NGKG_AUTH_FILE_SHA256="$(openssl dgst -sha256 -r .ngkg/tokens.json | awk '{print $1}')"
export NGKG_POLICY_SHA256="$(openssl dgst -sha256 -r .ngkg/tenant-policy.json | awk '{print $1}')"
kubectl -n "$NGKG_NAMESPACE" create secret generic ngkg-auth --from-file=tokens.json=.ngkg/tokens.json --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NGKG_NAMESPACE" create secret generic ngkg-tenant-admission --from-file=tenant-policy.json=.ngkg/tenant-policy.json --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NGKG_NAMESPACE" create secret generic ngkg-database \
  --from-literal=database-url='postgresql://ngkg_runtime:<password>@<host>:5432/ngkg?sslmode=require' \
  --from-literal=migration-database-url='postgresql://ngkg_migrator:<password>@<host>:5432/ngkg?sslmode=require' \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NGKG_NAMESPACE" create secret generic ngkg-reasoner-token \
  --from-literal=token="$NGKG_REASONER_TOKEN" --from-literal=token-sha256="$NGKG_REASONER_TOKEN_SHA256" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Also provision `ngkg-internal-tls` through your TLS/service-mesh system and an existing `ReadWriteMany` PVC named `ngkg-reasoner-workspace`. Prefer workload identity; otherwise create narrowly scoped registry/object-store Secrets.

### 4. Configure Helm

Create `.ngkg/platform-values.yaml`:

```yaml
images:
  api: {repository: <registry>/ngkg-api, digest: sha256:<digest>}
  operator: {repository: <registry>/ngkg-operator, digest: sha256:<digest>}
  distributedOperator: {repository: <registry>/ngkg-distributed-operator, digest: sha256:<digest>}
  distributedWorker: {repository: <registry>/ngkg-distributed-worker, digest: sha256:<digest>}
  hpcWorker: {repository: <registry>/ngkg-distributed-worker, digest: sha256:<digest>}
  referenceWorker: {repository: <registry>/ngkg-reference-worker, digest: sha256:<digest>}
  catalogMigrator: {repository: <registry>/ngkg-catalog-migrator, digest: sha256:<digest>}
  storageRecoveryOperator: {repository: <registry>/ngkg-storage-recovery-operator, digest: sha256:<digest>}
  storageRecoveryWorker: {repository: <registry>/ngkg-storage-recovery-worker, digest: sha256:<digest>}
dependencies: {databaseSecret: ngkg-database, authTokensSecret: ngkg-auth, objectStoreCredentialsSecret: ''}
artifactStore: {baseUrl: s3://<bucket>/ngkg}
api:
  replicas: 3
  authTokensFileSha256: <NGKG_AUTH_FILE_SHA256>
  autoscaling: {enabled: true, minReplicas: 3, maxReplicas: 12, cpuUtilizationTargetPercent: 80, memoryUtilizationTargetPercent: 80}
operator:
  reference: {reasonerAdapterSha256: <hermit-jar-sha256>}
hpc: {enabled: false}
storageRecovery:
  primaryTarget: primary-zone-a
  targetsJson: >-
    {"formatVersion":1,"targets":[{"name":"primary-zone-a","failureDomain":"zone-a","baseUrl":"s3://<primary>/ngkg","writable":true},{"name":"replica-zone-b","failureDomain":"zone-b","baseUrl":"s3://<replica>/ngkg","writable":true},{"name":"backup-region","failureDomain":"backup-region","baseUrl":"s3://<backup>/ngkg","writable":true}]}
```

Create `.ngkg/workloads-values.yaml`:

```yaml
images:
  query: {repository: <registry>/ngkg-online-serving, digest: sha256:<digest>}
  fragment: {repository: <registry>/ngkg-online-serving, digest: sha256:<digest>}
  locator: {repository: <registry>/ngkg-online-serving, digest: sha256:<digest>}
  hydration: {repository: <registry>/ngkg-online-serving, digest: sha256:<digest>}
  reasoner: {repository: <registry>/ngkg-direct-reasoner-worker, digest: sha256:<digest>}
tls: {existingSecret: ngkg-internal-tls}
onlineServing:
  databaseSecret: ngkg-database
  authTokensSecret: ngkg-auth
  authTokensFileSha256: <NGKG_AUTH_FILE_SHA256>
  tenantAdmissionSecret: ngkg-tenant-admission
  tenantAdmissionPolicySha256: <NGKG_POLICY_SHA256>
  objectStoreCredentialsSecret: ''
  artifactStoreBaseUrl: s3://<bucket>/ngkg
  nativeCutoverMode: shadow # use required only after workload-specific cutover qualification
onlineReasoning:
  enabled: true
  sharedWorkspaceClaim: ngkg-reasoner-workspace
  sharedTokenSecret: ngkg-reasoner-token
  adapterSha256: <hermit-jar-sha256>
networking:
  dependencyCidrs: [<postgres-and-object-store-cidr>]
  queryClientNamespace: ngkg
  metricsNamespace: monitoring
```

Select one provider overlay:

| Cluster | Overlay in `charts/ngkg-workloads/profiles/` |
| --- | --- |
| Generic/on-prem | `phase40.13.20-generic.yaml` |
| RKE / RKE2 | `phase40.13.20-rke.yaml` / `phase40.13.20-rke2.yaml` |
| EKS / AKS / GKE | `phase40.13.20-eks.yaml` / `phase40.13.20-aks.yaml` / `phase40.13.20-gke.yaml` |

### 5. Validate and install

```bash
export NGKG_PROVIDER_VALUES=charts/ngkg-workloads/profiles/phase40.13.20-eks.yaml
export NGKG_PRODUCTION_VALUES=charts/ngkg-workloads/profiles/phase40.13.20-production.yaml

python3 scripts/validate_platform_values.py charts/ngkg-platform/values.yaml
python3 scripts/validate_helm_values.py charts/ngkg-workloads/values.yaml \
  --overlay "$NGKG_PROVIDER_VALUES" --overlay "$NGKG_PRODUCTION_VALUES" --overlay .ngkg/workloads-values.yaml
helm lint charts/ngkg-crds
helm lint charts/ngkg-platform -f .ngkg/platform-values.yaml
helm lint charts/ngkg-workloads -f "$NGKG_PROVIDER_VALUES" -f "$NGKG_PRODUCTION_VALUES" -f .ngkg/workloads-values.yaml

helm upgrade --install ngkg-crds charts/ngkg-crds -n "$NGKG_NAMESPACE" --wait --atomic --timeout 10m
helm upgrade --install ngkg-platform charts/ngkg-platform -n "$NGKG_NAMESPACE" -f .ngkg/platform-values.yaml --wait --atomic --timeout 30m
helm upgrade --install ngkg-workloads charts/ngkg-workloads -n "$NGKG_NAMESPACE" \
  -f "$NGKG_PROVIDER_VALUES" -f "$NGKG_PRODUCTION_VALUES" -f .ngkg/workloads-values.yaml \
  --wait --atomic --timeout 30m
```

The charts expose ClusterIP services only. Route the authenticated control endpoint to `ngkg-api:80` and SPARQL/query endpoint to `ngkg-query:32010`. Do not expose worker services.

### 6. Verify HA and scaling

```bash
kubectl -n "$NGKG_NAMESPACE" get deploy,statefulset,pod -o wide
kubectl -n "$NGKG_NAMESPACE" get hpa,scaledobject,pdb
kubectl -n "$NGKG_NAMESPACE" get ngkgcompilations,ngkgsourceimports,ngkgstoragerecoveries
kubectl -n "$NGKG_NAMESPACE" top pod
kubectl -n "$NGKG_NAMESPACE" describe hpa ngkg-api
kubectl -n "$NGKG_NAMESPACE" describe hpa ngkg-query-shard
kubectl -n "$NGKG_NAMESPACE" describe hpa ngkg-fragment-worker
```

The API starts at three replicas and scales to twelve at 80% CPU **or** memory. Query, fragment, hydration, and reasoner roles scale independently; finite batch pools scale from zero through operator work, Kueue, and the provider autoscaler. An HPA only creates pod demand—it cannot add nodes unless the provider autoscaler recognizes the role labels and taints. Test zone loss, node drain, database failover, storage failure, scale from zero, scale down with spill/checkpoint state, and exact-result equivalence before production approval.

The current chart does not make every control component active-active. `ngkg-operator` and `ngkg-distributed-operator` default to one replica, and the platform chart does not install an API PodDisruptionBudget. Their durable catalog/CRD state makes restart recovery possible, but they can pause reconciliation during a pod or node outage. Do not increase operator replicas unless leader election has been implemented and qualified; add an API PDB in your platform policy layer if uninterrupted voluntary maintenance is required.

## REST API route map

Protected routes use `Authorization: Bearer <token>`. Control operations require the listed scope. Online queries require `queries:execute` and matching graph labels. Query-log auditors may use `query-logs:read`; `query-logs:read:text` permits another principal's query text. Restrict application-public health/docs/metrics paths at the Gateway as needed.

### Control plane — `ngkg-api:80`

| Method | Route | Scope | Purpose |
| --- | --- | --- | --- |
| `GET` | `/health/live`, `/health/ready` | Public | Liveness and dependency readiness |
| `GET` | `/docs`, `/openapi.yaml`, `/openapi.json` | Public | Swagger UI and control API contracts |
| `POST` | `/v1/datasets` | `datasets:write` | Create/return a dataset by readable name |
| `PUT` | `/v1/datasets/{datasetId}` | `datasets:write` | Idempotently create/verify a chosen dataset UUID |
| `PUT` | `/v1/datasets/{datasetId}/sources/{sourceId}` | `sources:write` | Upload and validate immutable `application/trig` |
| `POST` | `/v1/datasets/{datasetId}/ingestions` | `ingestions:create` | Start durable compilation with an `Idempotency-Key` |
| `POST` | `/v1/datasets/{datasetName}/imports` | `imports:create` | Import existing cloud TriG objects by dataset name |
| `GET` | `/v1/datasets/{datasetName}/imports/{operationId}` | `imports:read` | Read cloud-import status |
| `POST` | `/v1/datasets/by-id/{datasetId}/imports` | `imports:create` | Import cloud objects by dataset UUID |
| `GET` | `/v1/jobs/{operationId}` | `jobs:read` | Read compilation state |
| `POST` | `/v1/jobs/{operationId}/cancel` | `jobs:cancel` | Cancel compilation |
| `GET` | `/v1/datasets/{datasetId}/snapshots/{snapshotId}` | `snapshots:read` | Read immutable snapshot/qualification metadata |
| `POST` | `/v1/datasets/{datasetId}/snapshots/{snapshotId}/publish` | `snapshots:publish` | Atomically activate a qualified snapshot |
| `POST` | `/v1/datasets/{datasetName}/snapshots/{snapshotId}/storage-operations` | `storage:write` | Replicate, relocate, repair, or back up a snapshot |
| `POST` | `/v1/datasets/{datasetName}/restores` | `storage:restore` | Restore a checksum-bound backup into inactive storage |
| `GET` | `/v1/storage-operations/{operationId}` | `storage:read` | Read recovery operation status |

Upload accepts only UTF-8 RDF 1.1 TriG and requires `Content-Type: application/trig` and `X-NGKG-Content-SHA256`.

### Query plane — `ngkg-query:32010`

| Method | Route | Access | Purpose |
| --- | --- | --- | --- |
| `GET` | `/health/live`, `/health/ready`, `/metrics` | Public | Health and Prometheus metrics |
| `GET` | `/docs`, `/openapi.yaml`, `/openapi.json` | Public | Swagger UI and online contracts |
| `GET` | `/v1/hpc/capabilities` | `queries:execute` | Inspect bounded query-pod capabilities |
| `GET` | `/v1/query_logs` | Owner or `query-logs:read` | List tenant query executions |
| `GET` | `/v1/query_logs/{queryExecutionId}` | Owner or `query-logs:read` | Read one execution's resources/outcome/evidence |
| `GET` | `/v1/datasets/{datasetId}/sparql` | Query + graph access | SPARQL Protocol GET (`query` parameter) |
| `POST` | `/v1/datasets/{datasetId}/sparql` | Query + graph access | SPARQL Protocol POST |
| `POST` | `/v1/datasets/{datasetId}/query` | Query + graph access | c8 JSON query API with optional hydration |
| `GET` | `/v1/datasets/{datasetId}/sparql/service-description` | Query + graph access | Authenticated SPARQL service description |
| `POST` | `/v1/datasets/{datasetId}/sparql/direct/validate` | Query + graph access | Validate OWL Direct BGP legality without execution |
| `POST` | `/v1/datasets/{datasetId}/sparql/direct/route` | Query + graph access | Resolve the fail-closed exact-reasoning route |

### Internal data plane — never expose publicly

| Role | Method and route | Purpose |
| --- | --- | --- |
| Fragment | `POST /v1/datasets/{datasetId}/fragments/{querySha256}/{fragmentId}/execute` | Execute a certified graph fragment |
| Fragment | `POST /v1/datasets/{datasetId}/shuffles/{querySha256}/{stage}/{partition}/join` | Execute a hash-owned join partition |
| Fragment | `POST /v1/datasets/{datasetId}/algebra/{querySha256}/{replica}/execute` | Execute a complete-algebra replica |
| Fragment | `POST /v1/datasets/{datasetId}/paths/{querySha256}/{pathId}/{iteration}/{partition}/expand` | Expand a property-path partition |
| Fragment | `POST /v1/datasets/{datasetId}/native/leaves/{querySha256}/{partition}/scan` | Scan semantic Parquet |
| Locator | `POST /v1/datasets/{datasetId}/locate` | Resolve GUIDs through the mmap locator |
| Hydration | `POST /v1/datasets/{datasetId}/hydrate` | Hydrate GUIDs from Parquet row groups |

Use `/sparql` or `/query` from applications. Full bodies, media types, headers, responses, and examples are in [the control specification](api/openapi.yaml) and [online specification](api/online-openapi.yaml).

## First API calls

```bash
kubectl -n "$NGKG_NAMESPACE" port-forward service/ngkg-api 8080:80
kubectl -n "$NGKG_NAMESPACE" port-forward service/ngkg-query 8081:32010

curl --fail-with-body http://127.0.0.1:8080/v1/datasets \
  -H "Authorization: Bearer $NGKG_BEARER_TOKEN" -H 'Content-Type: application/json' \
  --data '{"name":"supply_chain"}'

curl --fail-with-body http://127.0.0.1:8081/v1/datasets/<dataset-uuid>/sparql \
  -H "Authorization: Bearer $NGKG_BEARER_TOKEN" \
  -H 'Content-Type: application/sparql-query' -H 'Accept: application/sparql-results+json' \
  --data 'SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10'
```

The query requires an uploaded, compiled, qualified, and published snapshot. See [QUICKSTART.md](QUICKSTART.md) for the first-TriG flow.

## Operations

- Upgrade in order: CRDs, platform/migrations, then workloads. Back up catalog and object roots first.
- Keep the old snapshot available until its replacement is qualified and atomically published.
- Monitor admission rejections, queues, saturation, scale lag, placement, spill/checkpoint pressure, reasoning load, storage errors, and checksum failures.
- Preserve exact values, image digests, snapshot IDs, logs, metrics, events, and certificate checksums.
- Follow `release/1.0.0/ACCEPTANCE_TEST_PLAN.md`; production publication requires `scripts/assess_ga_readiness.py --require-publishable` to pass.

## License

Licensed under the [Apache License 2.0](LICENSE).
