# NGKG quickstart: HA Kubernetes deployment and first TriG upload

This guide covers only the shortest supported path to:

1. prepare an HA RKE/RKE2, EKS, AKS, or GKE cluster;
2. deploy the NGKG control plane with three API replicas;
3. autoscale when either CPU or memory reaches 80%; and
4. create a dataset and upload one UTF-8 RDF 1.1 TriG file.

It does not qualify a production release or explain every advanced compiler, reasoner, query, recovery, federation, or benchmark setting.

## Important file limitation

The source-upload API accepts only `application/trig`. It does not accept Turtle, RDF/XML, OWL files, JSON, CSV, Parquet, PDFs, Word documents, ZIP files, or arbitrary attachments. Those files return HTTP `415` on the source-upload route.

TriG files may contain a default graph, but NGKG requires at least one named graph whose name is an absolute IRI. Blank-node graph names are rejected. The normal graph names are:

```text
https://c8-next-generation.io/<scope>/<subdomain>/semkg
https://c8-next-generation.io/<scope>/<subdomain>/closure
https://c8-next-generation.io/<scope>/<subdomain>/provenance
```

Ontology, policy, expected-result, and other compiler-support files belong in a checksum-bound compilation bundle or operator-controlled object storage. The direct upload route does not silently convert arbitrary files into RDF.

## 1. Prerequisites

Install these locally:

- `kubectl`
- Helm 3 or Helm 4
- `curl`
- `jq`
- `openssl`
- `uuidgen`

You also need:

- an existing Kubernetes cluster on a currently supported release (use 1.35 or newer; the charts retain 1.33+ compatibility, but 1.33 is end-of-life and 1.34 is no longer one of the three actively maintained release branches);
- an HA PostgreSQL database reachable from the cluster;
- an S3 or S3-compatible bucket for immutable NGKG artifacts;
- digest-pinned NGKG container images built from this source tree; and
- the SHA-256 digest of the HermiT adapter JAR used by the reference worker.

The archive contains source code and Dockerfiles, not published production image coordinates. Replace every `<repository>` and `<sha256-digest>` below with artifacts you built and pushed.

## 2. Prepare the Kubernetes provider

Use at least three worker nodes across two or more zones when zones are available. For the upload-only control plane, start with at least 8 vCPU, 32 GiB RAM, and 250 GiB ephemeral storage per worker. Larger compilation and reasoning jobs require the HPC pools described by the full operator documentation.

Install Metrics Server before NGKG. The API HPA uses Kubernetes resource metrics. Kueue, KEDA, Prometheus/custom metrics, and the workload chart are not required for the first direct upload; install them later when you enable compilation, reasoning, and the distributed query data plane.

| Platform | Required cluster configuration | Node autoscaler | TriG bucket identity |
| --- | --- | --- | --- |
| RKE/RKE2 | Three workers, topology labels, a network-policy CNI, CSI storage, and Metrics Server | Externally managed Rancher-compatible Cluster Autoscaler | OIDC, Vault/external-secrets, or restricted S3-compatible credentials |
| EKS | Multi-AZ managed node group, Metrics Server, and an S3-compatible artifact path | Karpenter; workload profile `charts/ngkg-workloads/profiles/phase40.13.20-eks.yaml` | EKS Pod Identity or IRSA; S3 CSI/object API for bucket imports |
| AKS | Availability-zone node pool, Azure Workload Identity, and Metrics Server | AKS managed Cluster Autoscaler; workload profile `phase40.13.20-aks.yaml` | Azure Workload Identity and Blob CSI for bucket imports; use reachable S3-compatible storage for the current artifact store |
| GKE | Regional or multi-zonal node pool, GKE Workload Identity, and Metrics Server | GKE managed Cluster Autoscaler; workload profile `phase40.13.20-gke.yaml` | GKE Workload Identity and GCS Fuse CSI for bucket imports; use reachable S3-compatible storage for the current artifact store |

The autoscaler itself is provider-owned and is not installed by the NGKG chart. NGKG creates pod demand; the provider autoscaler adds nodes.

Confirm the minimum services:

```bash
kubectl get nodes -L topology.kubernetes.io/zone
kubectl get apiservice v1beta1.metrics.k8s.io
```

## 3. Set local quickstart variables

Run from the extracted repository root:

```bash
export NGKG_NAMESPACE=ngkg
export NGKG_DATASET_NAME=first_graph
export NGKG_POLICY_VERSION=quickstart-v1
export NGKG_TRIG_FILE=/absolute/path/to/your-file.trig

export NGKG_TENANT_ID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
export NGKG_BEARER_TOKEN="$(openssl rand -hex 32)"
export NGKG_TOKEN_SHA256="$(printf '%s' "$NGKG_BEARER_TOKEN" | openssl dgst -sha256 -r | awk '{print $1}')"
```

Keep `NGKG_BEARER_TOKEN` private. Only its SHA-256 digest is stored in `tokens.json`.

## 4. Create authentication and database Secrets

Create a private local directory. The supplied `.gitignore` excludes it.

```bash
mkdir -p .ngkg

cat > .ngkg/tokens.json <<JSON
{
  "formatVersion": 1,
  "tokens": [
    {
      "tokenSha256": "${NGKG_TOKEN_SHA256}",
      "tenantId": "${NGKG_TENANT_ID}",
      "principalId": "quickstart-admin",
      "scopes": ["datasets:write", "sources:write"]
    }
  ]
}
JSON

export NGKG_TOKENS_FILE_SHA256="$(openssl dgst -sha256 -r .ngkg/tokens.json | awk '{print $1}')"
```

Create `.ngkg/database.env` with the two PostgreSQL connections. The migration identity needs schema-migration permission; the runtime identity should be restricted to normal NGKG operations.

```dotenv
database-url=postgresql://ngkg_runtime:<password>@<postgres-host>:5432/ngkg?sslmode=require
migration-database-url=postgresql://ngkg_migrator:<password>@<postgres-host>:5432/ngkg?sslmode=require
```

Create the namespace and Secrets:

```bash
kubectl create namespace "$NGKG_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NGKG_NAMESPACE" create secret generic ngkg-auth \
  --from-file=tokens.json=.ngkg/tokens.json \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NGKG_NAMESPACE" create secret generic ngkg-database \
  --from-env-file=.ngkg/database.env \
  --dry-run=client -o yaml | kubectl apply -f -
```

If the image registry requires registry credentials, create a pull Secret in the same namespace. This is separate from the Docker login used to push images:

```bash
kubectl -n "$NGKG_NAMESPACE" create secret docker-registry ngkg-registry \
  --docker-server="$NGKG_LOCAL_REGISTRY" \
  --docker-username="$NGKG_LOCAL_REGISTRY_USERNAME" \
  --docker-password="$NGKG_LOCAL_REGISTRY_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -
```

If the API pod receives S3 credentials from workload identity, leave `objectStoreCredentialsSecret` empty. For a development S3-compatible service that requires explicit SDK environment variables, create a restricted Secret and set its name in the values file; never commit it.

## 5. Create the minimal Helm values file

Create `.ngkg/platform-values.yaml`:

```yaml
images:
  api: {repository: <api-image-repository>, digest: sha256:<sha256-digest>}
  operator: {repository: <operator-image-repository>, digest: sha256:<sha256-digest>}
  distributedOperator: {repository: <distributed-operator-image-repository>, digest: sha256:<sha256-digest>}
  distributedWorker: {repository: <distributed-worker-image-repository>, digest: sha256:<sha256-digest>}
  hpcWorker: {repository: <hpc-worker-image-repository>, digest: sha256:<sha256-digest>}
  referenceWorker: {repository: <reference-worker-image-repository>, digest: sha256:<sha256-digest>}
  catalogMigrator: {repository: <catalog-migrator-image-repository>, digest: sha256:<sha256-digest>}
  storageRecoveryOperator: {repository: <storage-recovery-operator-image-repository>, digest: sha256:<sha256-digest>}
  storageRecoveryWorker: {repository: <storage-recovery-worker-image-repository>, digest: sha256:<sha256-digest>}

# Leave empty for a public registry or provider-native registry authentication.
imagePullSecrets:
  - name: ngkg-registry

dependencies:
  databaseSecret: ngkg-database
  authTokensSecret: ngkg-auth
  objectStoreCredentialsSecret: ''

artifactStore:
  baseUrl: s3://your-ngkg-bucket/general

api:
  replicas: 3
  authTokensFileSha256: <tokens-json-sha256>
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 12
    cpuUtilizationTargetPercent: 80
    memoryUtilizationTargetPercent: 80
    scaleUpStabilizationSeconds: 30
    scaleUpPercent: 100
    scaleUpPeriodSeconds: 60
    scaleDownStabilizationSeconds: 600
    scaleDownPercent: 25
    scaleDownPeriodSeconds: 120

operator:
  reference:
    reasonerAdapterSha256: <hermit-adapter-jar-sha256>
```

The local image builder writes the same image block, including pull policies, to `docker_repos/generated/platform-local-registry-values.yaml`. Supply that generated file last so its digest-pinned image coordinates override the placeholders above:

```bash
helm lint NGKG_1_0_0_GA/charts/ngkg-platform \
  --values .ngkg/platform-values.yaml \
  --values docker_repos/generated/platform-local-registry-values.yaml
```

Insert the local checksum without exposing the bearer token:

```bash
sed -i.bak "s/<tokens-json-sha256>/${NGKG_TOKENS_FILE_SHA256}/" .ngkg/platform-values.yaml
```

The chart defaults already set each API pod to 4 CPU, 8 GiB RAM, and 128 GiB ephemeral storage. The HPA keeps at least three replicas and scales toward twelve when either average CPU or average memory reaches 80%.

## 6. Run every Helm command required for the upload path

The minimum upload deployment consists of Metrics Server, `ngkg-crds`, and `ngkg-platform`. RKE2 commonly supplies Metrics Server already. Check first:

```bash
kubectl get apiservice v1beta1.metrics.k8s.io
```

If that APIService does not exist, install an operator-approved, Kubernetes-compatible Metrics Server chart. Pin the reviewed chart version instead of silently taking the newest release:

```bash
export METRICS_SERVER_CHART_VERSION=<approved-chart-version>

helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
helm repo update metrics-server

helm upgrade --install metrics-server metrics-server/metrics-server \
  --namespace kube-system \
  --version "$METRICS_SERVER_CHART_VERSION" \
  --wait --atomic --timeout 10m

kubectl get apiservice v1beta1.metrics.k8s.io
kubectl top nodes
```

Do not add `--kubelet-insecure-tls` to make a broken cluster appear healthy. Configure trusted kubelet certificates instead.

Create and select the NGKG namespace, then validate the local charts after replacing every placeholder in `.ngkg/platform-values.yaml`:

```bash
kubectl create namespace "$NGKG_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl config set-context --current --namespace="$NGKG_NAMESPACE"

helm lint NGKG_1_0_0_GA/charts/ngkg-crds
helm lint NGKG_1_0_0_GA/charts/ngkg-platform \
  --values .ngkg/platform-values.yaml \
  --values docker_repos/generated/platform-local-registry-values.yaml
```

Install CRDs first, then the platform chart:

```bash
helm upgrade --install ngkg-crds NGKG_1_0_0_GA/charts/ngkg-crds \
  --namespace "$NGKG_NAMESPACE" \
  --wait --atomic --timeout 10m

helm upgrade --install ngkg-platform NGKG_1_0_0_GA/charts/ngkg-platform \
  --namespace "$NGKG_NAMESPACE" \
  --values .ngkg/platform-values.yaml \
  --values docker_repos/generated/platform-local-registry-values.yaml \
  --wait --atomic --timeout 20m
```

These are all Helm releases required to create a dataset and upload a TriG source. The provider node autoscaler is deliberately external to the NGKG charts: AKS and GKE use their managed autoscaler, EKS uses the preconfigured Karpenter path, and RKE/RKE2 uses the separately administered Rancher-compatible Cluster Autoscaler. Helm cannot create the required cloud node pools, IAM/workload identities, PostgreSQL service, or object bucket.

Confirm the releases:

```bash
helm list --namespace "$NGKG_NAMESPACE"
helm status ngkg-crds --namespace "$NGKG_NAMESPACE"
helm status ngkg-platform --namespace "$NGKG_NAMESPACE"
kubectl -n "$NGKG_NAMESPACE" get events --sort-by=.lastTimestamp
```

Check HA placement, readiness, and autoscaling:

```bash
kubectl -n "$NGKG_NAMESPACE" get pods -o wide
kubectl -n "$NGKG_NAMESPACE" get deployment ngkg-api
kubectl -n "$NGKG_NAMESPACE" get hpa ngkg-api
kubectl -n "$NGKG_NAMESPACE" rollout status deployment/ngkg-api
```

For a first upload, do not install `ngkg-workloads`. That chart requires five additional digest-pinned service images, tenant-admission and reasoner secrets, responsibility-specific labelled and tainted HPC node pools, Kueue, KEDA, Prometheus/custom metrics, and a provider overlay. Its production install is an advanced post-upload operation documented in `docs/phases/PHASE_40_13_20.md`.

## 7. Connect to the API

The quickstart uses a local port-forward. Keep this command running:

```bash
kubectl -n "$NGKG_NAMESPACE" port-forward service/ngkg-api 8080:80
```

In another terminal:

```bash
export NGKG_API_URL=http://127.0.0.1:8080
curl --fail-with-body "$NGKG_API_URL/health/ready"
```

Swagger is available at `http://127.0.0.1:8080/docs`.

## 8. Create a dataset

```bash
DATASET_RESPONSE="$(curl --fail-with-body --silent --show-error \
  -X POST "$NGKG_API_URL/v1/datasets" \
  -H "Authorization: Bearer $NGKG_BEARER_TOKEN" \
  -H 'Content-Type: application/json' \
  --data "{\"name\":\"$NGKG_DATASET_NAME\",\"policyVersion\":\"$NGKG_POLICY_VERSION\"}")"

printf '%s\n' "$DATASET_RESPONSE" | jq .
export NGKG_DATASET_ID="$(printf '%s' "$DATASET_RESPONSE" | jq -r .datasetId)"
```

Successful response:

```json
{
  "datasetId": "<dataset-uuid>",
  "datasetName": "first_graph",
  "identityNamespace": "<stable-identity-namespace-uuid>",
  "policyVersion": "quickstart-v1"
}
```

The route is idempotent for the same authenticated tenant, dataset name, and policy version.

## 9. Upload the TriG file

Compute the checksum and generate a source identity:

```bash
test -f "$NGKG_TRIG_FILE"
export NGKG_SOURCE_ID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
export NGKG_TRIG_SHA256="$(openssl dgst -sha256 -r "$NGKG_TRIG_FILE" | awk '{print $1}')"
```

Upload the original bytes:

```bash
curl --fail-with-body --silent --show-error \
  -X PUT "$NGKG_API_URL/v1/datasets/$NGKG_DATASET_ID/sources/$NGKG_SOURCE_ID" \
  -H "Authorization: Bearer $NGKG_BEARER_TOKEN" \
  -H 'Content-Type: application/trig; charset=utf-8' \
  -H "X-NGKG-Content-SHA256: $NGKG_TRIG_SHA256" \
  --data-binary "@$NGKG_TRIG_FILE" | tee .ngkg/upload-response.json | jq .
```

The default upload ceiling is 100 GiB, two billion quads, and 10,000 named graphs. Only one upload lane is admitted per API pod by default.

Successful HTTP `201` response:

```json
{
  "sourceId": "<source-uuid>",
  "datasetId": "<dataset-uuid>",
  "objectKey": "sources/<tenant>/<dataset>/<source>/<sha256>/source.trig",
  "sha256": "<uploaded-file-sha256>",
  "bytes": 12345,
  "parsedQuadCount": 250,
  "defaultGraphQuadCount": 0,
  "namedGraphs": [
    {
      "graphIri": "https://c8-next-generation.io/example/core/semkg",
      "parsedQuadCount": 250
    }
  ],
  "metadataObjectKey": "sources/<tenant>/<dataset>/<source>/<sha256>/source-metadata.json",
  "metadataSha256": "<metadata-sha256>"
}
```

The API verifies the supplied SHA-256, parses the complete TriG dataset, counts quads by graph, rejects blank-node graph names, and writes immutable source and metadata objects.

## 10. Minimal valid TriG example

Use this only to verify the upload path:

```trig
@prefix ex: <https://c8-next-generation.io/example/core/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .

<https://c8-next-generation.io/example/core/semkg> {
  ex:Asset a owl:Class .
  ex:asset-1 rdf:type ex:Asset .
}
```

## 11. Common upload failures

| Status/code | Meaning | Fix |
| --- | --- | --- |
| `401` | Missing or unknown bearer token | Use the original token whose digest is in `tokens.json` |
| `403` | Token lacks `datasets:write` or `sources:write` | Add the required scope, recompute the file checksum, update the Secret and roll the API |
| `404 DATASET_NOT_FOUND` | Dataset UUID is wrong or belongs to another tenant | Use the `datasetId` returned by dataset creation |
| `415` | File is not sent as UTF-8 `application/trig` | Use the exact Content-Type shown above; other files are not accepted |
| `422 TRIG_SHA256_MISMATCH` | Header checksum does not match the bytes received | Recompute the checksum from the exact uploaded file |
| `422 INVALID_TRIG` | RDF parser rejected the file | Validate RDF 1.1 TriG syntax and UTF-8 encoding |
| `422 NAMED_SUBDOMAIN_GRAPH_REQUIRED` | The file contains no IRI-named graph | Add at least one absolute IRI graph block |
| `422 BLANK_GRAPH_NAME_REJECTED` | A graph is named with a blank node | Replace it with an absolute graph IRI |
| `429 SOURCE_UPLOAD_CAPACITY_EXHAUSTED` | All bounded upload lanes are busy | Retry after an active upload completes; HPA responds to sustained resource use |
| `503` | PostgreSQL, object storage, or Kubernetes dependency is unavailable | Check `/health/ready`, pod events, database connectivity, and bucket permissions |

## What the upload does not do

A successful upload validates and stores the TriG source; it does not by itself certify and publish a queryable OWL 2 DL snapshot. Compilation requires a checksum-bound compilation bundle or the advanced cloud-import workflow, operator jobs, ontology qualification, distributed reasoning, certification, and snapshot publication. Follow `README.md` and `docs/GA_RELEASE_AND_OPERATIONS.md` when you are ready for that workflow.
