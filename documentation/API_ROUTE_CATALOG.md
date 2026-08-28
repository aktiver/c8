# NGKG REST API Route Catalog — Phase 40 Baseline

Phase 40 requires runtime-route/OpenAPI parity. The control plane now exposes its Swagger UI at `/docs` and both `/openapi.yaml` and `/openapi.json`; every online role exposes the same documentation endpoints with its role-filtered online contract. `api/online-openapi.yaml` remains the complete source contract for all online roles.

## Control-plane API

| Method | Route | Purpose |
| --- | --- | --- |
| GET | `/docs` | Open the vendored Swagger UI for the control-plane contract. |
| GET | `/openapi.yaml` | Return the embedded control-plane OpenAPI 3.1 YAML. |
| GET | `/openapi.json` | Return the same control-plane contract as JSON. |
| GET | `/health/live` | Report that the API process is alive. |
| GET | `/health/ready` | Check PostgreSQL and Kubernetes API readiness. |
| PUT | `/v1/datasets/{datasetId}` | Idempotently create a tenant-scoped dataset. |
| PUT | `/v1/datasets/{datasetId}/sources/{sourceId}` | Stream, validate, checksum and persist an RDF 1.1 TriG source. |
| POST | `/v1/datasets/{datasetId}/ingestions` | Register an immutable compilation request and ensure its Kubernetes work exists. |
| GET | `/v1/jobs/{operationId}` | Read durable operation/job state and immutable request metadata. |
| POST | `/v1/jobs/{operationId}/cancel` | Durably cancel an operation and trigger active-work cleanup. |
| GET | `/v1/datasets/{datasetId}/snapshots/{snapshotId}` | Read an immutable certified or published snapshot record. |
| POST | `/v1/datasets/{datasetId}/snapshots/{snapshotId}/publish` | Atomically publish a snapshot if its predecessor is still active. |
| POST | `/v1/datasets/{datasetName}/snapshots/{snapshotId}/storage-operations` | Start checksum-bound replication, relocation, node-loss repair, or backup using operator-owned storage targets. |
| POST | `/v1/datasets/{datasetName}/restores` | Restore every artifact from a certified backup into an inactive storage namespace. |
| GET | `/v1/storage-operations/{operationId}` | Read the Kubernetes-backed status and immutable certificate references for storage work. |

## Online/data-plane API

Common routes exist on every online role; query, fragment, locator and hydration operations are exposed only by the corresponding role. The role-filtered `/openapi.json` used by Swagger therefore shows only operations callable on that replica, while `api/online-openapi.yaml` contains the complete online contract.

| Method | Route | Role | Purpose |
| --- | --- | --- | --- |
| GET | `/docs` | all | Open the vendored Swagger UI for that online role. |
| GET | `/openapi.yaml` | all | Return that role's filtered OpenAPI YAML. |
| GET | `/openapi.json` | all | Return that role's filtered OpenAPI JSON. |
| GET | `/health/live` | all | Report that the online process is alive. |
| GET | `/health/ready` | all | Check the catalog dependency required by the role. |
| GET | `/metrics` | all | Return Prometheus admission, latency, cache, streaming and join metrics. |
| GET | `/v1/datasets/{datasetId}/sparql` | query | Execute SPARQL 1.1 using protocol GET parameters. |
| POST | `/v1/datasets/{datasetId}/sparql` | query | Execute SPARQL 1.1 using raw or form-encoded protocol POST. |
| GET | `/v1/datasets/{datasetId}/sparql/service-description` | query | Return the authenticated fail-closed SPARQL Service Description. |
| POST | `/v1/datasets/{datasetId}/query` | query | Execute an exact NGKG JSON query with optional optimized certification. |
| POST | `/v1/datasets/{datasetId}/fragments/{querySha256}/{fragmentId}/execute` | fragment | Execute one immutable certified graph fragment and stream exact bindings. |
| POST | `/v1/datasets/{datasetId}/shuffles/{querySha256}/{stage}/{partition}/join` | fragment | Execute one exact hash-owned distributed join partition. |
| POST | `/v1/datasets/{datasetId}/locate` | locator | Resolve qualified GUIDs through the checksum-verified mmap locator index. |
| POST | `/v1/datasets/{datasetId}/hydrate` | hydration | Hydrate authorized GUIDs from exact Parquet row groups. |

`/docs/{*asset}` is an internal Swagger static-asset route on both services and is intentionally excluded from the REST operation contract.

## Contract enforcement

`scripts/verify_api_openapi_parity.py` parses the actual Axum `Router::route(...)` registrations, normalizes Rust parameter names to OpenAPI parameter names, and fails if a runtime REST operation is missing from OpenAPI or if OpenAPI advertises a route that no runtime handler registers. It also requires `/docs`, `/openapi.yaml`, and `/openapi.json` on both REST services.
