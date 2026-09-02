# NGKG REST and Swagger route catalog

Every operation below is embedded in its serving binary and appears in Swagger UI. Request and response keys, JSON value types, required fields, enums and bounds remain authoritative in the linked component schema inside each OpenAPI document.

## `NGKG_1_0_0_GA/api/openapi.yaml`

| Method | Route | Operation | Request | Success response |
|---|---|---|---|---|
| GET | `/docs` | `swaggerUi` — Open the Swagger UI for the control-plane API | path/query/header parameters only | 200 text/html string |
| GET | `/openapi.json` | `getOpenApiJson` — Retrieve the control-plane OpenAPI document as JSON | path/query/header parameters only | 200 application/json object |
| GET | `/openapi.yaml` | `getOpenApi` — Retrieve the control-plane OpenAPI document as YAML | path/query/header parameters only | 200 application/yaml string |
| GET | `/health/live` | `live` — Report control-plane liveness | path/query/header parameters only | 204 no body |
| GET | `/health/ready` | `ready` — Report control-plane dependency readiness | path/query/header parameters only | 204 no body |
| POST | `/v1/datasets` | `createNamedDataset` — Create a tenant dataset with a human-readable name | application/json #/components/schemas/CreateNamedDatasetRequest | 200 application/json #/components/schemas/Dataset |
| PUT | `/v1/datasets/{datasetId}` | `createDataset` — Create or verify a tenant dataset | application/json #/components/schemas/CreateDatasetRequest | 204 no body |
| PUT | `/v1/datasets/{datasetId}/sources/{sourceId}` | `uploadTrigSource` — Upload and validate an immutable RDF 1.1 TriG source | application/trig string | 201 application/json #/components/schemas/TrigUploadResponse |
| POST | `/v1/datasets/{datasetId}/ingestions` | `createIngestion` — Start an immutable dataset compilation | application/json #/components/schemas/CreateIngestionRequest | 202 application/json #/components/schemas/OperationAccepted |
| POST | `/v1/datasets/{datasetName}/imports` | `createCloudImportByName` — Import existing TriG objects from a cloud bucket | application/json #/components/schemas/CreateCloudImportRequest | 202 application/json #/components/schemas/CloudImportAccepted |
| GET | `/v1/datasets/{datasetName}/imports/{operationId}` | `getCloudImportByName` — Read existing-cloud TriG import status | path/query/header parameters only | 200 application/json #/components/schemas/CloudImportStatus |
| POST | `/v1/datasets/by-id/{datasetId}/imports` | `createCloudImportById` — Import existing TriG objects using the internal dataset UUID | application/json #/components/schemas/CreateCloudImportRequest | 202 application/json #/components/schemas/CloudImportAccepted |
| GET | `/v1/jobs/{operationId}` | `getJob` — Read durable compilation job state | path/query/header parameters only | 200 application/json #/components/schemas/Job |
| POST | `/v1/jobs/{operationId}/cancel` | `cancelJob` — Cancel a durable compilation job | application/json #/components/schemas/CancelRequest | 200 application/json #/components/schemas/Operation |
| GET | `/v1/datasets/{datasetId}/snapshots/{snapshotId}` | `getSnapshot` — Read immutable snapshot metadata | path/query/header parameters only | 200 application/json #/components/schemas/Snapshot |
| POST | `/v1/datasets/{datasetId}/snapshots/{snapshotId}/publish` | `publishSnapshot` — Atomically publish a qualified snapshot | application/json #/components/schemas/PublishRequest | 200 application/json #/components/schemas/Snapshot |
| POST | `/v1/datasets/{datasetName}/snapshots/{snapshotId}/storage-operations` | `createStorageOperation` — Replicate, relocate, repair, or back up a certified snapshot | application/json #/components/schemas/CreateStorageOperationRequest | 202 application/json #/components/schemas/StorageOperationAccepted |
| POST | `/v1/datasets/{datasetName}/restores` | `createRestore` — Restore a checksum-bound backup into inactive storage | application/json #/components/schemas/CreateRestoreRequest | 202 application/json #/components/schemas/StorageOperationAccepted |
| GET | `/v1/storage-operations/{operationId}` | `getStorageOperation` — Read distributed storage operation status | path/query/header parameters only | 200 application/json #/components/schemas/StorageOperationStatus |

## `NGKG_1_0_0_GA/api/online-openapi.yaml`

| Method | Route | Operation | Request | Success response |
|---|---|---|---|---|
| GET | `/docs` | `getDocs` — Open the Swagger UI for this contract | path/query/header parameters only | 200 text/html string |
| GET | `/openapi.yaml` | `getOpenapiYaml` — Retrieve the canonical OpenAPI YAML | path/query/header parameters only | 200 application/yaml string |
| GET | `/openapi.json` | `getOpenapiJson` — Retrieve the canonical OpenAPI document as JSON | path/query/header parameters only | 200 application/json object |
| GET | `/health/live` | `getHealthLive` — Report online-role process liveness | path/query/header parameters only | 204 no body |
| GET | `/health/ready` | `getHealthReady` — Report online-role catalog readiness | path/query/header parameters only | 204 no body |
| GET | `/metrics` | `getMetrics` — Prometheus admission, service-time, cache and worker-join metrics | path/query/header parameters only | 200 text/plain string |
| GET | `/v1/hpc/capabilities` | `getHpcCapabilities` — Inspect the authenticated query pod's bounded HPC capabilities | path/query/header parameters only | 200 application/json #/components/schemas/HpcCapabilities |
| GET | `/v1/query_logs` | `getV1QueryLogs` — List tenant-scoped SPARQL query executions | path/query/header parameters only | 200 application/json #/components/schemas/QueryLogPage |
| GET | `/v1/query_logs/{queryExecutionId}` | `getV1QueryLogsQueryExecutionId` — Read one tenant-scoped SPARQL query execution | path/query/header parameters only | 200 application/json #/components/schemas/QueryLog |
| GET | `/v1/datasets/{datasetId}/sparql` | `getV1DatasetsDatasetIdSparql` — Execute a SPARQL 1.1 query through a SPARQL Protocol GET encoding | path/query/header parameters only | 200 #/components/responses/SparqlQuery; 200 no body |
| POST | `/v1/datasets/{datasetId}/sparql` | `postV1DatasetsDatasetIdSparql` — Execute a SPARQL 1.1 query through a SPARQL Protocol POST encoding | application/sparql-query string; application/x-www-form-urlencoded #/components/schemas/SparqlFormRequest | 200 #/components/responses/SparqlQuery; 200 no body |
| POST | `/v1/datasets/{datasetId}/sparql/direct/validate` | `postV1DatasetsDatasetIdSparqlDirectValidate` — Validate OWL 2 Direct-Semantics BGP legality | application/json #/components/schemas/DirectBgpValidationRequest | 200 application/json #/components/schemas/DirectBgpLegalityReport |
| POST | `/v1/datasets/{datasetId}/sparql/direct/route` | `postV1DatasetsDatasetIdSparqlDirectRoute` — Route OWL 2 Direct-Semantics BGPs fail closed | application/json #/components/schemas/DirectBgpValidationRequest | 200 application/json #/components/schemas/DirectEntailmentRoutingResponse |
| GET | `/v1/datasets/{datasetId}/sparql/service-description` | `getV1DatasetsDatasetIdSparqlServiceDescription` — Retrieve the authenticated dataset service description | path/query/header parameters only | 200 text/turtle string |
| POST | `/v1/datasets/{datasetId}/query` | `postV1DatasetsDatasetIdQuery` — Execute an exact SPARQL query through the NGKG JSON API | application/json #/components/schemas/QueryRequest | 200 application/json #/components/schemas/QueryResponse |
| POST | `/v1/datasets/{datasetId}/locate` | `postV1DatasetsDatasetIdLocate` — Resolve qualified GUIDs through the checksum-verified mmap index | application/json #/components/schemas/PhysicalRequest | 200 application/json #/components/schemas/LocatorResponse |
| POST | `/v1/datasets/{datasetId}/fragments/{querySha256}/{fragmentId}/execute` | `postV1DatasetsDatasetIdFragmentsQuerySha256FragmentIdExecute` — Execute one immutable certified graph fragment | application/json #/components/schemas/FragmentExecutionRequest | 200 application/vnd.apache.arrow.stream string |
| POST | `/v1/datasets/{datasetId}/shuffles/{querySha256}/{stage}/{partition}/join` | `postV1DatasetsDatasetIdShufflesQuerySha256StagePartitionJoin` — Execute one exact hash-owned join partition | application/vnd.apache.arrow.stream string | 200 application/vnd.apache.arrow.stream string |
| POST | `/v1/datasets/{datasetId}/hydrate` | `postV1DatasetsDatasetIdHydrate` — Hydrate qualified GUIDs from exact Parquet row groups | application/json #/components/schemas/PhysicalRequest | 200 application/json #/components/schemas/HydrationResponse |
| POST | `/v1/datasets/{datasetId}/algebra/{querySha256}/{replica}/execute` | `postV1DatasetsDatasetIdAlgebraQuerySha256ReplicaExecute` — Execute one checksum-bound complete SPARQL algebra replica | application/json #/components/schemas/DistributedAlgebraExecutionRequest | 200 application/json #/components/schemas/DistributedAlgebraExecutionResponse |
| POST | `/v1/datasets/{datasetId}/paths/{querySha256}/{pathId}/{iteration}/{partition}/expand` | `postV1DatasetsDatasetIdPathsQuerySha256PathIdIterationPartitionExpand` — Execute one immutable semantic-partition property-path scan | application/json #/components/schemas/PartitionPathExecutionRequest | 200 application/json #/components/schemas/PartitionPathExecutionResponse |
| POST | `/v1/datasets/{datasetId}/native/leaves/{querySha256}/{partition}/scan` | `postV1DatasetsDatasetIdNativeLeavesQuerySha256PartitionScan` — Scan one checksum-verified semantic Parquet partition | application/json #/components/schemas/NativeLeafScanRequest | 200 application/json #/components/schemas/NativeLeafScanResponse |

## `ngkg-agents/contracts/agent-input-openapi.yaml`

| Method | Route | Operation | Request | Success response |
|---|---|---|---|---|
| POST | `/agent-inputs` | `createAgentInput` — Create a tenant-scoped resumable input session | application/json #/components/schemas/CreateInput | 201 application/json ./prompt-manifest.schema.json |
| PUT | `/agent-inputs/{inputId}/parts/{ordinal}` | `putAgentInputPart` — Idempotently store one checksum-bound source part | application/octet-stream string; text/* string | 200 no body |
| POST | `/agent-inputs/{inputId}/finalize` | `finalizeAgentInput` — Freeze the exact source manifest and enqueue compilation shards | application/json #/components/schemas/FinalizeInput | 202 application/json ./prompt-manifest.schema.json |
| GET | `/agent-inputs/{inputId}` | `getAgentInputStatus` — Get upload or compilation progress | path/query/header parameters only | 200 application/json #/components/schemas/InputStatus |
| GET | `/agent-inputs/{inputId}/manifest` | `getAgentInputManifest` — Get the frozen source and derived-root manifest; storage references are redacted | path/query/header parameters only | 200 application/json ./prompt-manifest.schema.json |
| GET | `/agent-inputs/{inputId}/requirements` | `listAgentInputRequirements` — List the permanent constraint ledger in source order | path/query/header parameters only | 200 application/json array |

## `ngkg-agents/contracts/agent-memory-openapi.yaml`

| Method | Route | Operation | Request | Success response |
|---|---|---|---|---|
| POST | `/v1/memories` | `proposeMemory` — propose Memory | application/json ./memory-proposal.schema.json | 201 application/json ./memory-view.schema.json |
| POST | `/v1/memories/search` | `searchMemories` — search Memories | application/json ./memory-search.schema.json | 200 application/json array |
| GET | `/v1/memories/{memoryId}` | `getMemory` — get Memory | path/query/header parameters only | 200 application/json ./memory-view.schema.json |
| GET | `/v1/memories/{memoryId}/explain` | `explainMemory` — explain Memory | path/query/header parameters only | 200 no body |
| POST | `/v1/memories/{memoryId}/validate` | `validateMemory` — validate Memory | path/query/header parameters only | 200 no body |
| POST | `/v1/memories/{memoryId}/approve` | `approveMemory` — approve Memory | path/query/header parameters only | 200 no body |
| POST | `/v1/memories/{memoryId}/publish` | `publishMemory` — publish Memory | application/json ./memory-publication.schema.json | 200 no body |
| POST | `/v1/memories/{memoryId}/supersede` | `supersedeMemory` — supersede Memory | application/json object | 200 no body |
| POST | `/v1/memories/{memoryId}/revoke` | `revokeMemory` — revoke Memory | path/query/header parameters only | 200 no body |

## `ngkg-agents/contracts/context-slice-openapi.yaml`

| Method | Route | Operation | Request | Success response |
|---|---|---|---|---|
| POST | `/v1/context-slices` | `createContextSlice` — Create an immutable-slice upload session | application/json #/components/schemas/CreateSlice | 201 application/json #/components/schemas/Slice |
| GET | `/v1/context-slices/{sliceId}` | `getContextSlice` — Read the redacted slice lifecycle and semantic bindings | path/query/header parameters only | 200 application/json #/components/schemas/Slice |
| PUT | `/v1/context-slices/{sliceId}/chunks/{ordinal}` | `putContextSliceChunk` — Store one checksum-bound immutable chunk | application/octet-stream string | 204 no body |
| POST | `/v1/context-slices/{sliceId}/finalize` | `finalizeContextSlice` — Verify every chunk and atomically activate the manifest and mmap locator index | application/json object | 200 application/json #/components/schemas/Slice |
| POST | `/v1/context-slices/{sliceId}/capabilities` | `issueContextSliceCapability` — Issue a short-lived subject, tenant, audience and byte-range-bound capability | application/json #/components/schemas/CapabilityRequest | 200 application/json #/components/schemas/Capability |
| GET | `/v1/context-slices/{sliceId}/content` | `readContextSliceRange` — Read the exact authorized range after index and per-chunk verification | path/query/header parameters only | 206 application/octet-stream string; 206 application/n-triples string |
| POST | `/v1/context-slices/{sliceId}/expire` | `expireContextSlice` — Revoke new access and enter the recoverable deletion window | path/query/header parameters only | 200 application/json #/components/schemas/Slice |
| GET | `/health/live` | `contextSliceLiveness` — context Slice Liveness | path/query/header parameters only | 204 no body |
| GET | `/health/ready` | `contextSliceReadiness` — context Slice Readiness | path/query/header parameters only | 204 no body |
| GET | `/metrics` | `contextSliceMetrics` — context Slice Metrics | path/query/header parameters only | 200 text/plain string |

## `ngkg-agents/contracts/inference-gateway-openapi.yaml`

| Method | Route | Operation | Request | Success response |
|---|---|---|---|---|
| GET | `/health/live` | `inferenceLive` — inference Live | path/query/header parameters only | 204 no body |
| GET | `/health/ready` | `inferenceReady` — inference Ready | path/query/header parameters only | 204 no body |
| GET | `/v1/status` | `inferenceInstanceStatus` — inference Instance Status | path/query/header parameters only | 200 application/json #/components/schemas/InferenceStatus |
| POST | `/v1/chat/completions` | `createVllmChatCompletion` — create Vllm Chat Completion | application/json object | 200 application/json object |
| GET | `/metrics` | `inferenceMetrics` — inference Metrics | path/query/header parameters only | 200 no body |
| GET | `/openapi.yaml` | `inferenceOpenApi` — inference Open Api | path/query/header parameters only | 200 no body |

## `ngkg-agents/contracts/managed-agent-openapi.yaml`

| Method | Route | Operation | Request | Success response |
|---|---|---|---|---|
| POST | `/v1/agent-executions` | `executeManagedAgent` — Produce a wholly entailed, snapshot-bound RDF answer | application/json ./managed-agent-request.schema.json | 200 application/json object |

## `ngkg-agents/contracts/mcp-agent-openapi.yaml`

| Method | Route | Operation | Request | Success response |
|---|---|---|---|---|
| GET | `/openapi.yaml` | `getOpenApiContract` — Download the complete gateway OpenAPI 3.1 contract | path/query/header parameters only | 200 application/yaml string |
| GET | `/swagger-ui` | `getSwaggerUi` — Open interactive Swagger documentation for every REST-driven MCP and agent operation | path/query/header parameters only | 200 no body |
| POST | `/mcp` | `mcpStreamableHttp` — MCP initialize | application/json object | 200 no body |
| GET | `/v1/datasets/{datasetId}/active-snapshot` | `getActiveSnapshot` — Resolve the authorized active snapshot through an ASK barrier | path/query/header parameters only | 200 #/components/responses/SemanticEnvelope; 200 no body |
| POST | `/v1/datasets/{datasetId}/query` | `executeSemanticQuery` — Execute authorized snapshot-bound SPARQL and return a reasoned envelope | application/json #/components/schemas/QueryRequest | 200 #/components/responses/SemanticEnvelope; 200 no body |
| GET | `/v1/query_logs/{queryExecutionId}` | `getQueryLog` — Get immutable query timing | path/query/header parameters only | 200 no body |
| POST | `/v1/agent-inputs` | `createAgentInput` — Create a resumable checksum-bound input session | path/query/header parameters only | 201 no body |
| PUT | `/v1/agent-inputs/{inputId}/parts/{ordinal}` | `uploadAgentInputPart` — Upload an idempotent input part | path/query/header parameters only | 200 no body |
| POST | `/v1/agent-inputs/{inputId}/finalize` | `finalizeAgentInput` — Freeze the exact input manifest and enqueue deterministic compilation | path/query/header parameters only | 200 no body |
| GET | `/v1/agent-inputs/{inputId}` | `getAgentInput` — get Agent Input | path/query/header parameters only | 200 no body |
| GET | `/v1/agent-inputs/{inputId}/manifest` | `getAgentInputManifest` — get Agent Input Manifest | path/query/header parameters only | 200 no body |
| GET | `/v1/agent-inputs/{inputId}/requirements` | `getAgentInputRequirements` — get Agent Input Requirements | path/query/header parameters only | 200 no body |
| POST | `/v1/qualification-workloads` | `createQualificationWorkload` — Partition one frozen input into deterministic multinode CPU work | application/json #/components/schemas/QualificationWorkloadCreate | 202 application/json #/components/schemas/QualificationWorkload |
| GET | `/v1/qualification-workloads/{workloadId}` | `getQualificationWorkload` — Get exact partition progress and deterministic result root | path/query/header parameters only | 200 application/json #/components/schemas/QualificationWorkload |
| GET | `/v1/qualification-workloads/{workloadId}/checkpoints` | `getQualificationCheckpoints` — List immutable per-partition checkpoint evidence | path/query/header parameters only | 200 application/json array |
| POST | `/v1/qualification-workloads/{workloadId}/cancel` | `cancelQualificationWorkload` — Cancel all uncommitted partitions without deleting evidence | path/query/header parameters only | 200 application/json #/components/schemas/QualificationWorkload |
| POST | `/v1/agent-executions` | `executeManagedAgent` — Run a model proposal loop and issue an answer certificate only when every RDF claim is entailed | path/query/header parameters only | 200 no body |
| POST | `/v1/tool-providers` | `registerToolProvider` — Register an immutable pending tenant MCP provider | path/query/header parameters only | 201 no body |
| POST | `/v1/tool-providers/{providerId}/versions/{version}/qualify` | `qualifyToolProvider` — Perform live MCP initialization and catalog qualification | path/query/header parameters only | 200 no body |
| POST | `/v1/tool-approvals` | `recordToolApproval` — Record an immutable scoped approval or denial | path/query/header parameters only | 201 no body |
| POST | `/v1/tool-calls` | `invokeQualifiedTool` — Invoke one catalog-qualified tool with bounded transport | path/query/header parameters only | 200 no body |
| POST | `/v1/memories` | `proposeMemory` — Propose immutable working | application/json #/components/schemas/MemoryProposal | 201 #/components/responses/MemoryView; 201 no body |
| POST | `/v1/memories/search` | `searchMemories` — Search only authorized | application/json #/components/schemas/MemorySearch | 200 application/json array |
| GET | `/v1/memories/{memoryId}` | `getMemory` — Read an exact authorized memory version | path/query/header parameters only | 200 #/components/responses/MemoryView; 200 no body |
| GET | `/v1/memories/{memoryId}/explain` | `explainMemory` — Explain provenance | path/query/header parameters only | 200 no body |
| POST | `/v1/memories/{memoryId}/validate` | `validateMemory` — Structurally validate memory and re-entail semantic statements through NGKG | path/query/header parameters only | 200 no body |
| POST | `/v1/memories/{memoryId}/approve` | `approveMemory` — Approve an entailed semantic memory for publication activation | path/query/header parameters only | 200 #/components/responses/MemoryView; 200 no body |
| POST | `/v1/memories/{memoryId}/publish` | `publishMemory` — Activate semantic memory only after re-entailment in an atomically published NGKG snapshot | application/json #/components/schemas/MemoryPublication | 200 no body |
| POST | `/v1/memories/{memoryId}/supersede` | `supersedeMemory` — Exclude an older memory using an immutable supersession edge | application/json #/components/schemas/MemorySupersede | 200 #/components/responses/MemoryView; 200 no body |
| POST | `/v1/memories/{memoryId}/revoke` | `revokeMemory` — Revoke and exclude a memory without deleting history | path/query/header parameters only | 200 #/components/responses/MemoryView; 200 no body |

## `ngkg-agents/contracts/tool-broker-openapi.yaml`

| Method | Route | Operation | Request | Success response |
|---|---|---|---|---|
| POST | `/v1/tool-providers` | `registerToolProvider` — register Tool Provider | application/json ./tool-provider.schema.json | 201 no body |
| POST | `/v1/tool-providers/{providerId}/versions/{version}/qualify` | `qualifyToolProvider` — qualify Tool Provider | path/query/header parameters only | 200 application/json ./qualified-tool-catalog.schema.json |
| POST | `/v1/tool-approvals` | `recordToolApproval` — record Tool Approval | application/json ./tool-approval.schema.json | 201 no body |
| POST | `/v1/tool-calls` | `invokeQualifiedTool` — invoke Qualified Tool | application/json ./tool-call.schema.json | 200 no body |

## `ngkg-agents/contracts/vllm-pod-agent-openapi.yaml`

| Method | Route | Operation | Request | Success response |
|---|---|---|---|---|
| GET | `/health/live` | `vllmPodLive` — vllm Pod Live | path/query/header parameters only | 204 no body |
| GET | `/health/ready` | `vllmPodReady` — vllm Pod Ready | path/query/header parameters only | 204 no body |
| POST | `/v1/chat/completions` | `proxyVllmChatCompletion` — proxy Vllm Chat Completion | application/json object | 200 no body |
| GET | `/metrics` | `vllmPodMetrics` — vllm Pod Metrics | path/query/header parameters only | 200 no body |
| GET | `/openapi.yaml` | `vllmPodOpenApi` — vllm Pod Open Api | path/query/header parameters only | 200 no body |
| POST | `/admin/drain` | `drainVllmPod` — drain Vllm Pod | path/query/header parameters only | 204 no body |

## Swagger descriptions

Each Swagger operation contains a concise three- or four-sentence description covering intent, when to use it, the request and success payload shape, and the security/failure boundary.
