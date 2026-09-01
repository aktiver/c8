# Evidence-bound agent memory API

Phase 7 adds five memory classes without making an LLM a semantic authority. Every operation is available through authenticated REST and every model-facing memory operation is also an MCP tool backed by the same Rust service and policy checks. The complete contract is served at `GET /openapi.yaml`; interactive documentation is at `GET /swagger-ui`.

## Authority and retrieval rules

| Class | Purpose | Current-memory rule |
| --- | --- | --- |
| `WORKING` | Short-lived execution state and objectives | Validated, authorized and inside its mandatory TTL |
| `EPISODIC` | Immutable interaction and outcome history | Validated, authorized and not revoked/superseded |
| `SEMANTIC` | Candidate RDF facts | Only `PUBLISHED`, after certificate-subset checks, snapshot-pinned OWL entailment, approval and published-snapshot re-entailment |
| `PROCEDURAL` | Versioned workflows, policies and templates | Validated, authorized and not revoked/superseded |
| `EVIDENCE` | Snapshot, query, proof and certificate references | Validated, authorized and not revoked/superseded |

Absence remains unknown. An unentailed statement becomes `UNKNOWN`; it is never promoted to false or accepted by approval. Federated volatile, partial, stale, graph-mismatched or proof-mismatched results fail closed. Models can propose memory, but cannot directly publish an authoritative RDF fact.

## REST and MCP parity

| REST operation | Scope | MCP equivalent | Purpose |
| --- | --- | --- | --- |
| `POST /v1/memories` | `memory:write` | `ngkg_memory_propose` | Store an immutable proposal and evidence identity |
| `POST /v1/memories/search` | `memory:read` | `ngkg_memory_search` | Search only current authorized memory |
| `GET /v1/memories/{memoryId}` | `memory:read` | — | Read one authorized memory record |
| `GET /v1/memories/{memoryId}/explain` | `memory:read` | `ngkg_memory_explain` | Return provenance, transitions, edges and inclusion rule |
| `POST /v1/memories/{memoryId}/validate` | `memory:validate` | `ngkg_memory_validate` | Validate content; re-entail every semantic statement through public NGKG query APIs |
| `POST /v1/memories/{memoryId}/approve` | `memory:approve` | `ngkg_memory_approve` | Approve an entailed semantic candidate |
| `POST /v1/memories/{memoryId}/publish` | `memory:publish` | `ngkg_memory_publish` | Re-entail against the published snapshot and atomically activate retrieval |
| `POST /v1/memories/{memoryId}/supersede` | `memory:write` | `ngkg_memory_supersede` | Preserve history while replacing an older memory |
| `POST /v1/memories/{memoryId}/revoke` | `memory:write` | `ngkg_memory_revoke` | Exclude memory without deleting evidence |

The semantic MCP/query features also have REST parity: active snapshot, SPARQL query/context graph and query log are exposed at `GET /v1/datasets/{datasetId}/active-snapshot`, `POST /v1/datasets/{datasetId}/query`, and `GET /v1/query_logs/{queryExecutionId}`. Tenant MCP provider registration, qualification, approvals and calls are REST-controlled under `/v1/tool-*`. OpenAPI `x-mcp-tool` or `x-mcp-tools` fields provide the machine-readable mapping.

## Semantic publication sequence

1. Submit canonical, ground N-Triples that are an exact subset of a completed Phase 5 answer certificate.
2. `validate` reconstructs server-owned `ASK` queries, pins the certified dataset/snapshot and requires complete local reasoning evidence.
3. An authorized approver moves only entailed memory to `APPROVED`.
4. The external graph publication operation atomically publishes a qualified NGKG snapshot.
5. `publish` re-entails every statement in that published snapshot, records proof/query IDs and activates `PUBLISHED` in one database transaction.

`ngkgOperationId` is provenance, not permission to bypass step 5. No method imports NGKG planner, executor, reasoner or storage internals.

## Storage, isolation and bounds

PostgreSQL tables force tenant RLS and use transaction-local tenant identity. Versions, transitions, edges and publication receipts are immutable; top-level memory can transition but cannot be deleted. Owner-only memory additionally checks the authenticated subject. Idempotency is tenant-and-subject scoped. Credential-like content and common prompt-poisoning material are blocked before storage.

The default content limit is 512 KiB, search returns at most 100 rows, semantic proposals contain at most 1,000 statements, working memory expires within 24 hours, and retention is capped at 3,650 days. Larger transcripts and context artifacts belong in the Phase 4 checksum-bound object-input pipeline; memory stores compact facts, summaries and evidence references.

The gateway is Kubernetes-native and stateless. HPA scales replicas when either CPU or RAM reaches 80%, while a provider-neutral cluster autoscaler provisions additional nodes for pending pods. Compilation and later qualification workloads use cgroup-aware parallel workers, deterministic partition reduction, bounded spill, leases and restartable checkpoints. OpenMP is kept out of I/O-bound request handlers, and mmap is permitted only for checksum-verified immutable indexes after measurements justify it.
