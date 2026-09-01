# Qualified user MCP tools

Phase 6 allows each authenticated tenant to register its own remote MCP servers without giving those servers access to NGKG internals. Registration creates an immutable `PENDING` provider version. Qualification performs live MCP `initialize`, `notifications/initialized`, and bounded paginated `tools/list`, rejects unsupported or remotely referenced schemas, canonicalizes tools by name, and records an immutable `QUALIFIED` provider version plus catalog and qualification hashes.

Tool calls require `tools:execute`, a catalog explicitly included in the immutable agent profile, the exact completed Phase 5 answer-certificate hash, a qualified provider version, a catalog-listed tool, schema-valid arguments, and any required unexpired approval. Approval records require the separate `tools:approve` scope and bind execution, tool, profile approval policy and catalog. The authenticated tenant and approver never come from request bodies.

Remote MCP output is classified `UNTRUSTED_EXTERNAL_TOOL`. It is useful for actions or additional evidence but is never inserted into the reasoned graph, treated as OWL truth, or attached to an answer certificate automatically. If an agent wants to turn tool output into factual claims, a later orchestrator step must canonicalize those claims and repeat NGKG authorization and OWL entailment validation.

Security controls include HTTPS-only endpoints, no redirects, credential indirection through checksum-bound mounted files, public-address enforcement by default, optional operator-controlled cluster-local service access, DNS resolution pinning, private/link-local/loopback/special-range rejection, operator-controlled egress CIDRs, MCP session and protocol-version propagation, closed protocol-version allowlists, bounded pagination, schemas, request/response bytes, timeouts and concurrency, forced tenant RLS, immutable catalogs, immutable approvals and finalize-once tool-call evidence. Credential entry paths must canonicalize into the same projected Secret generation directory as the checksum-bound registry manifest, preventing the broker from turning another pod-mounted identity file into a provider bearer token.

The `QUALIFIED` provider version and catalog are committed in one PostgreSQL transaction. Registration and qualification are intentionally separate so an operator can review the endpoint and egress policy before any live discovery call.

The API workflow is:

1. `POST /v1/tool-providers` with `tools:providers:write`.
2. `POST /v1/tool-providers/{providerId}/versions/1/qualify`.
3. Add the returned `catalogSha256` to a new immutable agent-profile version.
4. If policy requires it, `POST /v1/tool-approvals` with `tools:approve`.
5. `POST /v1/tool-calls` with `tools:execute`, the completed execution ID, its certificate hash, exact provider/catalog identities, arguments and optional approval ID.

Gateway pods continue to scale horizontally at 80% CPU or memory. Remote-wait concurrency is bounded per replica; unschedulable replicas trigger the configured RKE/RKE2, EKS, AKS, GKE or other Kubernetes node autoscaler. Tenant provider destinations remain reachable only when an operator adds their CIDRs to `toolBroker.externalEgressIpBlocks`.
