# NGKG MCP/Agent 0.1.0 first-slice delivery report

This delivery starts the approved enterprise plan without changing the frozen NGKG 1.0 product. The archive contains two sibling release units: the manifest-bound `NGKG_1_0_0_GA` source baseline and the new independently versioned `ngkg-agents` add-on. The deterministic archive builder omits non-source `__pycache__` files.

## Implemented

1. **Bounded NGKG public API client.** Strict Rust request/response/query-log types, HTTPS-only production origin, root-path pinning, no redirects, bounded admission, timeouts, streaming response ceiling, request correlation, query execution identity, SHA-256 validation, snapshot pin validation, and subordinate reasoner/path/federation completion checks.
2. **Semantic evidence contracts.** A versioned context envelope, explicit `CERTIFIED_COMPLETE`, `EXACT_COMPLETE`, `SPARQL_COMPLETE`, and `FEDERATED_VOLATILE` states, open-world `unknownIsFalse=false`, deterministic domain-separated full-result hashes, graph statement IDs, proof references, and fail-closed row/triple/byte limits.
3. **Authenticated MCP gateway.** Rust `rmcp` Streamable HTTP server, Host and Origin allowlists, request limit, stateless compatibility behavior, graceful shutdown, readiness, liveness, metrics, and four read-only semantic/query-log tools.
4. **Compatibility identity boundary.** Checksum-bound, tenant-specific NGKG 1.0 bearer token verification before MCP discovery or execution; query scope and graph authorization labels are mandatory. The exact validated bearer is forwarded only to the NGKG public Query service.
5. **HA Kubernetes deployment.** Separate Helm chart with three replicas, 80% CPU-or-memory HPA, PDB, hard topology spread, default-deny ingress/egress, explicit DNS and NGKG Query flows, no service-account token mount, non-root/read-only/seccomp/capability hardening, resource equality to prevent oversubscription, and optional ServiceMonitor.
6. **Automation.** Static and native acceptance entry points, pinned OpenAPI input hashes, closed JSON Schemas, immutable-image Dockerfile inputs, deployment example, operating limits, and explicit incomplete-gate reporting.

## Architectural corrections enforced

- No existing NGKG 1.0 source, API, chart, version, lockfile, or manifest was changed.
- No add-on dependency or method reaches internal fragments, shuffles, algebra, property paths, locator, hydration, reasoner, catalog, storage, or Kubernetes control interfaces.
- No model or MCP client is treated as an OWL authority.
- Remote `SERVICE` evidence remains outside the local certified-snapshot trust boundary.
- Query-log allocation estimates are not relabeled as observed CPU, RAM, or physical-node telemetry.
- OpenMP and native BLAS pools are fixed to one thread in the gateway because this service is I/O-bound; distributed RDF and OWL HPC remains inside NGKG. mmap is intentionally deferred to the optional immutable context broker, where fixed-width checksum-verified indexing is relevant.

## Next implementation increment

1. Generate and review `Cargo.lock`, compile, lint, test, and fix all toolchain findings.
2. Replace the reviewed handwritten online wire module with reproducibly generated modules plus schema-drift snapshots and complete typed proof manifests.
3. Add standards-complete N-Triples parsing/canonical test vectors and API-driven MCP interoperability fixtures.
4. Implement OAuth resource metadata, OIDC validation, policy lookup, and NGKG 1.1 short-lived subject/actor delegation.
5. Add tenant-RLS agent/model/tool/approval/claim audit migrations and repositories.
6. Implement the managed orchestrator, Claude/OpenAI/Hugging Face/vLLM provider interfaces, bounded tool loop, claim validation, and answer certificate.
7. Implement the isolated user MCP provider controller and secure outbound broker.
8. Add vLLM GPU deployments, KEDA queue scaling, NVIDIA resource discovery, and RKE/RKE2/EKS/AKS/GKE GPU-node overlays with provider node provisioners.
9. Add the optional content-addressed context broker and checksum-verified read-only mmap index only after inline limits demonstrate the need.
10. Run live semantic, security, HA, chaos, GPU autoscaling, and multi-provider Kubernetes qualification before release classification changes.

See `BUILD_STATUS_MCP_AGENT_0_1_0.md` for the exact distinction between executed evidence and blocked qualification.
