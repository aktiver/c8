# NGKG MCP/Agent Phase 2 delivery report

Phase 2 implements dependency-program Milestone 4, **add-on catalog and audit**, on top of the supplied Phase 1 candidate. The frozen `NGKG_1_0_0_GA` sibling remains outside the add-on workspace and unchanged.

## Delivered

1. **Tenant database boundary.** Two checksum-tracked PostgreSQL migrations create the `ngkg_agents` schema. Every tenant relation enables and forces RLS against a transaction-local tenant UUID installed by the Rust repository.
2. **Immutable catalog history.** Tool providers, discovered MCP catalogs, agent profiles, and retention policies use append-only version rows with immutable triggers and bounded Rust validation.
3. **Durable execution state.** Agent executions use legal state transitions and optimistic state-version CAS. Model and tool calls have immutable identities and may be finalized only once.
4. **Semantic and human-control evidence.** Claim verdicts and approval decisions are immutable and bind hashes, query execution IDs, proof support identifiers, policy identity, catalog identity, principals, and epoch timestamps.
5. **Honest resource evidence.** Immutable execution-resource records distinguish `CONFIGURED_ALLOCATION` from `OBSERVED_USAGE`; configured estimates cannot contain a claimed distinct-physical-node count.
6. **Tamper-evident tenant audit.** Audit appends use per-tenant transaction advisory locks, a domain-separated canonical SHA-256 chain, deterministic logical event IDs, idempotent retries, immutable rows, and exact external WORM seal receipts.
7. **Fail-closed gateway audit.** The MCP gateway writes STARTED and terminal events around every semantic tool call. Deployment-policy rejection writes DENIED. An unavailable or conflicting audit ledger fails the tool call rather than returning unaudited semantic output.
8. **Separated database authority.** The chart uses different runtime and migration Secrets. The migrator accepts only a pre-created login role that is not superuser, has no `BYPASSRLS`, inherits no role, and owns no agent object; it grants only the runtime privileges required by repository operations.
9. **HA deployment integration.** PostgreSQL is an explicit default-deny egress flow for both the gateway and pre-install/pre-upgrade migration hook. Gateway CPU and memory HPA targets remain fixed at 80%, with PDB and topology spreading preserved.
10. **Qualification assets.** Closed audit/execution JSON Schemas, deterministic audit hash vector, Rust validation tests, SQL structural checks, and an executable PostgreSQL suite cover forced RLS, cross-tenant invisibility, immutable history, legal CAS transitions, finalize-once calls, and audit linkage.

## Trust boundaries preserved

- The gateway calls only the frozen authenticated NGKG public query and query-log APIs; it does not import planner, executor, reasoner, catalog, storage, worker, or Kubernetes internals.
- NGKG remains the OWL 2 DL semantic authority. This phase records claim evidence but does not permit a model to overrule the reasoner.
- Federated `SERVICE` results remain `FEDERATED_VOLATILE` and outside the immutable local snapshot trust boundary.
- Resource allocation estimates remain distinct from measured usage and distinct Kubernetes node identity.
- No model provider, user-supplied MCP execution, vLLM deployment, GPU scheduling, mmap context broker, or OpenMP worker was added. This catalog/audit path is transactional I/O; native parallel kernels would add no qualified benefit here.

## Next phase

Phase 3 is dependency-program Milestone 5: **shared delegation authentication**. It extracts a common `ngkg-auth` crate, adds OAuth resource metadata, OIDC/JWKS validation and bounded cache behavior, policy lookup, actor/subject delegation, and short-lived NGKG 1.1 internal credentials with complete negative and opaque-token regression tests.

Milestones 5 through 13 remain after this source increment. Milestone 11, the mmap context broker, is optional and is implemented only if measured payloads justify it.
