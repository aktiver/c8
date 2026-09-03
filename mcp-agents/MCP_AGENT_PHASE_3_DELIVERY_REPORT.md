# NGKG MCP/Agent Phase 3 Delivery Report

## Outcome

Phase 3 implements the source foundation for engineering milestone 5: shared
delegation authentication and the NGKG 1.1 subject/actor compatibility
contract. It is additive to the Phase 2 catalog/audit candidate and does not
modify the frozen `NGKG_1_0_0_GA` sibling.

## Delivered implementation

1. `crates/ngkg-auth` owns the immutable `Identity`, authenticated request,
   explicit trust-mode configuration, and sanitized failure model.
2. `opaque` preserves checksum-bound NGKG 1.0 token-file parsing, required
   query scope and graph labels, hashed lookup, duplicate rejection, and
   per-tenant identity.
3. `delegation` accepts only asymmetric JOSE algorithms, requires exact type,
   KID, issuer, audience, subject, expiry, not-before, issued-at, JTI, token use,
   tenant, policy checksum, query scope, and graph labels, and enforces a
   five-minute default maximum lifetime.
4. JWKS retrieval is HTTPS-only, redirect-free, size/key/time bounded, ETag
   aware, rotation capable, and backed by finite last-known-good grace.
   Readiness fails after that grace; key or identity failures never select
   opaque mode.
5. Optional token exchange requests only the configured NGKG audience and
   scope set. The returned internal token is signature-verified and rejected
   on scope or audience escalation. Workload identity/mTLS is the preferred
   client boundary; a checksum-bound mounted client-secret file is optional.
6. Gateway middleware replaces the external bearer with the verified internal
   bearer before MCP execution. Audit events now preserve original `subject`
   and delegating `actor`.
7. Delegation deployments expose OAuth protected-resource metadata. Opaque
   deployments do not advertise OAuth metadata.
8. Helm 0.3.0 renders mode-specific environment, Secrets, annotations, and
   identity-provider egress while retaining three-replica HA, 80% CPU-or-memory
   HPA, topology spread, disruption budget, and hardened containers.
9. Closed JSON contracts define delegation claims, OAuth resource metadata,
   and the NGKG 1.1 authenticated identity. The compatibility document defines
   the required 1.1 service integration without altering 1.0 behavior.
10. A deterministic Phase 3 structural qualification gate checks algorithms,
    no-fallback selection, bounded exchange/JWKS behavior, subject/actor audit,
    chart wiring, and contract closure.

## Security boundaries

Tenant, scopes, graph labels, policy version, and execution binding come only
from the issuer-signed `ngkg` namespace. Tool arguments, prompts, SPARQL,
headers, and model output cannot override them. External and internal tokens
are never included in audit payloads, tool results, or tracing fields.

The source uses configured exact HTTPS identity endpoints and Kubernetes CIDR
egress. Live qualification must additionally prove provider DNS and CNI policy,
TLS trust, key rotation, workload identity, revocation policy, and outage
behavior in every supported cluster.

## Qualification status

- Phase 2 archive checksum and complete payload manifest: passed before edits.
- Frozen GA acceptance/static gates: passed before edits.
- Phase 3 JSON parsing and authentication structural gate: passed.
- Cumulative static gate: passes after the Phase 3 source manifest is sealed.
- Native Rust format/check/test/clippy: blocked because Cargo/Rust are absent.
- Helm lint/template: blocked because Helm is absent.
- Signed-token/JWKS/exchange interoperability: requires native test services.
- Live RKE/RKE2, EKS, AKS, GKE, identity-provider, and NGKG 1.1 HA tests:
  require external infrastructure.

This is a source-implemented candidate, not a production-qualified identity
release. A controlled Rust build must generate and review `Cargo.lock`; it must
not be fabricated.

## Next phase

Phase 4 is long-input ingestion and deterministic context compilation. It adds
resumable prompt/file parts, immutable raw-object manifests, structural chunks,
stable requirement IDs, constraint-ledger roots, evidence-linked hierarchical
reduction, bounded compiled contexts, and requirement-coverage records. CPU
workers will parallelize independent parsing/extraction while deterministic
merge order makes one-core, many-core, and multinode results identical.

After Phase 3, ten planned source phases remain: Phase 4 through Phase 13.
