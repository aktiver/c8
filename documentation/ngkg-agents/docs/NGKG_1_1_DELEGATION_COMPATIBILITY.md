# NGKG 1.1 Delegation Compatibility Contract

Phase 3 does not modify the frozen sibling `NGKG_1_0_0_GA` source tree. It
defines the additive authentication contract that an NGKG 1.1 query or control
service must implement before deployment mode `delegation` can be qualified.

The service must use the same `ngkg-auth` verifier and immutable `Identity` as
the gateway. Authentication occurs before dataset, graph, snapshot, union
default, reasoning, query planning, publication, backup, restore, or recovery
authorization. The service records `subject` as the original principal and
`actor` as the delegating client. Existing `principalId` response fields remain
the subject for wire compatibility; new audit schema versions may add actor and
JTI without reinterpreting historical rows.

Only the signed `ngkg` claim namespace supplies tenant ID, scopes, graph
authorization labels, policy checksum, and optional agent-execution binding.
Headers, request bodies, SPARQL text, MCP tool arguments, model content, and
external-token claims cannot override that namespace. The configured issuer,
audience, algorithms, token type, time bounds, key ID, key use, and key
algorithm must all verify. Symmetric JOSE algorithms and `none` are prohibited.

Compatibility mode remains byte-for-byte equivalent to the NGKG 1.0 token-file
contract. It uses per-tenant, per-workload tokens and records the end-user
subject in the add-on audit chain. A deployment selects `opaque` or
`delegation` explicitly. Failure in delegation verification, JWKS refresh, or
token exchange never invokes the opaque verifier.

The gateway may accept an external OAuth access token only when exchange is
enabled. It sends that token to one exact HTTPS token endpoint, asks for the
configured NGKG audience and bounded scope set, then verifies the returned JWT
through the internal delegation verifier. The external token is retained only
in request memory. The exchanged token is rejected if its scopes or audience
exceed the request. Workload identity or mTLS is preferred; a checksum-bound
mounted client-secret file is a compatibility option.

Promotion to NGKG 1.1 requires native tests against both service binaries,
golden opaque-token regression, signed-token positive and negative corpora,
issuer/audience/algorithm/time/tenant/scope failures, key rotation, duplicate
KID, stale-key expiry, exchange scope escalation, audit subject/actor, and live
HA readiness during identity-provider disruption.
