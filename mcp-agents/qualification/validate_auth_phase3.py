#!/usr/bin/env python3
"""Deterministic structural gate for Phase 3 authentication invariants."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def document(relative: str) -> dict:
    with (ROOT / relative).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def require(source: str, values: list[str]) -> None:
    missing = [value for value in values if value not in source]
    if missing:
        raise SystemExit(f"missing Phase 3 invariant(s): {', '.join(missing)}")


def main() -> None:
    for schema in (
        "contracts/ngkg-delegation-claims.schema.json",
        "contracts/ngkg-1.1-authenticated-identity.schema.json",
        "contracts/oauth-protected-resource-metadata.schema.json",
        "charts/ngkg-agents/values.schema.json",
    ):
        document(schema)

    shared = text("crates/ngkg-auth/src/lib.rs")
    delegation = text("crates/ngkg-auth/src/delegation.rs")
    exchange = text("crates/ngkg-auth/src/exchange.rs")
    opaque = text("crates/ngkg-auth/src/opaque.rs")
    gateway = text("services/mcp-gateway/src/main.rs")
    middleware = text("services/mcp-gateway/src/auth.rs")
    chart = text("charts/ngkg-agents/templates/gateway.yaml")

    require(shared, ["enum AuthenticationConfiguration", "Opaque(", "Delegation(", "DelegationExchange", "No variant attempts a different mode"])
    require(opaque, ["token file checksum mismatch", "queries:execute", "duplicate opaque token hash"])
    require(delegation, ["Policy::none", "set_audience", "set_issuer", "set_required_spec_claims", "validate_nbf = true", "MAXIMUM_JWKS_BYTES", "MAXIMUM_JWKS_KEYS", "last_known_good_grace", "required_typ", "RS256", "EdDSA"])
    require(exchange, ["token-exchange", "requested_token_type", "is_subset", "Policy::none", "MAXIMUM_EXCHANGE_RESPONSE_BYTES", "ClientSecretFile", "WorkloadIdentity"])
    require(gateway, ["NGKG_AUTH_MODE must be opaque or delegation", "/.well-known/oauth-protected-resource", "authenticator.ready()", "NGKG_AUTH_EXCHANGE_ENABLED"])
    require(middleware, ["authenticated.upstream_authorization", "authenticated.identity"])
    require(chart, ["ngkg.io/auth-mode", "NGKG_AUTH_MODE", "client-secret-file", "NGKG_AUTH_JWKS_URL"])

    forbidden = ("Algorithm::HS256", "Algorithm::HS384", "Algorithm::HS512")
    if any(value in delegation for value in forbidden):
        raise SystemExit("symmetric JWT verification is forbidden")
    if "or_else(|_|" in middleware or "or_else(|_|" in shared:
        raise SystemExit("authentication-mode fallback is forbidden")
    if "principal_id" in middleware:
        raise SystemExit("gateway middleware must use shared subject/actor identity")

    claims = document("contracts/ngkg-delegation-claims.schema.json")
    required = set(claims["required"])
    if not {"iss", "aud", "sub", "exp", "nbf", "iat", "jti", "tokenUse", "ngkg"}.issubset(required):
        raise SystemExit("delegation claim contract is incomplete")
    ngkg = claims["properties"]["ngkg"]
    if ngkg.get("additionalProperties") is not False:
        raise SystemExit("trusted NGKG claim namespace must be closed")

    print("Phase 3 delegation-auth structural gate: PASS")


if __name__ == "__main__":
    main()
