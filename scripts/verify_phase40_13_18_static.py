#!/usr/bin/env python3
"""Fail-closed source-contract checks for Phase 40.13.18."""

from __future__ import annotations

import json
import pathlib
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(relative: str, *tokens: str) -> str:
    text = (ROOT / relative).read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{relative} is missing {token!r}")
    return text


def main() -> int:
    federation = require(
        "crates/ngkg-federation/src/lib.rs",
        "FEDERATION_REGISTRY_FORMAT_VERSION",
        "impl DefaultServiceHandler for FederationServiceHandler",
        "Policy::none()",
        ".resolve(host, pinned)",
        "resolve_public_addresses",
        "ForbiddenAddress",
        "max_calls_per_query",
        "max_concurrent_calls",
        "max_pending_calls",
        "max_response_bytes",
        "bearer_token_env",
        "tenant_ids",
        "FederationQueryEvidence",
    )
    reference = require(
        "crates/ngkg-reference/src/query.rs",
        "execute_compiled_query_with_dataset_federated_cancellable",
        "execute_entailment_rewritten_query_with_dataset_federated_cancellable",
        "with_default_service_handler",
    )
    serving = require(
        "services/online-serving/src/main.rs",
        "NGKG_FEDERATION_REGISTRY_FILE",
        "NGKG_FEDERATION_REGISTRY_SHA256",
        "execute_uncertified_federated_compiled_with_dataset_bounded_cancellable",
        "execute_exact_entailment_rewritten_federated_with_dataset_bounded_cancellable",
        "ngkg_federation_pending_calls",
        "sd:BasicFederatedQuery",
    )
    registry = json.loads(
        (ROOT / "config/federation-registry.example.json").read_text(encoding="utf-8")
    )
    if registry["formatVersion"] != 1 or not registry["endpoints"]:
        raise RuntimeError("example registry is empty or version-incompatible")
    endpoint = registry["endpoints"][0]
    if (
        not endpoint["iri"].startswith("https://")
        or "bearerToken" in endpoint
        or not endpoint["tenantIds"]
    ):
        raise RuntimeError("registry must contain only HTTPS endpoints and secret references")
    cases = json.loads(
        (ROOT / "test-corpus/federation/phase40.13.18-service-cases.json").read_text(
            encoding="utf-8"
        )
    )["cases"]
    queries = "\n".join(case["query"] for case in cases)
    for token in ("SERVICE <", "SERVICE ?service", "SERVICE SILENT", "OPTIONAL"):
        if token not in queries:
            raise RuntimeError(f"federation matrix omits {token}")
    openapi = yaml.safe_load((ROOT / "api/online-openapi.yaml").read_text(encoding="utf-8"))
    if "FederationQueryEvidence" not in openapi["components"]["schemas"]:
        raise RuntimeError("online OpenAPI omits federation evidence")
    values = yaml.safe_load((ROOT / "charts/ngkg-workloads/values.yaml").read_text(encoding="utf-8"))
    if values["onlineServing"]["federationEnabled"]:
        raise RuntimeError("federation must default closed until an endpoint registry is mounted")
    if "federationCidrs" not in values["networking"]:
        raise RuntimeError("federation egress CIDRs are absent")
    require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        "federationRegistrySecret",
        "federationCredentialsSecret",
        "NGKG_FEDERATION_REGISTRY_SHA256",
    )
    require(
        "charts/ngkg-workloads/templates/network-policies.yaml",
        "ngkg-federation-egress",
        "federationCidrs",
        "port: 443",
    )
    require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        "ngkg_federation_pending_calls",
        "federationPendingCallsAverageTarget",
        "cpu",
        "memory",
    )
    feature_matrix = json.loads(
        (ROOT / "conformance/sparql11-feature-matrix.json").read_text(encoding="utf-8")
    )
    service = next(feature for feature in feature_matrix["features"] if feature["id"] == "pattern.service")
    if service["layers"]["reference"] != "implemented" or service["layers"]["online"] != "implemented":
        raise RuntimeError("SPARQL SERVICE inventory was not updated")
    combined = federation + reference + serving
    if "align_ontology" in combined or "raw_data_mapping" in combined:
        raise RuntimeError("ontology alignment or raw-data mapping entered federation runtime")
    print("phase 40.13.18 static qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.18 static qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
