#!/usr/bin/env python3
"""Fail-closed static contract checks for the Phase 36 compliance baseline."""

from __future__ import annotations

import json
import pathlib
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, tokens: tuple[str, ...] = ()) -> str:
    target = ROOT / path
    if not target.is_file():
        raise RuntimeError(f"missing required file: {path}")
    text = target.read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{path} is missing required token: {token}")
    return text


def strict_json_schema(path: str) -> dict:
    value = json.loads(require(path))
    if value.get("additionalProperties") is not False:
        raise RuntimeError(f"{path} must reject unknown top-level fields")
    return value


def main() -> int:
    workspace = require(
        "Cargo.toml",
        (
            '"crates/ngkg-dataset"',
            'rust-version = "1.97.1"',
            'oxigraph = { version = "=0.5.9", default-features = false }',
            'spargebra = { version = "=0.4.6", features = ["standard-unicode-escaping"] }',
        ),
    )
    if (':' + 'latest') in workspace or (' ' + 'latest') in workspace:
        raise RuntimeError("workspace dependency policy contains an unpinned latest version")

    reference = require(
        "crates/ngkg-reference/src/lib.rs",
        (
            "fn certified_result(",
            "canonical_query_result_sha256(",
            "fresh semantic result differs from its offline form-aware certificate",
            "certificate.result_hash_version != QUERY_RESULT_HASH_VERSION",
            "execute_with_dataset(",
        ),
    )
    certified = reference.index("fn certified_result(")
    hashed = reference.index("canonical_query_result_sha256(", certified)
    compared = reference.index("certificate.observed_result_sha256", hashed)
    returned = reference.index("Ok(CertifiedSemanticResult", compared)
    if not certified < hashed < compared < returned:
        raise RuntimeError("fresh local results can escape before the result certificate is checked")

    online = require(
        "services/online-serving/src/main.rs",
        (
            "authorized_graph_set_sha256",
            "active_dataset_sha256",
            "dataset_selection_source",
            "validate_cached_query_response(",
            "standards_features: StandardsFeatureGates::default()",
            ".route(\"/openapi.json\", get(openapi_json))",
            ".route(\"/docs\", get(swagger_ui_root))",
            ".route(\"/docs/{*asset}\", get(swagger_ui_asset))",
            "/v1/datasets/{dataset_id}/sparql",
        ),
    )
    if "NGKG_ENABLE_SPARQL_11_CLAIM" in online or "NGKG_ENABLE_OWL_DIRECT_CLAIM" in online:
        raise RuntimeError("standards claims must not be enabled by operator environment booleans")

    auth = require(
        "services/api/src/auth.rs",
        (
            '"sources:write"',
            '"queries:execute"',
            "authentication token file checksum does not match its deployment",
            "graph_authorization_labels",
        ),
    )
    if "MAX_TOKEN_FILE_BYTES" not in auth:
        raise RuntimeError("authentication file parsing is not byte bounded")

    api = require(
        "services/api/src/main.rs",
        (
            'put(upload_trig_source)',
            '"application/trig"',
            "body.into_data_stream()",
            "NGKG_MAX_TRIG_UPLOAD_BYTES",
            "NGKG_MAX_TRIG_UPLOAD_QUADS",
            "NGKG_MAX_TRIG_UPLOADS_IN_FLIGHT",
            "dataset_exists(identity.tenant_id, dataset_id)",
            "put_file_immutable(",
            "sync_all()",
        ),
    )
    if "to_bytes(body" in api:
        raise RuntimeError("TriG upload path buffers the entire HTTP request")

    reasoner = require(
        "crates/ngkg-reference/src/reasoner.rs",
        (
            "report.format_version != 5",
            "!report.profile_valid",
            "!report.consistency_checked",
            "!report.consistent",
        ),
    )
    java = require(
        "adapters/hermit-reasoner/src/main/java/io/ngkg/reasoner/Main.java",
        (
            "new OWL2DLProfile().checkOntology(merged)",
            "reasoner.isConsistent()",
            "unmapped ontology import",
            "aggregate input SHA-256 mismatch",
        ),
    )
    # Phase 40.6 supersedes the original Phase 36 report envelope with v5. The current report retains
    # datatype/profile bindings and adds checksum-bound global consistency qualification evidence.
    if "new Report(\n                    5," not in java and "new Report(\n                    5," not in java.replace("\r\n", "\n"):
        raise RuntimeError("reasoner report contract is not current Phase 40.6 version 5")

    strict_json_schema("contracts/api-auth-tokens.schema.json")
    strict_json_schema("contracts/reasoner-report.schema.json")
    strict_json_schema("contracts/trig-source-metadata.schema.json")

    suite_lock = json.loads(require("conformance/w3c-rdf-tests.lock.json"))
    expected_suite_lock = {
        "formatVersion": 1,
        "repository": "https://github.com/w3c/rdf-tests.git",
        "commit": "8af71fed933539d09d5f4658fb1ea7ba4c8e30b9",
        "requiredManifests": [
            "rdf/rdf11/rdf-trig/manifest.ttl",
            "sparql/sparql11/manifest-sparql11-query.ttl",
            "sparql/sparql11/manifest-sparql11-results.ttl",
            "sparql/sparql11/protocol/manifest.ttl",
            "sparql/sparql11/service-description/manifest.ttl",
            "sparql/sparql11/entailment/manifest.ttl",
        ],
    }
    if suite_lock != expected_suite_lock:
        raise RuntimeError("W3C RDF/SPARQL suite lock is not the approved immutable snapshot")
    require(
        "scripts/fetch_w3c_conformance.py",
        (
            'protocol.version=2',
            '"--depth=1"',
            "verify_checkout(",
            '"status", "--porcelain=v1"',
            "os.replace(staging, destination)",
        ),
    )

    control_openapi = yaml.safe_load(require("api/openapi.yaml"))
    source = control_openapi["paths"]["/v1/datasets/{datasetId}/sources/{sourceId}"]["put"]
    if "application/trig" not in source["requestBody"]["content"]:
        raise RuntimeError("control OpenAPI does not expose application/trig ingestion")

    online_openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    for path in (
        "/v1/datasets/{datasetId}/sparql",
        "/v1/datasets/{datasetId}/sparql/service-description",
        "/openapi.yaml",
        "/openapi.json",
        "/docs",
    ):
        if path not in online_openapi["paths"]:
            raise RuntimeError(f"online OpenAPI is missing {path}")

    ci = require(
        "scripts/ci_release.sh",
        (
            "command -v cargo",
            "command -v mvn",
            "command -v helm",
            "command -v kubectl",
            "test -f Cargo.lock",
            "cargo clippy --locked",
            "cargo test --locked",
            "mvn -B -ntp -f adapters/hermit-reasoner/pom.xml verify",
            "fetch_w3c_conformance.py",
            "run_w3c_conformance.py",
            "run_cumulative_static_gates.py",
            "NGKG_W3C_SUITE_CACHE",
        ),
    )
    if "|| true" in ci:
        raise RuntimeError("release gates contain a success-forcing fallback")

    platform = yaml.safe_load(require("charts/ngkg-platform/values.yaml"))
    autoscaling = platform["api"]["autoscaling"]
    if not autoscaling["enabled"] or autoscaling["maxReplicas"] <= autoscaling["minReplicas"]:
        raise RuntimeError("API upload plane is not horizontally autoscalable")
    if autoscaling["cpuUtilizationTargetPercent"] > 80 or autoscaling["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("API autoscaling target exceeds the 80 percent enterprise headroom ceiling")
    require(
        "charts/ngkg-platform/templates/api-autoscaling.yaml",
        ("autoscaling/v2", "HorizontalPodAutoscaler", "averageUtilization"),
    )
    require("scripts/validate_platform_values.py", ("scratchSizeLimit", "1..80 headroom envelope"))

    print("Phase 36 static contract verification passed; live build/deploy gates remain mandatory")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 36 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
