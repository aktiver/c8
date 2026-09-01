#!/usr/bin/env python3
"""Source-level gates for Enterprise Stabilization Phase 1."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    manifest = {}
    for line in text("FILE_MANIFEST_SHA256.txt").splitlines():
        digest, relative = line.split("  ", 1)
        manifest[relative.removeprefix("./")] = digest
    for relative in (
        "Cargo.lock",
        "vendor/oxigraph/Cargo.lock",
        "vendor/spareval/Cargo.lock",
        "vendor/sparopt/Cargo.lock",
    ):
        path = ROOT / relative
        require(path.is_file(), f"missing reproducible build input: {relative}")
        require(manifest.get(relative) == sha256(path), f"manifest mismatch: {relative}")

    api = text("services/api/src/main.rs")
    request = api[api.index("struct CreateIngestionRequest"):]
    request = request[:request.index("}")]
    require("target_snapshot_id: Uuid" in request, "ingestion target snapshot must be required")

    for relative in ("services/api/src/auth.rs", "services/online-serving/src/auth.rs"):
        auth = text(relative)
        require('"imports:create"' in auth and '"imports:read"' in auth, f"import scopes missing: {relative}")
    token_schema = json.loads(text("contracts/api-auth-tokens.schema.json"))
    scopes = token_schema["properties"]["tokens"]["items"]["properties"]["scopes"]["items"]["enum"]
    require("imports:create" in scopes and "imports:read" in scopes, "token schema lacks import scopes")

    serving = text("services/online-serving/src/main.rs")
    fragment = serving[serving.index("async fn execute_fragment("):]
    fragment = fragment[:fragment.index("fn arrow_binding_response(")]
    require(fragment.index("authorization_state") < fragment.index("semantic_state"), "fragment auth must precede semantic loading")
    require("preauthorized.graph_iris.contains(&fragment.graph_iri)" in fragment, "fragment graph authorization missing")

    migration2 = text("migrations/0002_atomic_compilation.sql")
    require("BEFORE UPDATE ON operation" in migration2, "operation guard must cover every update")
    require("NEW.created_at IS DISTINCT FROM OLD.created_at" in migration2, "created_at immutability missing")
    migration6 = text("migrations/0006_named_datasets.sql")
    require(migration6.index("NO FORCE ROW LEVEL SECURITY") < migration6.index("UPDATE dataset"), "RLS must be relaxed before backfill")
    require(migration6.rindex("FORCE ROW LEVEL SECURITY") > migration6.index("UPDATE dataset"), "forced RLS must be restored")

    recovery = text("services/storage-recovery-operator/src/main.rs")
    require("&PatchParams::default(),\n        &Patch::Merge" in recovery, "Merge status patch must use default PatchParams")
    recovery_chart = text("charts/ngkg-platform/templates/storage-recovery-operator.yaml")
    require("mountPath: /tmp" in recovery_chart and "name: operator-tmp" in recovery_chart, "writable bounded /tmp missing")

    schema = json.loads(text("charts/ngkg-workloads/values.schema.json"))
    values = yaml.safe_load(text("charts/ngkg-workloads/values.yaml"))
    reasoning = schema["properties"]["onlineReasoning"]
    missing = set(reasoning["required"]) - set(values["onlineReasoning"])
    extras = set(values["onlineReasoning"]) - set(reasoning["properties"])
    require(not missing, f"onlineReasoning required values missing: {sorted(missing)}")
    require(not extras, f"onlineReasoning schema rejects values: {sorted(extras)}")
    require(values["metrics"]["cpuUtilizationTargetPercent"] == 80, "CPU HPA target must be 80")
    require(values["metrics"]["memoryUtilizationTargetPercent"] == 80, "RAM HPA target must be 80")

    policies = text("charts/ngkg-workloads/templates/network-policies.yaml")
    require("dependencyCidrs must contain" in policies, "empty dependency CIDRs must fail rendering")
    workloads = text("charts/ngkg-workloads/templates/online-data-plane.yaml")
    for owner in ("sparqlQueryProcessing.owner", "sparqlFragmentProcessing.owner", "parquetHydration.owner"):
        require(owner in workloads, f"HPA replica ownership missing: {owner}")
    require("phase40-online-ceilings-{{ toJson .Values.phase40.directAdmission | sha256sum | trunc 12 }}" in workloads, "content-addressed online ceiling reference missing")
    platform_ceiling = text("charts/ngkg-platform/templates/phase40-reference-ceilings.yaml")
    require("sha256sum | trunc 12" in platform_ceiling and "immutable: true" in platform_ceiling, "immutable reference ceilings must be content-addressed")

    operator = text("services/operator/src/main.rs")
    require("REFERENCE_COMPILE_OPTION_NAMES.contains" in operator, "reference worker argv allowlist missing")
    require((ROOT / ".dockerignore").is_file(), ".dockerignore missing")
    for relative in (
        "deploy/online-serving/Dockerfile",
        "deploy/direct-reasoner-worker/Dockerfile",
        "deploy/reference-worker/Dockerfile",
    ):
        dockerfile = text(relative)
        require("JAVA_RUNTIME_IMAGE" in dockerfile, f"Java runtime stage missing: {relative}")
    require((ROOT / ".github/workflows/enterprise-ci.yml").is_file(), "enterprise CI missing")
    require((ROOT / "charts/ngkg-workloads/profiles/enterprise-secure.yaml").is_file(), "enterprise Helm overlay missing")
    print("Enterprise Stabilization Phase 1 source gates: PASS")


if __name__ == "__main__":
    main()
