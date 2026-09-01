#!/usr/bin/env python3
"""Static, fail-closed source qualification for Enterprise Stabilization Phase 3."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PHASE3 = ROOT / "phase3"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def text(relative: str) -> str:
    path = ROOT / relative
    require(path.is_file() and path.stat().st_size > 0, f"missing file: {relative}")
    return path.read_text(encoding="utf-8")


def load(relative: str):
    return json.loads(text(relative))


def main() -> int:
    catalog = load("phase3/config/images.json")
    names = [item["name"] for item in catalog["images"]]
    require(catalog["formatVersion"] == 1 and len(names) == 12 and len(set(names)) == 12, "image catalog must contain twelve unique images")
    require({item["kind"] for item in catalog["images"]} == {"build", "mirror"}, "build and controlled mirror images are required")
    require(names[-1] == "ngkg-vllm" and catalog["images"][-1]["sourceEnvironment"] == "NGKG_VLLM_SOURCE_IMAGE", "vLLM must be mirrored from a digest-pinned source")

    for item in catalog["images"]:
        if item["kind"] != "build":
            continue
        dockerfile = text(f"{item['context']}/{item['dockerfile']}")
        require("--locked --offline --release" in dockerfile, f"offline locked release build missing: {item['name']}")
        if "hermit-reasoner" in dockerfile:
            require("mvn --batch-mode --no-transfer-progress --offline" in dockerfile, f"offline Maven build missing: {item['name']}")
        require("USER 65532:65532" in dockerfile, f"nonroot runtime missing: {item['name']}")

    supply = text("phase3/scripts/build_supply_chain.sh")
    for marker in ("docker buildx build", "--network=none", "--provenance=mode=max", "--sbom=true", "syft", "grype", "trivy", "cosign sign", "cosign attest", "cosign verify", "crane copy", "require_digest_ref"):
        require(marker in supply, f"supply-chain control missing: {marker}")
    require("highVulnerabilities:0" in supply and "criticalVulnerabilities:0" in supply, "zero high/critical vulnerability gate missing")
    require("verify_toolchain.py" in supply and "NGKG_PHASE3_TOOLCHAIN_LOCK" in supply, "controlled tool binary lock is missing")

    postgres = text("phase3/scripts/qualify_postgres.sh")
    for marker in ("pg_dump", "pg_stat_replication", "_sqlx_migrations", "core_rls_immutability.sql", "rls_immutability.sql", "streaming_replicas"):
        require(marker in postgres, f"PostgreSQL qualification control missing: {marker}")
    core_sql = text("phase3/sql/core_rls_immutability.sql")
    for marker in ("relforcerowsecurity", "operation audit mutation was accepted", "cross-tenant dataset row became visible", "ROLLBACK"):
        require(marker in core_sql, f"core PostgreSQL invariant missing: {marker}")

    scenarios = load("phase3/config/required-scenarios.json")
    require(set(scenarios["providers"]) == {"rke2", "eks", "aks", "gke"}, "provider matrix is incomplete")
    require(len(scenarios["scenarios"]) == 12 and len(set(scenarios["scenarios"])) == 12, "scenario matrix must contain twelve unique gates")
    cluster = text("phase3/scripts/qualify_cluster.py")
    for marker in ("qualificationCluster", "approvalEvidenceSha256", "ready node", "availability zones", "nvidia.com/gpu", "averageUtilization", '== 80', "terminate-node", "checksumFailureRejected", "scaledFromZero", "Bearer", "https://", "tenant_dataset_isolation", "tenant_mcp_memory_tool_isolation"):
        require(marker in cluster, f"cluster qualification control missing: {marker}")

    deploy = text("phase3/scripts/deploy_cluster.sh")
    for marker in ("helm upgrade --install", "--atomic", "--wait-for-jobs", "imageLockSha256", "renderedManifestSha256", "images.reasoner.repository", "vllm.image.repository"):
        require(marker in deploy, f"atomic deployment control missing: {marker}")

    issuer = text("phase3/scripts/verify_and_issue.py")
    for marker in ("cosign", "verify-attestation", "verify-blob", "twelve unique deployable images", "provider evidence matrix is incomplete", "scenario coverage mismatch", "certificateSha256"):
        require(marker in issuer, f"certificate verification control missing: {marker}")

    for schema in ("image-evidence.schema.json", "postgres-evidence.schema.json", "cluster-evidence.schema.json", "phase3-certificate.schema.json"):
        value = load(f"phase3/schemas/{schema}")
        require(value.get("additionalProperties") is False and value.get("$schema", "").endswith("2020-12/schema"), f"strict schema required: {schema}")

    workflow = text(".github/workflows/phase3-controlled-release.yml")
    for marker in ("permissions:", "id-token: write", "environment: ngkg-phase3-release", "build_supply_chain.sh", "qualify_postgres.sh", "deploy_cluster.sh", "qualify_cluster.py", "verify_and_issue.py", "matrix:", "rke2", "eks", "aks", "gke"):
        require(marker in workflow, f"controlled-runner workflow control missing: {marker}")
    print("Enterprise Stabilization Phase 3 static qualification: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
