#!/usr/bin/env python3
"""Verify every Phase 6 evidence family and emit the statement to be signed."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

from phase6_common import atomic_json, canonical, evidence_root, load_json, require, sha256_bytes, sha256_file, valid_sha256
from evidence_security import verify_reference, verify_signed_statement

PROVIDERS = {"rke", "rke2", "eks", "aks", "gke"}
PREREQUISITES = {
    "phase3-certificate.json": {"oci_supply_chain", "postgres_ha", "rke", "rke2", "eks", "aks", "gke"},
    "phase4-live-certificate.json": {"concurrent_status_writers", "operator_restart", "idempotent_object_retry", "parallel_spill", "datatype_differential", "blank_node_differential", "azure_artifacts", "gcs_artifacts"},
    "phase5-live-certificate.json": {"native_parquet_leaf_scan", "multinode_partition_barrier", "scalar_public_fallback_absent", "sparql_multiset_differential", "property_path_differential", "hpa_cpu_80", "hpa_memory_80"},
}


def validate_prerequisite(path: Path, required: set[str], subject: str, identity: str, issuer: str) -> dict[str, Any]:
    bundle = path.with_name(path.name.removesuffix(".json") + ".sigstore.json")
    document = verify_signed_statement(path, bundle, subject=subject, identity_regexp=identity, oidc_issuer=issuer)
    require(document.get("status") == "QUALIFIED" and document.get("signed") is True, f"{path.name} is not signed and QUALIFIED")
    scenario_ids = [row.get("id") for row in document.get("scenarios", [])]
    require(len(scenario_ids) == len(set(scenario_ids)), f"{path.name} has duplicate scenario IDs")
    passed = {row.get("id") for row in document.get("scenarios", []) if row.get("status") == "PASS" and valid_sha256(row.get("evidenceSha256"))}
    require(required <= passed, f"{path.name} has incomplete scenario coverage")
    for row in document.get("scenarios", []):
        if row.get("id") in required:
            verify_reference(path.parent, row, subject, row["id"])
    signature = document.get("signature", {})
    require(signature.get("keylessIdentity") and valid_sha256(signature.get("bundleSha256")), f"{path.name} lacks identity-bound signature evidence")
    return document


def validate_differential(path: Path, subject: str) -> dict[str, Any]:
    document = load_json(path)
    require(document.get("kind") == "Phase6DifferentialEvidence" and document.get("subjectSha256") == subject, "differential subject mismatch")
    require(document.get("nativeCutoverMode") == "required" and document.get("oracleIsolation") == "QUALIFICATION_ONLY", "differential execution boundary is invalid")
    require(document.get("oracleProductionDependency") is False and document.get("mismatchCount") == 0, "native/oracle differential failed")
    require(document.get("caseCount", 0) >= 4 and document.get("measuredRepetitions", 0) >= 3, "differential coverage is insufficient")
    require(document.get("status") == "PASS" and document.get("synthetic") is False and document.get("complete") is True, "differential evidence is incomplete")
    require(valid_sha256(document.get("semanticRootSha256")), "differential semantic root is invalid")
    scenario_ids = [row.get("id") for row in document.get("scenarios", [])]
    require(len(scenario_ids) == len(set(scenario_ids)), "differential evidence has duplicate scenario IDs")
    for row in document.get("scenarios", []):
        verify_reference(path.parent / "cases", row, subject, row["id"])
    return document


def validate_provider(path: Path, provider: str, subject: str) -> dict[str, Any]:
    document = load_json(path)
    require(document.get("kind") == "Phase6ProviderEvidence" and document.get("provider") == provider, f"provider evidence mismatch: {provider}")
    require(document.get("subjectSha256") == subject and valid_sha256(document.get("imageLockSha256")), f"provider subject/image lock mismatch: {provider}")
    require(document.get("failureCount") == 0 and document.get("status") == "PASS" and document.get("synthetic") is False and document.get("complete") is True, f"provider failed: {provider}")
    require(document.get("inventory", {}).get("readyNodes", 0) >= 3 and document.get("inventory", {}).get("failureDomains", 0) >= 2, f"provider is not HA: {provider}")
    hpas = document.get("autoscaling", {}).get("hpas", [])
    require(hpas and all(row.get("cpuPercent") == 80 and row.get("memoryPercent") == 80 for row in hpas), f"80% CPU/RAM HPA evidence missing: {provider}")
    capacity = document.get("capacity", {})
    require(capacity.get("resources", {}).get("physicalNodes", 0) >= 2 and capacity.get("trialCount", 0) > 0, f"multinode capacity evidence missing: {provider}")
    chaos = document.get("chaos", {})
    require(chaos.get("failureCount") == 0 and {row.get("scenario") for row in chaos.get("scenarios", [])} >= {"worker_node_loss", "postgres_failover", "object_corruption"}, f"chaos coverage missing: {provider}")
    integration = document.get("providerIntegrations", {})
    require(integration.get("workloadIdentity") is True and integration.get("longLivedCloudCredentials") is False, f"workload identity failed: {provider}")
    require(integration.get("trigIngestion") is True and integration.get("artifactRoundTrip") is True, f"cloud storage failed: {provider}")
    require(integration.get("gpuWorkloadObserved") is True and integration.get("gpuScaleFromZero") is True and int(integration.get("gpuTimeNs", 0)) > 0, f"GPU qualification missing: {provider}")
    require(integration.get("postCutoverTenantIsolation") is True, f"post-cutover tenant isolation missing: {provider}")
    scenario_ids = [row.get("id") for row in document.get("scenarios", [])]
    require(len(scenario_ids) == len(set(scenario_ids)), f"duplicate provider scenario IDs: {provider}")
    for row in document.get("scenarios", []):
        verify_reference(path.parent / "scenarios", row, subject, row["id"])
    return document


def validate_supply(path: Path, subject: str) -> dict[str, Any]:
    document = load_json(path)
    require(document.get("kind") == "Phase6SupplyChainEvidence" and document.get("subjectSha256") == subject, "supply-chain subject mismatch")
    require(document.get("imageCount") == 13 and document.get("signed") is True, "thirteen signed Phase 8 images were not proven")
    require(document.get("spdxComplete") is True and document.get("cycloneDxComplete") is True, "SBOM formats are incomplete")
    require(document.get("unapprovedCritical") == 0 and document.get("unapprovedHigh") == 0, "unapproved vulnerabilities remain")
    require(document.get("status") == "PASS" and document.get("synthetic") is False and document.get("complete") is True, "supply-chain evidence is incomplete")
    return document


def validate_reproducible(path: Path, subject: str) -> dict[str, Any]:
    document = load_json(path)
    require(document.get("kind") == "Phase6ReproducibleBuildEvidence" and document.get("subjectSha256") == subject, "reproducible-build subject mismatch")
    require(all(document.get(key) is True for key in ("distinctBuilders", "networkControlled", "dependenciesLocked", "timestampsNormalized", "complete")), "reproducible-build policy failed")
    require(document.get("status") == "PASS" and document.get("synthetic") is False and valid_sha256(document.get("artifactRootSha256")), "reproducible-build evidence is invalid")
    return document


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-root", required=True, type=Path)
    parser.add_argument("--subject-sha256", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--certificate-identity-regexp", required=True)
    parser.add_argument("--certificate-oidc-issuer", required=True)
    args = parser.parse_args()
    root = args.evidence_root.resolve()
    subject = args.subject_sha256
    require(valid_sha256(subject), "invalid release subject")
    records = []
    for name, scenarios in PREREQUISITES.items():
        path = root / "prerequisites" / name
        require(path.is_file(), f"missing prerequisite: {name}")
        validate_prerequisite(path, scenarios, subject, args.certificate_identity_regexp, args.certificate_oidc_issuer)
        records.append({"id": name, "evidencePath": path.relative_to(root).as_posix(), "sha256": sha256_file(path)})
    differential_path = root / "differential/differential-evidence.json"
    validate_differential(differential_path, subject)
    records.append({"id": "native-oracle-differential", "evidencePath": differential_path.relative_to(root).as_posix(), "sha256": sha256_file(differential_path)})
    image_lock_sha: str | None = None
    for provider in sorted(PROVIDERS):
        path = root / "providers" / provider / "provider-evidence.json"
        document = validate_provider(path, provider, subject)
        require(image_lock_sha in {None, document["imageLockSha256"]}, "providers used different image locks")
        image_lock_sha = document["imageLockSha256"]
        records.append({"provider": provider, "id": "provider-capacity-chaos", "evidencePath": path.relative_to(root).as_posix(), "sha256": sha256_file(path)})
    supply_path = root / "supply-chain/supply-chain-evidence.json"
    supply = validate_supply(supply_path, subject)
    require(supply.get("imageLockSha256") == image_lock_sha, "supply chain and providers used different images")
    records.append({"id": "oci-signatures-sboms", "evidencePath": supply_path.relative_to(root).as_posix(), "sha256": sha256_file(supply_path)})
    reproducible_path = root / "reproducibility/reproducible-build-evidence.json"
    reproducible = validate_reproducible(reproducible_path, subject)
    records.append({"id": "reproducible-build", "evidencePath": reproducible_path.relative_to(root).as_posix(), "sha256": sha256_file(reproducible_path)})
    defects_path = root / "defects/defect-ledger.json"
    defects = load_json(defects_path)
    require(defects.get("subjectSha256") == subject and defects.get("complete") is True, "defect ledger is incomplete")
    require(all(row.get("releaseBlocking") is False for row in defects.get("defects", [])), "release-blocking defect remains")
    require(not any(row.get("unresolved") is True and row.get("severity") in {"critical", "high"} for row in defects.get("defects", [])), "unresolved critical/high defect remains")
    records.append({"id": "defect-ledger", "evidencePath": defects_path.relative_to(root).as_posix(), "sha256": sha256_file(defects_path)})
    statement = {
        "formatVersion": 1,
        "kind": "EnterpriseStabilizationPhase6Statement",
        "subjectSha256": subject,
        "imageLockSha256": image_lock_sha,
        "providers": sorted(PROVIDERS),
        "nativeCutoverMode": "required",
        "oracleProductionDependency": False,
        "semanticMismatchCount": 0,
        "failedScenarioCount": 0,
        "unapprovedCriticalCves": 0,
        "unapprovedHighCves": 0,
        "evidence": sorted(records, key=lambda row: (row.get("provider", ""), row["id"])),
        "evidenceRootSha256": evidence_root(records),
        "reproducibleArtifactRootSha256": reproducible["artifactRootSha256"],
        "status": "QUALIFIED",
        "synthetic": False,
        "complete": True,
    }
    atomic_json(args.output.resolve(), statement)
    print(json.dumps({"status": "QUALIFIED", "statementSha256": sha256_file(args.output.resolve()), "evidenceRootSha256": statement["evidenceRootSha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, TypeError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"Phase 6 issuance blocked: {error}", file=sys.stderr)
        raise SystemExit(1)
