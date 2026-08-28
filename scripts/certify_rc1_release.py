#!/usr/bin/env python3
"""Issue RC1 only from live prerequisites, a frozen tree, signed artifacts, and exact build evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
import sys
from typing import Any

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
VERSION = "1.0.0-rc.1"
KINDS = {"sparql11", "authorized-rdf-dataset", "owl2-dl", "distributed-reasoning", "distributed-query-runtime", "atomic-publication", "federation", "storage-recovery", "autoscaling", "enterprise-security", "standards", "performance-capacity", "kubernetes-release", "semantic-context-graph"}
CLASSES = {"source-archive", "image-index", "helm-charts", "kubernetes-bundle", "crds", "migrations", "utilities", "api-schemas", "sbom-spdx", "sbom-cyclone-dx", "provenance", "qualification-evidence", "documentation", "checksums"}
SHA_CHARS = set("0123456789abcdef")


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def valid_sha(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= SHA_CHARS


def load(path: pathlib.Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream) if path.suffix in {".yaml", ".yml"} else json.load(stream)


def validate_ledger(value: dict[str, Any]) -> str:
    if value.get("formatVersion") != 1 or value.get("releaseVersion") != VERSION or value.get("complete") is not True or not valid_sha(value.get("releaseSha256")):
        raise ValueError("RC1 prerequisite ledger header is invalid")
    release_sha = value["releaseSha256"]
    seen = set()
    for item in value.get("prerequisites", []):
        if item.get("kind") in seen or item.get("kind") not in KINDS:
            raise ValueError("RC1 prerequisite kind is duplicated or unknown")
        seen.add(item["kind"])
        if item.get("evidenceClass") != "live-production-qualification" or item.get("complete") is not True or item.get("failureCount") != 0 or item.get("synthetic") is not False or item.get("subjectSha256") != release_sha or not valid_sha(item.get("certificateSha256")):
            raise ValueError(f"RC1 prerequisite is not live, complete, and same-subject: {item.get('kind')}")
    if seen != KINDS: raise ValueError("RC1 prerequisite coverage is incomplete")
    return release_sha


def validate_artifacts(value: dict[str, Any], release_sha: str, test_harness: bool) -> tuple[str, str]:
    if value.get("formatVersion") != 1 or value.get("releaseVersion") != VERSION or value.get("releaseSha256") != release_sha or value.get("complete") is not True:
        raise ValueError("RC1 artifact manifest header is invalid")
    classes, paths, root = set(), set(), hashlib.sha256()
    for item in sorted(value.get("artifacts", []), key=lambda row: row.get("path", "")):
        if item.get("class") in classes or item.get("class") not in CLASSES or item.get("path") in paths or not valid_sha(item.get("sha256")) or not valid_sha(item.get("signatureSha256")) or not item.get("mediaType"):
            raise ValueError("RC1 artifact is missing, duplicate, unsigned, or invalid")
        classes.add(item["class"]); paths.add(item["path"])
        if item["path"].startswith("oci://"):
            if item["class"] != "image-index" or "@sha256:" not in item["path"]:
                raise ValueError("only the immutable image index may use an OCI reference")
        else:
            supplied = pathlib.Path(item["path"])
            if supplied.is_absolute() and not test_harness:
                raise ValueError("absolute artifact paths are forbidden for publication")
            path = supplied if supplied.is_absolute() else ROOT / supplied
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
                raise ValueError(f"RC1 artifact bytes do not match: {item['path']}")
        root.update(item["path"].encode()); root.update(b"\0"); root.update(item["sha256"].encode()); root.update(b"\0"); root.update(item["signatureSha256"].encode()); root.update(b"\0")
    if classes != CLASSES: raise ValueError("RC1 signed artifact class coverage is incomplete")
    return digest(value), root.hexdigest()


def validate_supply(value: dict[str, Any]) -> None:
    hashes = ["signatureReportSha256", "provenanceReportSha256", "secretScanSha256", "licenseReportSha256", "vulnerabilityReportSha256"]
    if any(not valid_sha(value.get(name)) for name in hashes) or value.get("unapprovedCriticalCves") != 0 or value.get("unapprovedHighCves") != 0 or value.get("embeddedCredentials") != 0 or any(value.get(name) is not True for name in ["licensePolicyComplete", "runtimeHardeningComplete", "workloadIdentityComplete", "networkPolicyComplete", "complete"]):
        raise ValueError("RC1 supply-chain or security gate failed")


def validate_reproducible(value: dict[str, Any]) -> None:
    if not valid_sha(value.get("builderAManifestSha256")) or value.get("builderAManifestSha256") != value.get("builderBManifestSha256") or not valid_sha(value.get("sourceSha256")) or any(value.get(name) is not True for name in ["networkControlled", "dependenciesLocked", "timestampsNormalized", "complete"]):
        raise ValueError("RC1 isolated builds are not reproducible")


def validate_support(value: dict[str, Any]) -> None:
    providers = value.get("spec", {}).get("providers", [])
    if {item.get("provider") for item in providers} != {"rke", "rke2", "eks", "aks", "gke"} or any(item.get("qualified") is not True or not item.get("testedKubernetesVersions") or not valid_sha(item.get("evidenceSha256")) for item in providers):
        raise ValueError("RC1 exact Kubernetes support matrix is not qualified")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=pathlib.Path, required=True)
    parser.add_argument("--freeze", type=pathlib.Path, required=True)
    parser.add_argument("--source-files-manifest", type=pathlib.Path, default=ROOT / "release/1.0.0-rc1/source-input-files.sha256")
    parser.add_argument("--artifacts", type=pathlib.Path, required=True)
    parser.add_argument("--supply-chain", type=pathlib.Path, required=True)
    parser.add_argument("--reproducible-build", type=pathlib.Path, required=True)
    parser.add_argument("--support-matrix", type=pathlib.Path, required=True)
    parser.add_argument("--known-issues", type=pathlib.Path, required=True)
    parser.add_argument("--acceptance-plan", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--test-harness", action="store_true")
    args = parser.parse_args()
    check = subprocess.run([sys.executable, str(ROOT / "scripts/build_rc1_freeze.py"), "--output", str(args.freeze), "--source-files-output", str(args.source_files_manifest), "--check"], cwd=ROOT, capture_output=True, text=True, check=False)
    if check.returncode != 0: raise ValueError(f"RC1 compatibility freeze drifted: {check.stderr}{check.stdout}")
    ledger, freeze, artifacts = load(args.ledger), load(args.freeze), load(args.artifacts)
    release_sha = validate_ledger(ledger)
    if freeze.get("releaseVersion") != VERSION or freeze.get("complete") is not True or not freeze.get("changesRequireRcDefect") or not valid_sha(freeze.get("sourceManifestSha256")):
        raise ValueError("RC1 freeze manifest is invalid")
    artifact_sha, artifact_root = validate_artifacts(artifacts, release_sha, args.test_harness)
    validate_supply(load(args.supply_chain)); validate_reproducible(load(args.reproducible_build)); validate_support(load(args.support_matrix))
    certificate = {"formatVersion": 1, "releaseVersion": VERSION, "releaseSha256": release_sha,
        "prerequisiteLedgerSha256": digest(ledger), "freezeManifestSha256": digest(freeze),
        "artifactManifestSha256": artifact_sha, "artifactRootSha256": artifact_root,
        "supportMatrixSha256": hashlib.sha256(args.support_matrix.read_bytes()).hexdigest(),
        "knownIssuesSha256": hashlib.sha256(args.known_issues.read_bytes()).hexdigest(),
        "acceptancePlanSha256": hashlib.sha256(args.acceptance_plan.read_bytes()).hexdigest(),
        "failureCount": 0, "publishable": not args.test_harness, "complete": True}
    if args.test_harness: certificate["testHarness"] = True
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical(certificate) + b"\n")
    print(json.dumps({"publishable": certificate["publishable"], "certificateSha256": digest(certificate)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"RC1 certification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
