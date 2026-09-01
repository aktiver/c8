#!/usr/bin/env python3
"""Executable source qualification for the 1.0.0-RC1 freeze and packager."""

from __future__ import annotations

import hashlib
import json
import pathlib
import py_compile
import subprocess
import sys
import tempfile
import tomllib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
VERSION = "1.0.0-rc.1"
KINDS = ["sparql11", "authorized-rdf-dataset", "owl2-dl", "distributed-reasoning", "distributed-query-runtime", "atomic-publication", "federation", "storage-recovery", "autoscaling", "enterprise-security", "standards", "performance-capacity", "kubernetes-release", "semantic-context-graph"]
CLASSES = ["source-archive", "image-index", "helm-charts", "kubernetes-bundle", "crds", "migrations", "utilities", "api-schemas", "sbom-spdx", "sbom-cyclone-dx", "provenance", "qualification-evidence", "documentation", "checksums"]


def require(relative: str, *tokens: str) -> str:
    value = (ROOT / relative).read_text(encoding="utf-8")
    for token in tokens:
        if token not in value:
            raise RuntimeError(f"{relative} is missing {token!r}")
    return value


def run(*args: str, must_fail: bool = False) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=False)
    if (result.returncode == 0) == must_fail:
        raise RuntimeError(f"{' '.join(args)} returned {result.returncode}: {result.stdout}{result.stderr}")
    return result


def write_json(path: pathlib.Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def executable_barrier() -> None:
    with tempfile.TemporaryDirectory(prefix="rc1-harness-") as raw:
        temp = pathlib.Path(raw)
        release_sha = "1" * 64
        ledger = {"formatVersion": 1, "releaseVersion": VERSION, "releaseSha256": release_sha,
                  "prerequisites": [{"kind": kind, "evidenceClass": "live-production-qualification", "certificateSha256": hashlib.sha256(kind.encode()).hexdigest(), "subjectSha256": release_sha, "complete": True, "failureCount": 0, "synthetic": False} for kind in KINDS], "complete": True}
        ledger_path = temp / "ledger.json"; write_json(ledger_path, ledger)
        readiness = temp / "readiness.json"
        run(sys.executable, "scripts/assess_rc1_readiness.py", "--ledger", str(ledger_path), "--output", str(readiness), "--require-publishable")
        synthetic = json.loads(ledger_path.read_text(encoding="utf-8")); synthetic["prerequisites"][0]["evidenceClass"] = "synthetic-only"; synthetic["prerequisites"][0]["synthetic"] = True
        synthetic_path = temp / "synthetic-ledger.json"; write_json(synthetic_path, synthetic)
        run(sys.executable, "scripts/assess_rc1_readiness.py", "--ledger", str(synthetic_path), "--output", str(temp / "synthetic-readiness.json"), "--require-publishable", must_fail=True)

        artifacts = []
        for artifact_class in CLASSES:
            if artifact_class == "image-index":
                path = f"oci://registry.invalid/ngkg@sha256:{'a' * 64}"; artifact_sha = "a" * 64
            else:
                file = temp / f"artifact-{artifact_class}.bin"; file.write_bytes(f"harness:{artifact_class}\n".encode())
                path = str(file.resolve()); artifact_sha = hashlib.sha256(file.read_bytes()).hexdigest()
            artifacts.append({"class": artifact_class, "path": path, "sha256": artifact_sha, "signatureSha256": hashlib.sha256(f"signature:{artifact_class}".encode()).hexdigest(), "mediaType": "application/octet-stream"})
        artifact_manifest = {"formatVersion": 1, "releaseVersion": VERSION, "releaseSha256": release_sha, "artifacts": artifacts, "complete": True}
        artifact_path = temp / "artifacts.json"; write_json(artifact_path, artifact_manifest)
        supply = {"signatureReportSha256": "2" * 64, "provenanceReportSha256": "3" * 64, "secretScanSha256": "4" * 64, "licenseReportSha256": "5" * 64, "vulnerabilityReportSha256": "6" * 64, "unapprovedCriticalCves": 0, "unapprovedHighCves": 0, "embeddedCredentials": 0, "licensePolicyComplete": True, "runtimeHardeningComplete": True, "workloadIdentityComplete": True, "networkPolicyComplete": True, "complete": True}
        reproducible = {"builderAManifestSha256": "7" * 64, "builderBManifestSha256": "7" * 64, "sourceSha256": "8" * 64, "networkControlled": True, "dependenciesLocked": True, "timestampsNormalized": True, "complete": True}
        supply_path, reproducible_path = temp / "supply.json", temp / "reproducible.json"; write_json(supply_path, supply); write_json(reproducible_path, reproducible)
        support = yaml.safe_load((ROOT / "release/1.0.0-rc1/support-matrix.yaml").read_text(encoding="utf-8"))
        for provider in support["spec"]["providers"]:
            provider["qualified"] = True; provider["testedKubernetesVersions"] = ["1.33.5"]; provider["evidenceSha256"] = hashlib.sha256(provider["provider"].encode()).hexdigest()
        support_path = temp / "support.yaml"; support_path.write_text(yaml.safe_dump(support, sort_keys=True), encoding="utf-8")
        freeze, source_files = temp / "freeze.json", temp / "source-files.sha256"
        run(sys.executable, "scripts/build_rc1_freeze.py", "--output", str(freeze), "--source-files-output", str(source_files))
        certificate = temp / "certificate.json"
        run(sys.executable, "scripts/certify_rc1_release.py", "--ledger", str(ledger_path), "--freeze", str(freeze), "--source-files-manifest", str(source_files), "--artifacts", str(artifact_path), "--supply-chain", str(supply_path), "--reproducible-build", str(reproducible_path), "--support-matrix", str(support_path), "--known-issues", "release/1.0.0-rc1/KNOWN_ISSUES.md", "--acceptance-plan", "release/1.0.0-rc1/ACCEPTANCE_TEST_PLAN.md", "--output", str(certificate), "--test-harness")
        result = json.loads(certificate.read_text(encoding="utf-8"))
        if result["publishable"] is not False or result.get("testHarness") is not True or not result["complete"]:
            raise RuntimeError("test harness incorrectly emitted a publishable RC1 certificate")
        unequal = json.loads(reproducible_path.read_text(encoding="utf-8")); unequal["builderBManifestSha256"] = "9" * 64; write_json(reproducible_path, unequal)
        run(sys.executable, "scripts/certify_rc1_release.py", "--ledger", str(ledger_path), "--freeze", str(freeze), "--source-files-manifest", str(source_files), "--artifacts", str(artifact_path), "--supply-chain", str(supply_path), "--reproducible-build", str(reproducible_path), "--support-matrix", str(support_path), "--known-issues", "release/1.0.0-rc1/KNOWN_ISSUES.md", "--acceptance-plan", "release/1.0.0-rc1/ACCEPTANCE_TEST_PLAN.md", "--output", str(temp / "must-not-exist.json"), "--test-harness", must_fail=True)

        archive_source = temp / "archive-source"; archive_source.mkdir(); (archive_source / "a.txt").write_text("alpha\n", encoding="utf-8"); (archive_source / "run.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8"); (archive_source / "run.sh").chmod(0o755)
        first, second = temp / "first.zip", temp / "second.zip"
        for output in (first, second): run(sys.executable, "scripts/build_deterministic_rc1_archive.py", "--source", str(archive_source), "--output", str(output), "--root-name", "test", "--source-date-epoch", "1700000000")
        if first.read_bytes() != second.read_bytes(): raise RuntimeError("normalized RC1 archive is not reproducible")


def main() -> int:
    require("Cargo.toml", 'version = "1.0.0-rc.1"')
    for chart in ("ngkg-crds", "ngkg-platform", "ngkg-workloads"):
        require(f"charts/{chart}/Chart.yaml", "version: 1.0.0-rc.1", "appVersion: 1.0.0-rc.1")
    require("api/openapi.yaml", "version: 1.0.0-rc.1", "/v1/datasets", "/v1/datasets/{datasetName}/restores", "/publish")
    require("api/online-openapi.yaml", "version: 1.0.0-rc.1", "/sparql", "/query", "/v1/query_logs")
    rust = require("crates/ngkg-release-qualification/src/rc1.rs", "RC1_VERSION", "EvidenceClass::LiveProductionQualification", "validate_prerequisites", "validate_freeze", "certify_rc1", "failure_count: 0", "publishable: true")
    spec = yaml.safe_load((ROOT / "release/1.0.0-rc1/release-spec.yaml").read_text(encoding="utf-8"))["spec"]
    if spec["featureDevelopmentAllowed"] is not False or spec["prerequisitePolicy"]["syntheticEvidenceAllowedForPublication"] is not False or spec["kubernetes"]["autoscalingCpuPercent"] != 80 or spec["kubernetes"]["autoscalingMemoryPercent"] != 80 or spec["productionRuntime"]["apacheJenaLinkedEmbeddedOrDeployed"] is not False:
        raise RuntimeError("RC1 feature freeze, evidence, autoscaling, or Rust runtime boundary is invalid")
    workspace = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))["workspace"]["members"]
    names = {tomllib.loads((ROOT / member / "Cargo.toml").read_text(encoding="utf-8"))["package"]["name"] for member in workspace}
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text(encoding="utf-8"))
    locked = {item["name"] for item in lock["package"] if item.get("version") == VERSION}
    if not names <= locked: raise RuntimeError(f"RC1 Cargo.lock omits workspace packages: {sorted(names - locked)}")
    for schema in sorted((ROOT / "contracts").glob("rc1-*.schema.json")):
        if json.loads(schema.read_text(encoding="utf-8")).get("additionalProperties") is not False:
            raise RuntimeError(f"{schema.name} is not closed")
    for script in ("freeze_rc1_versions.py", "build_rc1_freeze.py", "assess_rc1_readiness.py", "certify_rc1_release.py", "build_deterministic_rc1_archive.py"):
        py_compile.compile(str(ROOT / "scripts" / script), doraise=True)
    run(sys.executable, "scripts/freeze_rc1_versions.py", "--check")
    actual = ROOT / "release/1.0.0-rc1/rc1-readiness.json"
    run(sys.executable, "scripts/assess_rc1_readiness.py", "--output", str(actual))
    status = json.loads(actual.read_text(encoding="utf-8"))
    if status["publishable"] is not False or status["status"] != "blocked" or status["blockerCount"] == 0:
        raise RuntimeError("missing live evidence was not retained as an RC1 publication blocker")
    executable_barrier()
    run(sys.executable, "scripts/build_rc1_freeze.py", "--check")
    if "align_ontology" in rust or "raw_data_mapping" in rust: raise RuntimeError("ontology alignment or raw-data mapping entered RC1")
    print("phase 1.0.0-RC1 freeze, packaging, and fail-closed publication harness passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"RC1 source qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
