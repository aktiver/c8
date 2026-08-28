#!/usr/bin/env python3
"""Executable static and fail-closed qualification for NGKG 1.0.0 GA."""

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
VERSION = "1.0.0"
KINDS = ["rc1-acceptance", "sparql-correctness", "cross-domain-owl2-dl", "reasoning-correctness", "multinode-hpc", "autoscaling", "kubernetes-matrix", "cloud-trig-ingestion", "ha-chaos", "backup-restore", "upgrade-rollback", "enterprise-security", "query-logs", "performance-capacity", "operational-readiness", "production-runtime-audit", "security-license", "reproducible-build", "contract-freeze", "artifact-publication"]
CLASSES = ["source-archive", "image-index", "helm-charts", "kubernetes-bundle", "crds", "migrations", "utilities", "api-schemas", "sbom-spdx", "sbom-cyclone-dx", "provenance", "qualification-evidence", "documentation", "checksums", "signatures"]


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
    with tempfile.TemporaryDirectory(prefix="ga-harness-") as raw:
        temp = pathlib.Path(raw)
        release_sha = "1" * 64
        qualifications = {"formatVersion": 1, "releaseVersion": VERSION, "releaseSha256": release_sha,
            "qualifications": [{"kind": kind, "certificateSha256": hashlib.sha256(kind.encode()).hexdigest(), "subjectSha256": release_sha, "live": True, "synthetic": False, "failureCount": 0, "complete": True} for kind in KINDS], "complete": True}
        qualification_path = temp / "qualifications.json"; write_json(qualification_path, qualifications)
        readiness = temp / "readiness.json"
        run(sys.executable, "scripts/assess_ga_readiness.py", "--ledger", str(qualification_path), "--output", str(readiness), "--require-publishable")
        synthetic = json.loads(qualification_path.read_text(encoding="utf-8")); synthetic["qualifications"][0]["live"] = False; synthetic["qualifications"][0]["synthetic"] = True
        synthetic_path = temp / "synthetic.json"; write_json(synthetic_path, synthetic)
        run(sys.executable, "scripts/assess_ga_readiness.py", "--ledger", str(synthetic_path), "--output", str(temp / "synthetic-readiness.json"), "--require-publishable", must_fail=True)

        defects = {"formatVersion": 1, "releaseSha256": release_sha, "defects": [], "complete": True}
        runtime = {"releaseSha256": release_sha, "rustProductionRuntime": True, "apacheJenaInProduction": False, "hermitIsolatedExactBoundary": True, "reportSha256": "2" * 64, "complete": True}
        defect_path, runtime_path = temp / "defects.json", temp / "runtime.json"; write_json(defect_path, defects); write_json(runtime_path, runtime)
        artifacts = []
        for artifact_class in CLASSES:
            if artifact_class == "image-index":
                path, artifact_sha = f"oci://registry.invalid/ngkg@sha256:{'a' * 64}", "a" * 64
            else:
                file = temp / f"artifact-{artifact_class}.bin"; file.write_bytes(f"harness:{artifact_class}\n".encode())
                path, artifact_sha = str(file.resolve()), hashlib.sha256(file.read_bytes()).hexdigest()
            artifacts.append({"class": artifact_class, "path": path, "sha256": artifact_sha, "signatureSha256": hashlib.sha256(f"signature:{artifact_class}".encode()).hexdigest(), "mediaType": "application/octet-stream", "immutable": True})
        artifact_manifest = {"formatVersion": 1, "releaseVersion": VERSION, "releaseSha256": release_sha, "artifacts": artifacts, "complete": True}
        artifact_path = temp / "artifacts.json"; write_json(artifact_path, artifact_manifest)
        supply = {"signatureReportSha256": "3" * 64, "provenanceReportSha256": "4" * 64, "secretScanSha256": "5" * 64, "licenseReportSha256": "6" * 64, "vulnerabilityReportSha256": "7" * 64, "unapprovedCriticalCves": 0, "unapprovedHighCves": 0, "embeddedCredentials": 0, "licensePolicyComplete": True, "runtimeHardeningComplete": True, "workloadIdentityComplete": True, "networkPolicyComplete": True, "complete": True}
        reproducible = {"builderAManifestSha256": "8" * 64, "builderBManifestSha256": "8" * 64, "sourceSha256": "9" * 64, "networkControlled": True, "dependenciesLocked": True, "timestampsNormalized": True, "complete": True}
        supply_path, reproducible_path = temp / "supply.json", temp / "reproducible.json"; write_json(supply_path, supply); write_json(reproducible_path, reproducible)
        support = yaml.safe_load((ROOT / "release/1.0.0/support-matrix.yaml").read_text(encoding="utf-8"))
        for provider in support["spec"]["providers"]:
            provider["qualified"] = True; provider["testedKubernetesVersions"] = ["1.33.5"]; provider["evidenceSha256"] = hashlib.sha256(provider["provider"].encode()).hexdigest()
        support_path = temp / "support.yaml"; support_path.write_text(yaml.safe_dump(support, sort_keys=True), encoding="utf-8")
        freeze, source_files = temp / "freeze.json", temp / "source-files.sha256"
        run(sys.executable, "scripts/build_ga_freeze.py", "--output", str(freeze), "--source-files-output", str(source_files))
        certificate = temp / "certificate.json"
        command = [sys.executable, "scripts/certify_ga_release.py", "--qualifications", str(qualification_path), "--defects", str(defect_path), "--runtime-audit", str(runtime_path), "--freeze", str(freeze), "--source-files-manifest", str(source_files), "--artifacts", str(artifact_path), "--supply-chain", str(supply_path), "--reproducible-build", str(reproducible_path), "--support-matrix", str(support_path), "--known-issues", "release/1.0.0/KNOWN_ISSUES.md", "--acceptance-plan", "release/1.0.0/ACCEPTANCE_TEST_PLAN.md", "--output", str(certificate), "--test-harness"]
        run(*command)
        result = json.loads(certificate.read_text(encoding="utf-8"))
        if result["publishable"] is not False or result.get("testHarness") is not True or result["decision"] != "test-only":
            raise RuntimeError("test harness incorrectly emitted a publishable GA certificate")

        bad_defects = {"formatVersion": 1, "releaseSha256": release_sha, "defects": [{"defectId": "GA-1", "severity": "critical", "unresolved": True, "releaseBlocking": False, "regressionPassed": False, "compatibilityReviewed": True, "evidenceSha256": "a" * 64}], "complete": True}
        write_json(defect_path, bad_defects); run(*command, must_fail=True); write_json(defect_path, defects)
        runtime["apacheJenaInProduction"] = True; write_json(runtime_path, runtime); run(*command, must_fail=True); write_json(runtime_path, {**runtime, "apacheJenaInProduction": False})
        artifact_manifest["artifacts"][0]["immutable"] = False; write_json(artifact_path, artifact_manifest); run(*command, must_fail=True)

        archive_source = temp / "archive-source"; archive_source.mkdir(); (archive_source / "a.txt").write_text("alpha\n", encoding="utf-8")
        first, second = temp / "first.zip", temp / "second.zip"
        for output in (first, second):
            run(sys.executable, "scripts/build_deterministic_ga_archive.py", "--source", str(archive_source), "--output", str(output), "--root-name", "test", "--source-date-epoch", "1700000000")
        if first.read_bytes() != second.read_bytes():
            raise RuntimeError("normalized GA archive is not reproducible")


def main() -> int:
    require("Cargo.toml", 'version = "1.0.0"')
    for chart in ("ngkg-crds", "ngkg-platform", "ngkg-workloads"):
        require(f"charts/{chart}/Chart.yaml", "version: 1.0.0", "appVersion: 1.0.0")
    require("api/openapi.yaml", "version: 1.0.0", "/v1/datasets", "/publish")
    require("api/online-openapi.yaml", "version: 1.0.0", "/sparql", "/query", "/v1/query_logs")
    rust = require("crates/ngkg-release-qualification/src/ga.rs", "GA_VERSION", "validate_ga_qualifications", "validate_defects", "validate_runtime", "certify_ga", "apache_jena_in_production")
    spec = yaml.safe_load((ROOT / "release/1.0.0/release-spec.yaml").read_text(encoding="utf-8"))["spec"]
    if spec["featureDevelopmentAllowed"] is not False or spec["kubernetes"]["autoscalingCpuPercent"] != 80 or spec["kubernetes"]["autoscalingMemoryPercent"] != 80 or spec["kubernetes"]["scaleOnEitherResource"] is not True or spec["productionRuntime"]["apacheJenaLinkedEmbeddedOrDeployed"] is not False:
        raise RuntimeError("GA feature freeze, scaling, or production runtime boundary is invalid")
    workspace = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))["workspace"]["members"]
    names = {tomllib.loads((ROOT / member / "Cargo.toml").read_text(encoding="utf-8"))["package"]["name"] for member in workspace}
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text(encoding="utf-8"))
    locked = {item["name"] for item in lock["package"] if item.get("version") == VERSION}
    if not names <= locked:
        raise RuntimeError(f"GA Cargo.lock omits workspace packages: {sorted(names - locked)}")
    for schema in sorted((ROOT / "contracts").glob("ga-*.schema.json")):
        if json.loads(schema.read_text(encoding="utf-8")).get("additionalProperties") is not False:
            raise RuntimeError(f"{schema.name} is not closed")
    for script in ("freeze_ga_versions.py", "build_ga_freeze.py", "assess_ga_readiness.py", "audit_ga_production_runtime.py", "certify_ga_release.py", "build_deterministic_ga_archive.py"):
        py_compile.compile(str(ROOT / "scripts" / script), doraise=True)
    run(sys.executable, "scripts/freeze_ga_versions.py", "--check")
    run(sys.executable, "scripts/assess_ga_readiness.py")
    status = json.loads((ROOT / "release/1.0.0/ga-readiness.json").read_text(encoding="utf-8"))
    if status["publishable"] is not False or status["status"] != "blocked" or status["blockerCount"] == 0:
        raise RuntimeError("missing live evidence was not retained as a GA publication blocker")
    executable_barrier()
    run(sys.executable, "scripts/build_ga_freeze.py", "--check")
    if "align_ontology" in rust or "raw_data_mapping" in rust:
        raise RuntimeError("ontology alignment or raw-data mapping entered GA")
    print("phase 1.0.0 GA freeze, defect, runtime, artifact, and fail-closed go/no-go harness passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"GA source qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
