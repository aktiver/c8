#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

PHASE6 = Path(__file__).resolve().parents[1]
SCRIPT = PHASE6 / "scripts/verify_and_issue.py"
SHA = "a" * 64
OTHER_SHA = "b" * 64


def write(path: Path, value: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


class IssuerTests(unittest.TestCase):
    def populate(self, root: Path) -> None:
        required = {
            "phase3-certificate.json": {"oci_supply_chain", "postgres_ha", "rke", "rke2", "eks", "aks", "gke"},
            "phase4-live-certificate.json": {"concurrent_status_writers", "operator_restart", "idempotent_object_retry", "parallel_spill", "datatype_differential", "blank_node_differential", "azure_artifacts", "gcs_artifacts"},
            "phase5-live-certificate.json": {"native_parquet_leaf_scan", "multinode_partition_barrier", "scalar_public_fallback_absent", "sparql_multiset_differential", "property_path_differential", "hpa_cpu_80", "hpa_memory_80"},
        }
        prereq = root / "prerequisites"
        for name, scenarios in required.items():
            rows = []
            for item in scenarios:
                relative = f"{name}.evidence/{item}.json"
                digest = write(prereq / relative, {"subjectSha256": SHA, "scenarioId": item})
                rows.append({"id": item, "status": "PASS", "evidencePath": relative, "evidenceSha256": digest})
            bundle_sha = write(prereq / name.replace(".json", ".sigstore.json"), {})
            write(prereq / name, {"subjectSha256": SHA, "status": "QUALIFIED", "signed": True, "scenarios": rows, "signature": {"keylessIdentity": "test", "bundleSha256": bundle_sha}})
        write(root / "differential/differential-evidence.json", {"kind": "Phase6DifferentialEvidence", "subjectSha256": SHA, "nativeCutoverMode": "required", "oracleIsolation": "QUALIFICATION_ONLY", "oracleProductionDependency": False, "mismatchCount": 0, "caseCount": 4, "measuredRepetitions": 3, "semanticRootSha256": SHA, "scenarios": [], "status": "PASS", "synthetic": False, "complete": True})
        for provider in ("rke", "rke2", "eks", "aks", "gke"):
            write(root / f"providers/{provider}/provider-evidence.json", {"kind": "Phase6ProviderEvidence", "provider": provider, "subjectSha256": SHA, "imageLockSha256": OTHER_SHA, "failureCount": 0, "status": "PASS", "synthetic": False, "complete": True, "inventory": {"readyNodes": 3, "failureDomains": 2}, "autoscaling": {"hpas": [{"cpuPercent": 80, "memoryPercent": 80}]}, "capacity": {"resources": {"physicalNodes": 2}, "trialCount": 1}, "chaos": {"failureCount": 0, "scenarios": [{"scenario": item} for item in ("worker_node_loss", "postgres_failover", "object_corruption")]}, "providerIntegrations": {"workloadIdentity": True, "longLivedCloudCredentials": False, "trigIngestion": True, "artifactRoundTrip": True, "gpuWorkloadObserved": True, "gpuScaleFromZero": True, "gpuTimeNs": 1, "postCutoverTenantIsolation": True}, "scenarios": []})
        write(root / "supply-chain/supply-chain-evidence.json", {"kind": "Phase6SupplyChainEvidence", "subjectSha256": SHA, "imageLockSha256": OTHER_SHA, "imageCount": 13, "signed": True, "spdxComplete": True, "cycloneDxComplete": True, "unapprovedCritical": 0, "unapprovedHigh": 0, "status": "PASS", "synthetic": False, "complete": True})
        write(root / "reproducibility/reproducible-build-evidence.json", {"kind": "Phase6ReproducibleBuildEvidence", "subjectSha256": SHA, "distinctBuilders": True, "networkControlled": True, "dependenciesLocked": True, "timestampsNormalized": True, "artifactRootSha256": SHA, "status": "PASS", "synthetic": False, "complete": True})
        write(root / "defects/defect-ledger.json", {"subjectSha256": SHA, "complete": True, "defects": []})
        cosign = root / "bin/cosign"
        cosign.parent.mkdir()
        cosign.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        cosign.chmod(0o700)

    def invoke(self, root: Path) -> subprocess.CompletedProcess[str]:
        environment = dict(os.environ)
        environment["PATH"] = str(root / "bin") + os.pathsep + environment["PATH"]
        return subprocess.run([sys.executable, str(SCRIPT), "--evidence-root", str(root), "--subject-sha256", SHA, "--certificate-identity-regexp", "test", "--certificate-oidc-issuer", "https://issuer.example/", "--output", str(root / "statement.json")], text=True, capture_output=True, check=False, env=environment)

    def test_complete_evidence_issues_statement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); self.populate(root)
            result = self.invoke(root)
            self.assertEqual(0, result.returncode, result.stderr)
            statement = json.loads((root / "statement.json").read_text())
            self.assertEqual("QUALIFIED", statement["status"])
            self.assertFalse(statement["oracleProductionDependency"])

    def test_semantic_mismatch_blocks_issuance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); self.populate(root)
            differential = json.loads((root / "differential/differential-evidence.json").read_text())
            differential["mismatchCount"] = 1
            write(root / "differential/differential-evidence.json", differential)
            result = self.invoke(root)
            self.assertNotEqual(0, result.returncode)
            self.assertIn("differential failed", result.stderr)


if __name__ == "__main__":
    unittest.main()
