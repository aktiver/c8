#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

PHASE6 = Path(__file__).resolve().parents[1]
SCRIPT = PHASE6 / "scripts/verify_and_issue.py"
SHA = "a" * 64
OTHER_SHA = "b" * 64


def write(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


class IssuerTests(unittest.TestCase):
    def populate(self, root: Path) -> None:
        required = {
            "phase3-certificate.json": {"oci_supply_chain", "postgres_ha", "rke2", "eks", "aks", "gke"},
            "phase4-live-certificate.json": {"concurrent_status_writers", "operator_restart", "idempotent_object_retry", "parallel_spill", "datatype_differential", "blank_node_differential", "azure_artifacts", "gcs_artifacts"},
            "phase5-live-certificate.json": {"native_parquet_leaf_scan", "multinode_partition_barrier", "scalar_public_fallback_absent", "sparql_multiset_differential", "property_path_differential", "hpa_cpu_80", "hpa_memory_80"},
        }
        for name, scenarios in required.items():
            write(root / name, {"status": "QUALIFIED", "signed": True, "scenarios": [{"id": item, "status": "PASS", "evidenceSha256": SHA} for item in scenarios], "signature": {"keylessIdentity": "test", "bundleSha256": SHA}})
        write(root / "differential-evidence.json", {"kind": "Phase6DifferentialEvidence", "subjectSha256": SHA, "nativeCutoverMode": "required", "oracleIsolation": "QUALIFICATION_ONLY", "oracleProductionDependency": False, "mismatchCount": 0, "caseCount": 4, "measuredRepetitions": 3, "semanticRootSha256": SHA, "status": "PASS", "synthetic": False, "complete": True})
        for provider in ("rke2", "eks", "aks", "gke"):
            write(root / f"{provider}-evidence.json", {"kind": "Phase6ProviderEvidence", "provider": provider, "subjectSha256": SHA, "imageLockSha256": OTHER_SHA, "failureCount": 0, "status": "PASS", "synthetic": False, "complete": True, "inventory": {"readyNodes": 3, "failureDomains": 2}, "autoscaling": {"hpas": [{"cpuPercent": 80, "memoryPercent": 80}]}, "capacity": {"resources": {"physicalNodes": 2}, "trialCount": 1}, "chaos": {"failureCount": 0, "scenarios": [{"scenario": item} for item in ("worker_node_loss", "postgres_failover", "object_corruption")]}, "providerIntegrations": {"workloadIdentity": True, "longLivedCloudCredentials": False, "trigIngestion": True, "artifactRoundTrip": True}})
        write(root / "supply-chain-evidence.json", {"kind": "Phase6SupplyChainEvidence", "subjectSha256": SHA, "imageLockSha256": OTHER_SHA, "imageCount": 12, "signed": True, "spdxComplete": True, "cycloneDxComplete": True, "unapprovedCritical": 0, "unapprovedHigh": 0, "status": "PASS", "synthetic": False, "complete": True})
        write(root / "reproducible-build-evidence.json", {"kind": "Phase6ReproducibleBuildEvidence", "subjectSha256": SHA, "distinctBuilders": True, "networkControlled": True, "dependenciesLocked": True, "timestampsNormalized": True, "artifactRootSha256": SHA, "status": "PASS", "synthetic": False, "complete": True})
        write(root / "defect-ledger.json", {"subjectSha256": SHA, "complete": True, "defects": []})

    def invoke(self, root: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run([sys.executable, str(SCRIPT), "--evidence-root", str(root), "--subject-sha256", SHA, "--output", str(root / "statement.json")], text=True, capture_output=True, check=False)

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
            differential = json.loads((root / "differential-evidence.json").read_text())
            differential["mismatchCount"] = 1
            write(root / "differential-evidence.json", differential)
            result = self.invoke(root)
            self.assertNotEqual(0, result.returncode)
            self.assertIn("differential failed", result.stderr)


if __name__ == "__main__":
    unittest.main()
