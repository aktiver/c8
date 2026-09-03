#!/usr/bin/env python3
"""Unit tests for Phase 3 evidence issuance; no test evidence is releasable."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "phase3/scripts/verify_and_issue.py"


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical(value) + b"\n")


class EvidenceVerifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="ngkg-phase3-verifier-")
        self.root = Path(self.temp.name)
        self.supply = self.root / "supply"
        self.clusters = self.root / "clusters"
        self.signatures = self.root / "signatures"
        self.signatures.mkdir(parents=True)
        toolchain = {"formatVersion": 1, "complete": True}
        write(self.supply / "toolchain-evidence.json", toolchain)
        catalog = json.loads((ROOT / "phase3/config/images.json").read_text())
        lock = {"formatVersion":1,"sourceRevision":"a"*40,"platforms":["linux/amd64","linux/arm64"],"toolchainEvidenceSha256":sha((self.supply / "toolchain-evidence.json").read_bytes()),"images":[]}
        for entry in catalog["images"]:
            name = entry["name"]
            directory = self.supply / "images" / name
            artifacts = {
                "spdxSha256": ("sbom.spdx.json", {"name":name,"type":"spdx"}),
                "cycloneDxSha256": ("sbom.cyclonedx.json", {"name":name,"type":"cyclonedx"}),
                "grypeSha256": ("grype.json", {"matches":[]}),
                "trivySha256": ("trivy.json", {"Results":[]}),
                "provenanceSha256": ("provenance.json", {"subject":name}),
            }
            hashes = {}
            for field, (filename, payload) in artifacts.items():
                write(directory / filename, payload)
                hashes[field] = sha((directory / filename).read_bytes())
            digest = "sha256:" + sha(name.encode())
            repository = f"registry.example/ngkg/{name}"
            evidence = {"formatVersion":1,"sourceRevision":"a"*40,"image":repository,"digest":digest,"platforms":["linux/amd64","linux/arm64"],**hashes,"signatureVerified":True,"highVulnerabilities":0,"criticalVulnerabilities":0,"complete":True}
            write(directory / "evidence.json", evidence)
            lock["images"].append({"name":name,"repository":repository,"digest":digest})
        lock["images"].sort(key=lambda item:item["name"])
        write(self.supply / "image-lock.json", lock)

        postgres_toolchain = self.root / "postgres-toolchain.json"
        write(postgres_toolchain, {"complete":True})
        self.postgres = self.root / "postgres-evidence.json"
        postgres = {"formatVersion":1,"toolchainEvidenceSha256":sha(postgres_toolchain.read_bytes()),"serverVersion":"17.1","primary":True,"streamingReplicas":1,"coreMigrationCount":9,"agentMigrationCount":7,"tenantRlsForced":True,"immutabilityVerified":True,"crossTenantDenied":True,"schemaBeforeSha256":"1"*64,"schemaAfterSha256":"2"*64,"complete":True}
        write(self.postgres, postgres)
        postgres_toolchain.rename(self.root / "toolchain-evidence.json")

        policy = json.loads((ROOT / "phase3/config/required-scenarios.json").read_text())
        lock_sha = sha((self.supply / "image-lock.json").read_bytes())
        for provider in policy["providers"]:
            report = {"formatVersion":1,"provider":provider,"clusterUid":f"cluster-{provider}","kubernetesVersion":"v1.33.1","readyNodes":3,"zones":3,"gpuNodes":1,"imageLockSha256":lock_sha,"deploymentEvidenceSha256":sha(provider.encode()),"scenarios":[{"id":scenario,"startedEpochMs":1,"endedEpochMs":2,"evidenceSha256":sha(f"{provider}:{scenario}".encode()),"complete":True} for scenario in policy["scenarios"]],"complete":True}
            write(self.clusters / f"{provider}.json", report)
        for name in ["postgres-evidence.json", *[f"{provider}.json" for provider in policy["providers"]]]:
            (self.signatures / f"{name}.sig").write_text("test-signature\n")

        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        cosign = bin_dir / "cosign"
        cosign.write_text("#!/bin/sh\nexit 0\n")
        cosign.chmod(0o755)
        self.env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
        self.output = self.root / "certificate.json"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def command(self) -> list[str]:
        return ["python3",str(SCRIPT),"--supply-chain",str(self.supply),"--postgres",str(self.postgres),"--clusters",str(self.clusters),"--signatures",str(self.signatures),"--cosign-key","test-public-key","--issued-epoch-ms","1","--output",str(self.output)]

    def test_complete_matrix_issues_certificate_and_tamper_fails(self) -> None:
        result = subprocess.run(self.command(), env=self.env, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        certificate = json.loads(self.output.read_text())
        self.assertTrue(certificate["complete"])
        self.assertEqual(certificate["providers"], ["aks","eks","gke","rke","rke2"])
        target = self.supply / "images/ngkg-api/sbom.spdx.json"
        target.write_bytes(target.read_bytes() + b"tamper")
        result = subprocess.run(self.command(), env=self.env, capture_output=True, check=False)
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
