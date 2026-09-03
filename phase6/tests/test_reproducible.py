#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/verify_reproducible_build.py"
PAYLOAD = b"reproducible-artifact\n"
SHA = hashlib.sha256(PAYLOAD).hexdigest()
CLASSES = ["rust-binary", "oci-index", "helm-chart", "crd", "openapi", "json-schema", "migration", "source-archive"]


def manifest(builder: str) -> dict:
    return {
        "formatVersion": 1,
        "builderId": builder,
        "sourceSha256": SHA,
        "isolatedBuilder": True,
        "networkControlled": True,
        "dependenciesLocked": True,
        "timestampsNormalized": True,
        "artifacts": [{"path": f"release/{kind}", "class": kind, "sha256": SHA} for kind in CLASSES],
    }


class ReproducibleBuildTests(unittest.TestCase):
    def invoke(self, left: dict, right: dict) -> subprocess.CompletedProcess[str]:
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        for side, document in (("a", left), ("b", right)):
            artifact_root = root / f"{side}-artifacts"
            artifact_root.mkdir()
            for row in document["artifacts"]:
                target = artifact_root / row["path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(PAYLOAD)
            document["artifactRoot"] = artifact_root.name
        (root / "a.json").write_text(json.dumps(left))
        (root / "b.json").write_text(json.dumps(right))
        return subprocess.run([sys.executable, str(SCRIPT), "--builder-a", str(root / "a.json"), "--builder-b", str(root / "b.json"), "--output", str(root / "evidence.json")], text=True, capture_output=True, check=False)

    def tearDown(self) -> None:
        if hasattr(self, "directory"):
            self.directory.cleanup()

    def test_identical_artifacts_pass(self) -> None:
        self.assertEqual(0, self.invoke(manifest("builder-a"), manifest("builder-b")).returncode)

    def test_digest_difference_fails(self) -> None:
        right = manifest("builder-b")
        right["artifacts"][0]["sha256"] = "d" * 64
        result = self.invoke(manifest("builder-a"), right)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("artifact checksum mismatch", result.stderr)



if __name__ == "__main__":
    unittest.main()
