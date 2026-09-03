#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
from evidence_security import verify_reference  # noqa: E402
from phase6_common import redact_diagnostic  # noqa: E402

SHA = "a" * 64


class EvidenceSecurityTests(unittest.TestCase):
    def test_reference_is_hash_and_subject_bound(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = json.dumps({"subjectSha256": SHA, "scenarioId": "case"}).encode()
            (root / "case.json").write_bytes(payload)
            document = verify_reference(root, {
                "id": "case", "evidencePath": "case.json",
                "evidenceSha256": hashlib.sha256(payload).hexdigest(),
            }, SHA)
            self.assertEqual("case", document["scenarioId"])

    def test_traversal_and_symlink_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outside = root.parent / "outside-evidence.json"
            outside.write_text("{}")
            with self.assertRaises((RuntimeError, OSError)):
                verify_reference(root, {"id": "case", "evidencePath": "../outside-evidence.json", "evidenceSha256": hashlib.sha256(b"{}").hexdigest()}, SHA)
            (root / "escape.json").symlink_to(outside)
            with self.assertRaises((RuntimeError, OSError)):
                verify_reference(root, {"id": "case", "evidencePath": "escape.json", "evidenceSha256": hashlib.sha256(b"{}").hexdigest()}, SHA)
            outside.unlink()

    def test_cloud_and_bearer_secrets_are_redacted(self) -> None:
        raw = b"Authorization: Bearer secret-token https://host/path?sig=secret&x=1 password=hunter2"
        redacted, digest = redact_diagnostic(raw)
        self.assertNotIn("secret-token", redacted)
        self.assertNotIn("hunter2", redacted)
        self.assertNotIn("sig=secret", redacted)
        self.assertEqual(hashlib.sha256(raw).hexdigest(), digest)


if __name__ == "__main__":
    unittest.main()
