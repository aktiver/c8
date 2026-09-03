#!/usr/bin/env python3
"""Verify a keyless signature over the Phase 6 statement and seal its certificate."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from phase6_common import atomic_json, load_json, require, run, sha256_file


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--statement", required=True, type=Path)
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--certificate-identity-regexp", required=True)
    parser.add_argument("--certificate-oidc-issuer", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    statement = load_json(args.statement.resolve())
    require(statement.get("status") == "QUALIFIED" and statement.get("synthetic") is False and statement.get("complete") is True, "statement is not qualification complete")
    run([
        "cosign", "verify-blob",
        "--bundle", str(args.bundle.resolve()),
        "--certificate-identity-regexp", args.certificate_identity_regexp,
        "--certificate-oidc-issuer", args.certificate_oidc_issuer,
        str(args.statement.resolve()),
    ], timeout=600)
    certificate = {
        "formatVersion": 1,
        "kind": "EnterpriseStabilizationPhase6Certificate",
        "subjectSha256": statement["subjectSha256"],
        "statementSha256": sha256_file(args.statement.resolve()),
        "evidenceRootSha256": statement["evidenceRootSha256"],
        "providers": statement["providers"],
        "status": "QUALIFIED",
        "signed": True,
        "synthetic": False,
        "complete": True,
        "signature": {
            "scheme": "sigstore-keyless",
            "keylessIdentity": args.certificate_identity_regexp,
            "oidcIssuer": args.certificate_oidc_issuer,
            "bundleSha256": sha256_file(args.bundle.resolve()),
        },
    }
    atomic_json(args.output.resolve(), certificate)
    print(json.dumps({"status": "QUALIFIED", "certificateSha256": sha256_file(args.output.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        print(f"Phase 6 certificate sealing failed: {error}", file=sys.stderr)
        raise SystemExit(1)
