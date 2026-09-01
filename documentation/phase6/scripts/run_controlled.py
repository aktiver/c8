#!/usr/bin/env python3
"""Controlled, resumable entry point for the complete live Phase 6 workflow."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from phase6_common import load_json, require, run, valid_sha256


def invoke(script: Path, *arguments: str, timeout: int = 172800) -> None:
    run([sys.executable, str(script), *arguments], timeout=timeout, maximum_stdout_bytes=16 * 1024 * 1024)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-root", required=True, type=Path)
    parser.add_argument("--evidence-root", required=True, type=Path)
    args = parser.parse_args()
    config_root = args.config_root.resolve()
    evidence_root = args.evidence_root.resolve()
    scripts = Path(__file__).resolve().parent
    repository = scripts.parents[1]
    release = load_json(config_root / "release.json")
    subject = release.get("subjectSha256")
    require(valid_sha256(subject), "invalid release subject")
    require(os.environ.get("NGKG_PHASE6_EXECUTE_LIVE") == "YES", "live execution is not approved")
    evidence_root.mkdir(parents=True, exist_ok=True)

    run([sys.executable, str(repository / "phase5/verify_live_prerequisites.py"), "--evidence-root", str(evidence_root)], timeout=600)
    invoke(scripts / "run_differential.py", "--config", str(config_root / "differential.json"), "--output", str(evidence_root))
    for provider in ("rke2", "eks", "aks", "gke"):
        invoke(scripts / "qualify_provider.py", "--config", str(config_root / "providers" / f"{provider}.json"), "--output", str(evidence_root))
    invoke(
        scripts / "verify_supply_chain.py",
        "--image-lock", str(config_root / "image-lock.json"),
        "--scan-report", str(config_root / "vulnerability-report.json"),
        "--output", str(evidence_root / "supply-chain-evidence.json"),
        "--subject-sha256", subject,
        "--certificate-identity-regexp", release["certificateIdentityRegexp"],
        "--certificate-oidc-issuer", release["certificateOidcIssuer"],
    )
    invoke(
        scripts / "verify_reproducible_build.py",
        "--builder-a", str(config_root / "builder-a-manifest.json"),
        "--builder-b", str(config_root / "builder-b-manifest.json"),
        "--output", str(evidence_root / "reproducible-build-evidence.json"),
    )
    statement = evidence_root / "phase6-statement.json"
    invoke(scripts / "verify_and_issue.py", "--evidence-root", str(evidence_root), "--subject-sha256", subject, "--output", str(statement))
    bundle = evidence_root / "phase6-statement.sigstore.json"
    run(["cosign", "sign-blob", "--yes", "--bundle", str(bundle), str(statement)], timeout=600)
    invoke(
        scripts / "seal_certificate.py",
        "--statement", str(statement),
        "--bundle", str(bundle),
        "--certificate-identity-regexp", release["certificateIdentityRegexp"],
        "--certificate-oidc-issuer", release["certificateOidcIssuer"],
        "--output", str(evidence_root / "phase6-certificate.json"),
    )
    print(evidence_root / "phase6-certificate.json")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        print(f"Phase 6 controlled workflow failed: {error}", file=sys.stderr)
        raise SystemExit(1)
