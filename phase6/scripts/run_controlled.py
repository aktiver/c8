#!/usr/bin/env python3
"""Controlled, resumable entry point for the complete live Phase 6 workflow."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import shutil
import sys

from phase6_common import atomic_json, config_root_sha256, load_json, require, run, sha256_file, valid_sha256


def invoke(script: Path, *arguments: str, timeout: int = 172800) -> None:
    run([sys.executable, str(script), *arguments], timeout=timeout, maximum_stdout_bytes=16 * 1024 * 1024)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-root", required=True, type=Path)
    parser.add_argument("--evidence-root", required=True, type=Path)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    config_root = args.config_root.resolve()
    evidence_base = args.evidence_root.resolve()
    scripts = Path(__file__).resolve().parent
    repository = scripts.parents[1]
    release = load_json(config_root / "release.json")
    subject = release.get("subjectSha256")
    require(valid_sha256(subject), "invalid release subject")
    require(valid_sha256(release.get("configRootSha256")), "invalid canonical config-root digest")
    require(config_root_sha256(config_root) == release["configRootSha256"], "configuration root checksum mismatch")
    require(os.environ.get("NGKG_PHASE6_EXECUTE_LIVE") == "YES", "live execution is not approved")
    run_id = args.run_id or release.get("runId")
    require(isinstance(run_id, str) and re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", run_id), "a stable run ID is required")
    evidence_root = evidence_base / subject / run_id
    prerequisites = evidence_root / "prerequisites"
    prerequisites.mkdir(parents=True, exist_ok=True)
    staged = []
    for name in (
        "phase3-certificate.json", "phase3-certificate.sigstore.json",
        "phase4-live-certificate.json", "phase4-live-certificate.sigstore.json",
        "phase5-live-certificate.json", "phase5-live-certificate.sigstore.json",
    ):
        source = config_root / "prerequisites" / name
        require(source.is_file(), f"missing prerequisite input: {name}")
        target = prerequisites / name
        if target.exists():
            require(sha256_file(target) == sha256_file(source), f"staged prerequisite changed: {name}")
        else:
            shutil.copy2(source, target)
        staged.append({"path": f"prerequisites/{name}", "sha256": sha256_file(target)})
    defect_source = config_root / "defect-ledger.json"
    require(defect_source.is_file(), "defect ledger is missing")
    defect_target = evidence_root / "defects/defect-ledger.json"
    defect_target.parent.mkdir(parents=True, exist_ok=True)
    if defect_target.exists():
        require(sha256_file(defect_target) == sha256_file(defect_source), "staged defect ledger changed")
    else:
        shutil.copy2(defect_source, defect_target)
    run_plan = {
        "formatVersion": 1, "runId": run_id, "subjectSha256": subject,
        "configRootSha256": release.get("configRootSha256"),
        "prerequisites": staged,
        "providers": ["rke", "rke2", "eks", "aks", "gke"],
        "state": "STARTED",
    }
    run_plan_path = evidence_root / "run-plan.json"
    if run_plan_path.exists():
        require(load_json(run_plan_path) == run_plan, "run plan changed during resume")
    else:
        atomic_json(run_plan_path, run_plan)

    run([
        sys.executable, str(repository / "phase5/verify_live_prerequisites.py"),
        "--evidence-root", str(prerequisites),
        "--subject-sha256", subject,
        "--certificate-identity-regexp", release["certificateIdentityRegexp"],
        "--certificate-oidc-issuer", release["certificateOidcIssuer"],
    ], timeout=600)
    invoke(scripts / "run_differential.py", "--config", str(config_root / "differential.json"), "--output", str(evidence_root / "differential"))
    for provider in ("rke", "rke2", "eks", "aks", "gke"):
        invoke(scripts / "qualify_provider.py", "--config", str(config_root / "providers" / f"{provider}.json"), "--output", str(evidence_root))
    invoke(
        scripts / "verify_supply_chain.py",
        "--image-lock", str(config_root / "image-lock.json"),
        "--scan-report", str(config_root / "vulnerability-report.json"),
        "--output", str(evidence_root / "supply-chain/supply-chain-evidence.json"),
        "--subject-sha256", subject,
        "--certificate-identity-regexp", release["certificateIdentityRegexp"],
        "--certificate-oidc-issuer", release["certificateOidcIssuer"],
    )
    invoke(
        scripts / "verify_reproducible_build.py",
        "--builder-a", str(config_root / "builder-a-manifest.json"),
        "--builder-b", str(config_root / "builder-b-manifest.json"),
        "--output", str(evidence_root / "reproducibility/reproducible-build-evidence.json"),
    )
    statement = evidence_root / "issuance/phase6-statement.json"
    invoke(
        scripts / "verify_and_issue.py",
        "--evidence-root", str(evidence_root),
        "--subject-sha256", subject,
        "--certificate-identity-regexp", release["certificateIdentityRegexp"],
        "--certificate-oidc-issuer", release["certificateOidcIssuer"],
        "--output", str(statement),
    )
    bundle = evidence_root / "issuance/phase6-statement.sigstore.json"
    run(["cosign", "sign-blob", "--yes", "--bundle", str(bundle), str(statement)], timeout=600)
    invoke(
        scripts / "seal_certificate.py",
        "--statement", str(statement),
        "--bundle", str(bundle),
        "--certificate-identity-regexp", release["certificateIdentityRegexp"],
        "--certificate-oidc-issuer", release["certificateOidcIssuer"],
        "--output", str(evidence_root / "issuance/phase6-certificate.json"),
    )
    print(evidence_root / "issuance/phase6-certificate.json")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        print(f"Phase 6 controlled workflow failed: {error}", file=sys.stderr)
        raise SystemExit(1)
