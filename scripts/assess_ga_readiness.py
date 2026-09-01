#!/usr/bin/env python3
"""Assess GA readiness without converting source/static evidence into a pass."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[1]
VERSION = "1.0.0"
REQUIRED = {"rc1-acceptance", "sparql-correctness", "cross-domain-owl2-dl", "reasoning-correctness", "multinode-hpc", "autoscaling", "kubernetes-matrix", "cloud-trig-ingestion", "ha-chaos", "backup-restore", "upgrade-rollback", "enterprise-security", "query-logs", "performance-capacity", "operational-readiness", "production-runtime-audit", "security-license", "reproducible-build", "contract-freeze", "artifact-publication"}


def sha_file(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate(value: dict[str, Any]) -> list[str]:
    blockers = []
    release_sha = value.get("releaseSha256")
    if value.get("formatVersion") != 1 or value.get("releaseVersion") != VERSION or value.get("complete") is not True or not isinstance(release_sha, str) or len(release_sha) != 64:
        return ["GA qualification ledger header is invalid"]
    seen = set()
    for item in value.get("qualifications", []):
        kind = item.get("kind")
        if kind in seen:
            blockers.append(f"duplicate qualification: {kind}")
        seen.add(kind)
        if item.get("live") is not True or item.get("synthetic") is not False or item.get("complete") is not True or item.get("failureCount") != 0 or item.get("subjectSha256") != release_sha or not isinstance(item.get("certificateSha256"), str) or len(item["certificateSha256"]) != 64:
            blockers.append(f"qualification is not complete live same-subject evidence: {kind}")
    blockers += [f"missing qualification: {kind}" for kind in sorted(REQUIRED - seen)]
    blockers += [f"unknown qualification: {kind}" for kind in sorted(seen - REQUIRED)]
    return blockers


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "release/1.0.0/ga-readiness.json")
    parser.add_argument("--require-publishable", action="store_true")
    args = parser.parse_args()
    evidence = []
    if args.ledger:
        value = json.loads(args.ledger.read_text(encoding="utf-8"))
        blockers = validate(value)
        evidence.append({"path": str(args.ledger), "sha256": sha_file(args.ledger), "class": "supplied-live-ledger"})
    else:
        blockers = ["no live 1.0.0 GA qualification ledger supplied"]
        readiness = ROOT / "release/1.0.0-rc1/rc1-readiness.json"
        evidence.append({"path": str(readiness.relative_to(ROOT)), "sha256": sha_file(readiness), "class": "inherited-rc1-source-status"})
        blockers += [f"no live same-release certificate supplied: {kind}" for kind in sorted(REQUIRED)]
    result = {"formatVersion": 1, "releaseVersion": VERSION, "status": "publishable" if not blockers else "blocked", "publishable": not blockers,
              "blockerCount": len(blockers), "blockers": blockers, "evidenceInspected": evidence, "complete": True}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "blockerCount": len(blockers)}, sort_keys=True))
    return 2 if args.require_publishable and blockers else 0


if __name__ == "__main__":
    raise SystemExit(main())
