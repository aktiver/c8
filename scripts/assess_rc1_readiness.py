#!/usr/bin/env python3
"""Assess the checked tree without fabricating missing live RC1 evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[1]
VERSION = "1.0.0-rc.1"
REQUIRED = ["sparql11", "authorized-rdf-dataset", "owl2-dl", "distributed-reasoning", "distributed-query-runtime", "atomic-publication", "federation", "storage-recovery", "autoscaling", "enterprise-security", "standards", "performance-capacity", "kubernetes-release", "semantic-context-graph"]

SOURCE_STATUS = {
    "autoscaling": "verification/phase-40.13.20.json",
    "enterprise-security": "verification/phase-40.13.21.json",
    "standards": "verification/phase-40.13.22.json",
    "performance-capacity": "verification/phase-40.13.23.json",
    "kubernetes-release": "verification/phase-40.13.24.json",
    "semantic-context-graph": "verification/phase-40.13.24.json",
}


def sha_file(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_live_ledger(value: dict[str, Any]) -> list[str]:
    failures = []
    if set(value) != {"formatVersion", "releaseVersion", "releaseSha256", "prerequisites", "complete"} or value.get("formatVersion") != 1 or value.get("releaseVersion") != VERSION or value.get("complete") is not True:
        failures.append("ledger header is incomplete")
        return failures
    release_sha = value.get("releaseSha256")
    if not isinstance(release_sha, str) or len(release_sha) != 64:
        failures.append("ledger release identity is invalid")
    seen = set()
    for item in value.get("prerequisites", []):
        kind = item.get("kind")
        if kind in seen: failures.append(f"duplicate prerequisite: {kind}")
        seen.add(kind)
        if item.get("evidenceClass") != "live-production-qualification" or item.get("complete") is not True or item.get("failureCount") != 0 or item.get("synthetic") is not False or item.get("subjectSha256") != release_sha:
            failures.append(f"prerequisite is not complete live same-subject evidence: {kind}")
        if not isinstance(item.get("certificateSha256"), str) or len(item["certificateSha256"]) != 64:
            failures.append(f"prerequisite certificate identity is invalid: {kind}")
    for missing in sorted(set(REQUIRED) - seen): failures.append(f"missing prerequisite: {missing}")
    for extra in sorted(seen - set(REQUIRED)): failures.append(f"unknown prerequisite: {extra}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "release/1.0.0-rc1/rc1-readiness.json")
    parser.add_argument("--require-publishable", action="store_true")
    args = parser.parse_args()
    blockers = []
    evidence = []
    if args.ledger:
        ledger = json.loads(args.ledger.read_text(encoding="utf-8"))
        blockers.extend(validate_live_ledger(ledger))
        evidence.append({"path": str(args.ledger), "sha256": sha_file(args.ledger), "class": "supplied-ledger"})
    else:
        blockers.append("no live RC1 prerequisite ledger supplied")
        for kind, relative in SOURCE_STATUS.items():
            path = ROOT / relative
            value = json.loads(path.read_text(encoding="utf-8"))
            evidence.append({"prerequisite": kind, "path": relative, "sha256": sha_file(path), "complete": value.get("complete") is True})
            if value.get("complete") is not True:
                blockers.append(f"{kind} has source/synthetic evidence only: {relative}")
        blockers.extend(f"no live same-release certificate supplied: {kind}" for kind in REQUIRED if kind not in SOURCE_STATUS)
    result = {"formatVersion": 1, "releaseVersion": VERSION, "status": "publishable" if not blockers else "blocked",
              "publishable": not blockers, "blockerCount": len(blockers), "blockers": blockers,
              "evidenceInspected": evidence, "complete": True}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "blockerCount": len(blockers)}, sort_keys=True))
    if args.require_publishable and blockers:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
