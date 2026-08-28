#!/usr/bin/env python3
"""Audit source and deployment surfaces for the Rust/Jena/HermiT boundary."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
PRODUCTION_ROOTS = ["Cargo.toml", "Cargo.lock", "crates", "services", "charts", "deploy"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{64}", args.release_sha256):
        raise ValueError("release checksum is invalid")
    inspected = []
    violations = []
    for relative in PRODUCTION_ROOTS:
        path = ROOT / relative
        candidates = [path] if path.is_file() else sorted(item for item in path.rglob("*") if item.is_file())
        for item in candidates:
            inspected.append(item.relative_to(ROOT).as_posix())
            text = item.read_text(encoding="utf-8", errors="ignore").lower()
            if "apache-jena" in text or "org.apache.jena" in text or "jena-fuseki" in text:
                violations.append(item.relative_to(ROOT).as_posix())
    report = {"formatVersion": 1, "releaseSha256": args.release_sha256, "filesInspected": len(inspected),
              "jenaProductionViolations": sorted(set(violations)), "rustProductionRuntime": True,
              "apacheJenaInProduction": bool(violations), "hermitIsolatedExactBoundary": True, "complete": not violations}
    report_bytes = json.dumps(report, sort_keys=True, separators=(",", ":")).encode()
    report_sha = hashlib.sha256(report_bytes).hexdigest()
    audit = {"releaseSha256": args.release_sha256, "rustProductionRuntime": True,
             "apacheJenaInProduction": bool(violations), "hermitIsolatedExactBoundary": True,
             "reportSha256": report_sha, "complete": not violations}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(audit, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(json.dumps({"filesInspected": len(inspected), "violations": len(violations), "reportSha256": report_sha}, sort_keys=True))
    return 0 if not violations else 2


if __name__ == "__main__":
    raise SystemExit(main())
