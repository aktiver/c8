#!/usr/bin/env python3
"""Verify the zero-failure Phase 40.13.5 scalar SPARQL oracle report."""
from __future__ import annotations

import argparse
import json
import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
SUITE_COMMIT = "8af71fed933539d09d5f4658fb1ea7ba4c8e30b9"
MANIFESTS = [
    "sparql/sparql11/manifest-sparql11-query.ttl",
    "sparql/sparql11/manifest-sparql11-results.ttl",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--report",
        type=pathlib.Path,
        default=ROOT / "qualification/w3c-phase40.13.5-query-results.json",
    )
    arguments = parser.parse_args()
    report = json.loads(arguments.report.read_text(encoding="utf-8"))
    if report.get("formatVersion") != 2:
        raise RuntimeError("unexpected report format")
    if report.get("suiteCommit") != SUITE_COMMIT:
        raise RuntimeError("report is not bound to the pinned W3C suite commit")
    if report.get("manifests") != MANIFESTS:
        raise RuntimeError("report does not cover the exact query/result manifests")
    if report.get("inventoryOnly") is not False:
        raise RuntimeError("inventory-only output is not execution evidence")
    if report.get("inventory", {}).get("total") != 338:
        raise RuntimeError("unexpected executable query/result inventory")
    if report.get("summary") != {"pass": 338}:
        raise RuntimeError(f"scalar SPARQL oracle is not green: {report.get('summary')}")
    tests = report.get("tests", [])
    if len(tests) != 338 or any(test.get("status") != "pass" for test in tests):
        raise RuntimeError("every one of the 338 case records must pass")
    executor = report.get("executor", {})
    if int(executor.get("jobs", 0)) < 1:
        raise RuntimeError("executor did not record bounded parallel work")
    if executor.get("nestedNativeThreadsPerCase") != 1:
        raise RuntimeError("nested native thread ceiling is not one per W3C child")
    gaps = json.loads(
        (ROOT / "conformance/sparql11-known-gaps-phase40.13.5.json").read_text(
            encoding="utf-8"
        )
    )
    if gaps.get("baseline") != {"total": 338, "pass": 338, "fail": 0}:
        raise RuntimeError("the Phase 40.13.5 gap ledger disagrees with the gate")
    if gaps.get("gaps") != []:
        raise RuntimeError("executable query/result gap ledger is not empty")
    print("Phase 40.13.5 scalar SPARQL oracle verified: 338 passed, 0 failed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, OSError, RuntimeError, TypeError, ValueError) as error:
        print(f"Phase 40.13.5 report verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
