#!/usr/bin/env python3
"""Verify that the pinned Phase 40.13.4 SPARQL baseline did not regress."""
from __future__ import annotations

import argparse
import json
import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--report",
        type=pathlib.Path,
        default=ROOT / "qualification/w3c-phase40.13.4-query-results.json",
    )
    arguments = parser.parse_args()
    report = json.loads(arguments.report.read_text(encoding="utf-8"))
    backlog = json.loads(
        (ROOT / "conformance/sparql11-known-gaps-phase40.13.4.json").read_text(
            encoding="utf-8"
        )
    )
    expected_names = {
        name for gap in backlog["gaps"] for name in gap["tests"]
    }
    observed_names = {
        test["name"] for test in report["tests"] if test["status"] == "fail"
    }
    summary = report["summary"]
    if summary.get("pass", 0) < 326 or summary.get("fail", 338) > 12:
        raise RuntimeError(f"SPARQL conformance regressed: {summary}")
    if summary.get("pass", 0) + summary.get("fail", 0) != 338:
        raise RuntimeError(f"unexpected executable query/results total: {summary}")
    if not observed_names.issubset(expected_names):
        raise RuntimeError(
            f"unregistered SPARQL failures: {sorted(observed_names - expected_names)}"
        )
    print(
        "Phase 40.13.4 SPARQL baseline verified: "
        f"{summary['pass']} passed, {summary['fail']} known failures"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, OSError, RuntimeError, TypeError, ValueError) as error:
        print(f"Phase 40.13.4 report verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
