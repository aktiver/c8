#!/usr/bin/env python3
"""Fail-closed all-partitions barrier for Phase 40.13.22."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
from collections import Counter
from typing import Any


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def load(path: pathlib.Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=pathlib.Path, required=True)
    parser.add_argument("--reports", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    plan = load(args.plan)
    plan_sha = sha(plan)
    paths = sorted(args.reports.glob("partition-*.json"))
    if len(paths) != plan["partitionCount"]:
        raise RuntimeError("one report per dense partition is required")
    reports = sorted((load(path) for path in paths), key=lambda value: value["partition"])
    expected = {case["caseId"]: case for case in plan["cases"]}
    observed: set[str] = set()
    workers: set[str] = set()
    for index, report in enumerate(reports):
        if report.get("formatVersion") != 1 or report.get("planSha256") != plan_sha or report.get("partition") != index or report.get("complete") is not True:
            raise RuntimeError("partition identity or completion barrier failed")
        if not report.get("workerId") or report["workerId"] in workers:
            raise RuntimeError("worker identities must be non-empty and unique")
        workers.add(report["workerId"])
        assigned = sorted(case_id for case_id, case in expected.items() if case["partition"] == index)
        rows = report.get("observations", [])
        if [row.get("caseId") for row in rows] != assigned:
            raise RuntimeError("partition observations differ from exact assignment")
        for row in rows:
            case = expected[row["caseId"]]
            if row["caseId"] in observed or row.get("family") != case["family"] or row.get("partition") != index or row.get("complete") is not True:
                raise RuntimeError("duplicate, misrouted or partial observation")
            observed.add(row["caseId"])
            if case["expectedOutcome"] == "success":
                if row.get("ngkgOutcome") != "success" or row.get("oracleOutcome") != "success" or row.get("ngkgResultSha256") != row.get("oracleResultSha256"):
                    raise RuntimeError("successful differential mismatch")
            elif row.get("ngkgOutcome") != "failure" or row.get("oracleOutcome") != "failure" or row.get("ngkgErrorClass") != case.get("expectedErrorClass") or row.get("oracleErrorClass") != case.get("expectedErrorClass"):
                raise RuntimeError("negative-case differential mismatch")
    if observed != set(expected):
        raise RuntimeError("all-cases completion barrier is incomplete")
    families = Counter(case["family"] for case in expected.values())
    certificate = {
        "formatVersion": 1, "planSha256": plan_sha, "reportSetSha256": sha(reports),
        "caseCount": len(expected), "casesByFamily": dict(sorted(families.items())),
        "oracleEngines": sorted({case["oracle"] for case in expected.values()}),
        "mismatchCount": 0, "missingCaseCount": 0, "complete": True,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_bytes(canonical(certificate) + b"\n")
    os.replace(temporary, args.output)
    print(json.dumps(certificate, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
