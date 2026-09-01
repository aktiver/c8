#!/usr/bin/env python3
"""Merge an exact dense Phase 40.13.24 report barrier."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from typing import Any


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=pathlib.Path, required=True)
    parser.add_argument("--reports", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    plan_sha = digest(plan)
    report_files = sorted(args.reports.glob("*.json"))
    if len(report_files) != plan["partitionCount"]:
        raise ValueError("dense partition report count is incomplete")
    scenarios = {item["scenarioId"]: item for item in plan["scenarios"]}
    partitions, workers, observed, evidence = set(), set(), {}, hashlib.sha256()
    for path in report_files:
        report = json.loads(path.read_text(encoding="utf-8"))
        if report.get("formatVersion") != 1 or report.get("planSha256") != plan_sha or report.get("complete") is not True or report["partition"] in partitions or report["workerId"] in workers:
            raise ValueError("partition report identity, barrier, or worker uniqueness is invalid")
        partitions.add(report["partition"]); workers.add(report["workerId"])
        for item in report["observations"]:
            scenario = scenarios.get(item["scenarioId"])
            if scenario is None or item["scenarioId"] in observed or scenario["partition"] != report["partition"] or item["provider"] != scenario["provider"] or item["gate"] != scenario["gate"] or item["outputSha256"] != scenario["expectedOutputSha256"] or item["postRecoveryResultSha256"] != scenario["expectedOutputSha256"] or item["complete"] is not True or item["injectedFailures"] != item["recoveredFailures"]:
                raise ValueError("scenario evidence is missing, duplicated, partial, unequal, or unrecovered")
            observed[item["scenarioId"]] = item
    if partitions != set(range(plan["partitionCount"])) or set(observed) != set(scenarios):
        raise ValueError("dense report or scenario barrier is incomplete")
    for scenario_id in sorted(observed):
        evidence.update(scenario_id.encode()); evidence.update(b"\0")
        evidence.update(observed[scenario_id]["evidenceSha256"].encode()); evidence.update(b"\0")
    certificate = {
        "formatVersion": 1, "planSha256": plan_sha, "releaseSha256": plan["releaseSha256"],
        "performanceCertificateSha256": plan["performanceCertificateSha256"], "semanticEvidenceSha256": plan["semanticEvidenceSha256"],
        "qualifiedProviders": sorted({item["provider"] for item in observed.values()}),
        "qualifiedGates": sorted({item["gate"] for item in observed.values()}),
        "evidenceRootSha256": evidence.hexdigest(), "failureCount": 0, "complete": True,
    }
    if len(certificate["qualifiedProviders"]) != 5 or len(certificate["qualifiedGates"]) != 15:
        raise ValueError("certificate omits a provider or release gate")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical(certificate) + b"\n")
    print(json.dumps({"certificateSha256": digest(certificate), "scenarios": len(observed)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
