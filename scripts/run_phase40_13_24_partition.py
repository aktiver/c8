#!/usr/bin/env python3
"""Execute one Phase 40.13.24 partition through a bounded external driver."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import shlex
import subprocess
import tempfile
from typing import Any


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=pathlib.Path, required=True)
    parser.add_argument("--catalog", type=pathlib.Path, required=True)
    parser.add_argument("--partition", type=int, required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--driver", required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=604800)
    parser.add_argument("--allow-disruptive", action="store_true")
    parser.add_argument("--approval-evidence", type=pathlib.Path)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    if not 0 <= args.partition < plan["partitionCount"] or not args.worker_id or args.timeout_seconds < 1:
        raise ValueError("partition, worker, or timeout is invalid")
    approval_sha = sha_bytes(args.approval_evidence.read_bytes()) if args.approval_evidence else None
    observations = []
    for scenario in (item for item in plan["scenarios"] if item["partition"] == args.partition):
        if scenario["disruptive"] and (not args.allow_disruptive or approval_sha != scenario["approvalEvidenceSha256"]):
            raise ValueError("disruptive scenario requires explicitly enabled, content-bound approval evidence")
        request = {
            "formatVersion": 1, "planSha256": sha_bytes(canonical(plan)), "scenario": scenario,
            "descriptor": catalog["scenarios"][scenario["scenarioId"]],
            "isolatedQualificationClusterRequired": scenario["disruptive"],
        }
        with tempfile.NamedTemporaryFile(prefix="ngkg-release-request-", suffix=".json") as stream:
            stream.write(canonical(request) + b"\n")
            stream.flush()
            command = [*shlex.split(args.driver), stream.name]
            result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=args.timeout_seconds)
        if result.returncode != 0:
            raise RuntimeError(f"release driver failed for {scenario['scenarioId']}: {result.stderr}")
        observation = json.loads(result.stdout)
        required = {"scenarioId", "provider", "gate", "outputSha256", "evidenceSha256", "activatedNodes", "activatedCpuMillis", "activatedMemoryBytes", "durationSeconds", "injectedFailures", "recoveredFailures", "postRecoveryResultSha256", "complete"}
        if set(observation) != required or observation["scenarioId"] != scenario["scenarioId"] or observation["provider"] != scenario["provider"] or observation["gate"] != scenario["gate"] or observation["outputSha256"] != scenario["expectedOutputSha256"] or observation["postRecoveryResultSha256"] != scenario["expectedOutputSha256"] or observation["complete"] is not True or observation["activatedNodes"] < scenario["minimumNodes"] or observation["activatedCpuMillis"] < scenario["minimumCpuMillis"] or observation["activatedMemoryBytes"] < scenario["minimumMemoryBytes"] or observation["durationSeconds"] < scenario["minimumDurationSeconds"] or observation["injectedFailures"] != observation["recoveredFailures"]:
            raise ValueError(f"release observation failed closed validation: {scenario['scenarioId']}")
        observations.append(observation)
    observations.sort(key=lambda item: item["scenarioId"])
    report = {"formatVersion": 1, "planSha256": sha_bytes(canonical(plan)), "partition": args.partition, "workerId": args.worker_id, "observations": observations, "complete": True}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical(report) + b"\n")
    print(json.dumps({"partition": args.partition, "observations": len(observations)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
