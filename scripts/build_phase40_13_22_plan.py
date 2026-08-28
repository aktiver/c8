#!/usr/bin/env python3
"""Build a content-bound, topology-independent Phase 40.13.22 case plan."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from typing import Any


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def partition(case_id: str, input_sha256: str, count: int) -> int:
    digest = hashlib.sha256(f"{case_id}\0{input_sha256}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % count


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite-inventory", type=pathlib.Path, required=True)
    parser.add_argument("--definitions", type=pathlib.Path, required=True)
    parser.add_argument("--partition-count", type=int, required=True)
    parser.add_argument("--plan-output", type=pathlib.Path, required=True)
    parser.add_argument("--catalog-output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.partition_count <= 65536:
        raise ValueError("partition count must be in [1,65536]")
    inventory = json.loads(args.suite_inventory.read_text(encoding="utf-8"))
    definitions = json.loads(args.definitions.read_text(encoding="utf-8"))
    if set(definitions) != {"formatVersion", "cases"} or definitions["formatVersion"] != 1:
        raise ValueError("invalid definitions header")
    plan_cases: list[dict[str, Any]] = []
    catalog: dict[str, Any] = {}
    seen: set[str] = set()
    allowed_oracles = {"w3c-expected", "apache-jena", "hermit"}
    allowed_outcomes = {"success", "failure"}
    for definition in definitions["cases"]:
        required = {"caseId", "family", "oracle", "expectedOutcome", "descriptor"}
        if not required <= set(definition) or definition["caseId"] in seen:
            raise ValueError("case definition is incomplete or duplicated")
        if definition["oracle"] not in allowed_oracles or definition["expectedOutcome"] not in allowed_outcomes:
            raise ValueError("case oracle or outcome is outside the closed contract")
        case_id = definition["caseId"]
        seen.add(case_id)
        descriptor = definition["descriptor"]
        input_sha = sha(descriptor)
        catalog[case_id] = descriptor
        case = {
            "caseId": case_id, "family": definition["family"], "inputSha256": input_sha,
            "oracle": definition["oracle"], "expectedOutcome": definition["expectedOutcome"],
            "expectedResultSha256": definition.get("expectedResultSha256"),
            "expectedErrorClass": definition.get("expectedErrorClass"),
            "partition": partition(case_id, input_sha, args.partition_count),
        }
        if case["expectedOutcome"] == "success" and case["expectedErrorClass"] is not None:
            raise ValueError("success case has an error class")
        if case["expectedOutcome"] == "failure" and not case["expectedErrorClass"]:
            raise ValueError("failure case lacks a stable error class")
        plan_cases.append(case)
    plan_cases.sort(key=lambda item: item["caseId"])
    plan = {"formatVersion": 1, "suiteInventorySha256": sha(inventory), "partitionCount": args.partition_count, "cases": plan_cases}
    case_catalog = {"formatVersion": 1, "cases": dict(sorted(catalog.items()))}
    for path, value in ((args.plan_output, plan), (args.catalog_output, case_catalog)):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical(value) + b"\n")
    print(json.dumps({"cases": len(plan_cases), "partitions": args.partition_count, "planSha256": sha(plan)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
