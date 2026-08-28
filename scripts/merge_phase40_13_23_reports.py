#!/usr/bin/env python3
"""Merge every Phase 40.13.23 trial or fail without publishing a claim."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
from collections import defaultdict
from typing import Any

import yaml


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def load(path: pathlib.Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream) if path.suffix in {".yaml", ".yml"} else json.load(stream)


def nearest(values: list[int], percentile: int) -> int:
    if not values:
        raise ValueError("percentile input is empty")
    values = sorted(values)
    rank = math.ceil(len(values) * percentile / 100)
    return values[rank - 1]


def dense(rows: list[dict[str, Any]], phase: str, count: int) -> list[dict[str, Any]]:
    selected = [row for row in rows if row["trialPhase"] == phase]
    if len(selected) != count or sorted(row["trial"] for row in selected) != list(range(count)):
        raise RuntimeError("trials are missing, duplicated, or excluded")
    return selected


def stats(rows: list[dict[str, Any]], scenario: dict[str, Any], engine: str) -> dict[str, Any]:
    engine_rows = [row for row in rows if row["engine"] == engine]
    dense(engine_rows, "warmup", scenario["warmupTrials"])
    measured = dense(engine_rows, "measured", scenario["measuredTrials"])
    duration = [row["durationNanoseconds"] for row in measured]
    throughput = [row["workItems"] * 1_000_000_000 // row["durationNanoseconds"] for row in measured]
    cost = [row["costMicroUsd"] * 1_000_000 // row["workItems"] for row in measured]
    return {
        "engine": engine,
        "measuredTrials": len(measured),
        "p50Nanoseconds": nearest(duration, 50),
        "p95Nanoseconds": nearest(duration, 95),
        "p99Nanoseconds": nearest(duration, 99),
        "medianThroughputPerSecond": nearest(throughput, 50),
        "medianCostMicroUsdPerMillion": nearest(cost, 50),
        "maximumNodesActivated": max(row["nodesActivated"] for row in measured),
        "maximumPeakRssBytes": max(row["peakRssBytes"] for row in measured),
    }


def integer_nth_root(value: int, degree: int) -> int:
    if value < 0 or degree < 1:
        raise ValueError("invalid integer root")
    low, high = 0, 1
    while high**degree <= value:
        high *= 2
    while low + 1 < high:
        middle = (low + high) // 2
        if middle**degree <= value:
            low = middle
        else:
            high = middle
    return low


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=pathlib.Path, required=True)
    parser.add_argument("--inventory", type=pathlib.Path, required=True)
    parser.add_argument("--reports", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    plan, inventory = load(args.plan), load(args.inventory)
    if digest(inventory) != plan["benchmarkInventorySha256"]:
        raise RuntimeError("inventory differs from the immutable plan")
    paths = sorted(args.reports.glob("partition-*.json"))
    if len(paths) != plan["partitionCount"]:
        raise RuntimeError("one report per dense partition is required")
    reports = sorted((load(path) for path in paths), key=lambda report: report["partition"])
    plan_sha, workers = digest(plan), set()
    scenario_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    scenarios = {item["scenarioId"]: item for item in plan["scenarios"]}
    for partition, report in enumerate(reports):
        if set(report) != {"formatVersion", "planSha256", "partition", "workerId", "observations", "complete"} or report["formatVersion"] != 1 or report["planSha256"] != plan_sha or report["partition"] != partition or report["complete"] is not True:
            raise RuntimeError("partition identity or completion failed")
        if not report["workerId"] or report["workerId"] in workers:
            raise RuntimeError("worker identities must be unique")
        workers.add(report["workerId"])
        for row in report["observations"]:
            scenario = scenarios.get(row["scenarioId"])
            if scenario is None or scenario["partition"] != partition:
                raise RuntimeError("observation is outside its stable partition")
            if row["complete"] is not True or row["resultSha256"] != scenario["expectedResultSha256"] or row["autoscalingEvidenceSha256"] != plan["autoscalingEvidenceSha256"]:
                raise RuntimeError("partial, unequal, or scaling-unbound observation")
            if row["nodesActivated"] > scenario["requestedNodes"] or row["cpuMillisActivated"] > scenario["requestedCpuMillis"] or row["ramBytesActivated"] > scenario["requestedMemoryBytes"]:
                raise RuntimeError("observation exceeds its resource envelope")
            scenario_rows[row["scenarioId"]].append(row)
    required_families = set(inventory["spec"]["requiredFamilies"])
    observed_families = {scenario["family"] for scenario in plan["scenarios"]}
    if observed_families != required_families:
        raise RuntimeError("plan does not cover the exact required family matrix")
    summaries, speedups, hot_speedups = [], [], []
    groups: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    total_measured = 0
    for scenario in plan["scenarios"]:
        rows = scenario_rows.get(scenario["scenarioId"], [])
        measured_ngkg = [row for row in rows if row["engine"] == "ngkg-rust" and row["trialPhase"] == "measured"]
        artifacts = {row["artifactRootSha256"] for row in measured_ngkg}
        if len(artifacts) > 1:
            raise RuntimeError(f"artifact identity changed or disappeared for {scenario['scenarioId']}")
        ngkg = stats(rows, scenario, "ngkg-rust")
        jena = stats(rows, scenario, "external-apache-jena") if scenario["requireExternalJena"] else None
        speedup = None if jena is None else jena["p50Nanoseconds"] * 1000 // ngkg["p50Nanoseconds"]
        if scenario["maximumP95Nanoseconds"] and ngkg["p95Nanoseconds"] > scenario["maximumP95Nanoseconds"]:
            raise RuntimeError(f"p95 threshold failed for {scenario['scenarioId']}")
        if ngkg["medianThroughputPerSecond"] < scenario["minimumThroughputPerSecond"]:
            raise RuntimeError(f"throughput threshold failed for {scenario['scenarioId']}")
        if scenario["minimumSpeedupMilliX"] and (speedup is None or speedup < scenario["minimumSpeedupMilliX"]):
            raise RuntimeError(f"speedup threshold failed for {scenario['scenarioId']}")
        if scenario["maximumCostMicroUsdPerMillion"] and ngkg["medianCostMicroUsdPerMillion"] > scenario["maximumCostMicroUsdPerMillion"]:
            raise RuntimeError(f"cost threshold failed for {scenario['scenarioId']}")
        summary = {"scenarioId": scenario["scenarioId"], "ngkg": ngkg, "externalJena": jena, "speedupMilliX": speedup}
        summaries.append(summary)
        groups[scenario["capacityGroup"]].append((scenario, summary))
        total_measured += scenario["measuredTrials"] * (2 if jena else 1)
        if speedup is not None:
            speedups.append((jena["p50Nanoseconds"], ngkg["p50Nanoseconds"]))
            if scenario["cacheState"] == "hot":
                hot_speedups.append(speedup)
    for name, points in groups.items():
        points.sort(key=lambda pair: pair[0]["scaleOrdinal"])
        if [pair[0]["scaleOrdinal"] for pair in points] != list(range(len(points))):
            raise RuntimeError(f"capacity group {name} is not dense")
        if len(points) > 1 and points[-1][1]["ngkg"]["medianThroughputPerSecond"] < points[0][1]["ngkg"]["medianThroughputPerSecond"]:
            raise RuntimeError(f"capacity group {name} regressed at its largest scale point")
        artifact_sets = []
        for scenario, _ in points:
            values = {row["artifactRootSha256"] for row in scenario_rows[scenario["scenarioId"]] if row["engine"] == "ngkg-rust" and row["trialPhase"] == "measured" and row["artifactRootSha256"] is not None}
            artifact_sets.extend(values)
        if len(set(artifact_sets)) > 1:
            raise RuntimeError(f"capacity group {name} changed artifact identity")
    gates = inventory["spec"]["claimGates"]
    if not speedups:
        raise RuntimeError("no external Jena comparison scenarios were measured")
    numerator = math.prod(jena * 1000 for jena, _ in speedups)
    denominator = math.prod(ngkg for _, ngkg in speedups)
    geometric_speedup = integer_nth_root(numerator // denominator, len(speedups))
    if geometric_speedup < gates["selectiveGeometricMeanSpeedupMilliX"]:
        raise RuntimeError("selective geometric-mean Jena comparison failed")
    if not hot_speedups or nearest(hot_speedups, 50) < gates["hotQueryMedianSpeedupMilliX"]:
        raise RuntimeError("hot-query median Jena comparison failed")
    ingestion_rows = [row for scenario in plan["scenarios"] if scenario["family"] == "trig-ingestion" for row in scenario_rows[scenario["scenarioId"]] if row["engine"] == "ngkg-rust" and row["trialPhase"] == "measured"]
    ingest_bytes = 100 * 1024**3
    if not any(row["inputBytes"] >= ingest_bytes and row["durationNanoseconds"] <= gates["ingest100GiBMaximumSeconds"] * 1_000_000_000 for row in ingestion_rows):
        raise RuntimeError("100-GiB ingestion target lacks a passing measured trial")
    traversal = [summary for scenario, summary in zip(plan["scenarios"], summaries) if scenario["family"] == "property-path"]
    if not traversal or max(item["ngkg"]["medianThroughputPerSecond"] for item in traversal) < gates["propertyPathMinimumEdgesPerSecond"]:
        raise RuntimeError("property-path edge-throughput target failed")
    if max(scenario["concurrency"] for scenario in plan["scenarios"] if scenario["family"] == "concurrent-sparql") < gates["sustainedConcurrentUsers"]:
        raise RuntimeError("250-user sustained-concurrency point is missing")
    certificate = {
        "formatVersion": 1, "planSha256": plan_sha, "reportSetSha256": digest(reports),
        "scenarios": summaries, "qualifiedFamilies": sorted(observed_families),
        "deterministicResults": True, "autoscalingEvidenceBound": True,
        "noExcludedTrials": True, "failedThresholdCount": 0, "complete": True,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_bytes(canonical(certificate) + b"\n")
    os.replace(temporary, args.output)
    print(json.dumps({"complete": True, "scenarios": len(summaries), "measuredTrials": total_measured, "selectiveGeometricMeanSpeedupMilliX": geometric_speedup, "hotMedianSpeedupMilliX": nearest(hot_speedups, 50), "certificateSha256": digest(certificate)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
