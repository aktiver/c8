#!/usr/bin/env python3
"""Build a content-bound Phase 40.13.23 performance plan and driver catalog."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from typing import Any

import yaml

SHA = set("0123456789abcdef")
FAMILIES = {"trig-ingestion", "semantic-compilation", "offline-reasoning", "property-path", "sparql-query", "concurrent-sparql", "recovery"}


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def valid_sha(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= SHA


def load(path: pathlib.Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream) if path.suffix in {".yaml", ".yml"} else json.load(stream)


def stable_partition(scenario_id: str, input_sha256: str, count: int) -> int:
    value = hashlib.sha256(f"{scenario_id}\0{input_sha256}".encode()).digest()
    return int.from_bytes(value[:8], "big") % count


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=pathlib.Path, required=True)
    parser.add_argument("--definitions", type=pathlib.Path, required=True)
    parser.add_argument("--hardware", type=pathlib.Path, required=True)
    parser.add_argument("--pricing", type=pathlib.Path, required=True)
    parser.add_argument("--autoscaling-evidence", type=pathlib.Path, required=True)
    parser.add_argument("--ngkg-image-sha256", required=True)
    parser.add_argument("--external-jena-image-sha256")
    parser.add_argument("--partition-count", type=int, required=True)
    parser.add_argument("--plan-output", type=pathlib.Path, required=True)
    parser.add_argument("--catalog-output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.partition_count <= 65536 or not valid_sha(args.ngkg_image_sha256):
        raise ValueError("partition count or NGKG release image digest is invalid")
    if args.external_jena_image_sha256 is not None and not valid_sha(args.external_jena_image_sha256):
        raise ValueError("external Jena image digest is invalid")
    inventory, definitions = load(args.inventory), load(args.definitions)
    hardware, pricing, autoscaling = load(args.hardware), load(args.pricing), load(args.autoscaling_evidence)
    if autoscaling.get("complete") is not True or autoscaling.get("targetPercent") != 80:
        raise ValueError("a complete Phase 40.13.20 live 80-percent autoscaling report is required")
    if set(definitions) != {"formatVersion", "runId", "scenarios"} or definitions["formatVersion"] != 1:
        raise ValueError("definitions header is invalid")
    hardware_fields = {"formatVersion", "collectedEpochSeconds", "kubernetesProvider", "region", "nodeType", "architecture", "cpuModel", "physicalCoresPerNode", "numaNodesPerNode", "memoryBytesPerNode", "networkBitsPerSecond", "storageClass", "kernelVersion", "cgroupVersion", "containerRuntime", "complete"}
    pricing_fields = {"formatVersion", "observedEpochSeconds", "provider", "region", "currency", "nodeMicroUsdPerHour", "objectReadMicroUsdPerMillion", "objectWriteMicroUsdPerMillion", "objectStorageMicroUsdPerGibMonth", "egressMicroUsdPerGib", "sourceUrlSha256", "complete"}
    if set(hardware) != hardware_fields or hardware.get("complete") is not True or hardware.get("formatVersion") != 1 or hardware.get("cgroupVersion") != 2:
        raise ValueError("closed complete cgroup-v2 hardware evidence is required")
    if set(pricing) != pricing_fields or pricing.get("complete") is not True or pricing.get("formatVersion") != 1 or pricing.get("currency") != "USD" or not valid_sha(pricing.get("sourceUrlSha256")):
        raise ValueError("complete observed hardware and USD pricing evidence is required")
    specification = inventory["spec"]
    trial_policy = specification["trialPolicy"]
    dataset_minimums = specification["representativeDatasetMinimums"]
    baseline_families = set(specification["externalBaselines"]["apacheJena"]["requiredFor"])
    scenarios, catalog, seen = [], {}, set()
    groups: dict[str, list[dict[str, Any]]] = {}
    datasets: dict[str, dict[str, Any]] = {}
    required = {
        "scenarioId", "family", "expectedResultSha256", "capacityGroup", "scaleOrdinal",
        "cacheState", "concurrency", "requestedNodes", "requestedCpuMillis",
        "requestedMemoryBytes", "warmupTrials", "measuredTrials", "requireExternalJena",
        "maximumP95Nanoseconds", "minimumThroughputPerSecond", "minimumSpeedupMilliX",
        "maximumCostMicroUsdPerMillion", "descriptor",
    }
    for definition in definitions["scenarios"]:
        if set(definition) != required or definition["scenarioId"] in seen:
            raise ValueError("scenario definition has unknown/missing fields or a duplicate identity")
        seen.add(definition["scenarioId"])
        if definition["family"] not in FAMILIES or not valid_sha(definition["expectedResultSha256"]):
            raise ValueError("scenario family or expected result digest is invalid")
        if definition["requireExternalJena"] and args.external_jena_image_sha256 is None:
            raise ValueError("an external Jena scenario lacks an image digest")
        if definition["family"] in baseline_families and not definition["requireExternalJena"]:
            raise ValueError("an applicable family omitted the external Jena competitor baseline")
        if definition["warmupTrials"] < trial_policy["minimumWarmupTrialsPerEngine"] or definition["measuredTrials"] < trial_policy["minimumMeasuredTrialsPerEngine"] or definition["concurrency"] < 1:
            raise ValueError("scenario trial count or concurrency is below the inventory minimum")
        dataset = definition["descriptor"].get("dataset")
        if not isinstance(dataset, dict) or set(dataset) != {"sha256", "snapshotSha256", "bytes", "namedGraphs", "triples", "propertyPathEdges", "owl2DlQualified", "pinnedImports", "provenance"}:
            raise ValueError("scenario lacks a closed representative dataset descriptor")
        if not valid_sha(dataset["sha256"]) or not valid_sha(dataset["snapshotSha256"]):
            raise ValueError("dataset or snapshot digest is invalid")
        if dataset["bytes"] < dataset_minimums["bytesPerEnterpriseDataset"] or dataset["namedGraphs"] < dataset_minimums["namedGraphsPerDataset"] or dataset["triples"] < dataset_minimums["triplesPerDataset"]:
            raise ValueError("dataset is below the representative enterprise minimum")
        if definition["family"] == "property-path" and dataset["propertyPathEdges"] < dataset_minimums["propertyPathEdgesPerDataset"]:
            raise ValueError("property-path dataset is below the edge minimum")
        if not dataset["owl2DlQualified"] or not dataset["pinnedImports"] or not dataset["provenance"]:
            raise ValueError("dataset lacks OWL 2 DL, pinned-import, or provenance evidence")
        prior_dataset = datasets.setdefault(dataset["sha256"], dataset)
        if prior_dataset != dataset:
            raise ValueError("one dataset digest has conflicting metadata")
        descriptor_sha = digest(definition["descriptor"])
        scenario = {name: definition[name] for name in required - {"descriptor"}}
        scenario["inputSha256"] = descriptor_sha
        scenario["partition"] = stable_partition(definition["scenarioId"], descriptor_sha, args.partition_count)
        scenarios.append(scenario)
        catalog[definition["scenarioId"]] = definition["descriptor"]
        groups.setdefault(definition["capacityGroup"], []).append(scenario)
    for group, points in groups.items():
        points.sort(key=lambda point: point["scaleOrdinal"])
        if [point["scaleOrdinal"] for point in points] != list(range(len(points))):
            raise ValueError(f"capacity group {group} has a non-dense scale sequence")
        first = points[0]
        for previous, point in zip(points, points[1:]):
            if point["family"] != first["family"] or point["expectedResultSha256"] != first["expectedResultSha256"]:
                raise ValueError(f"capacity group {group} changes meaning across scale")
            if point["requestedNodes"] <= previous["requestedNodes"] or point["requestedCpuMillis"] <= previous["requestedCpuMillis"] or point["requestedMemoryBytes"] <= previous["requestedMemoryBytes"]:
                raise ValueError(f"capacity group {group} resources do not strictly increase")
        required_nodes = set(specification["capacityPoints"]["minimumNodeCounts"])
        if not required_nodes <= {point["requestedNodes"] for point in points}:
            raise ValueError(f"capacity group {group} omits a required node scale point")
    if len(datasets) < dataset_minimums["distinctDatasets"]:
        raise ValueError("qualification lacks the required distinct enterprise datasets")
    concurrent_levels = {scenario["concurrency"] for scenario in scenarios if scenario["family"] == "concurrent-sparql"}
    if not set(specification["capacityPoints"]["concurrencyLevels"]) <= concurrent_levels:
        raise ValueError("concurrent SPARQL matrix omits a required client level")
    scenarios.sort(key=lambda item: item["scenarioId"])
    plan = {
        "formatVersion": 1,
        "runId": definitions["runId"],
        "benchmarkInventorySha256": digest(inventory),
        "ngkgImageSha256": args.ngkg_image_sha256,
        "externalJenaImageSha256": args.external_jena_image_sha256,
        "hardwareSha256": digest(hardware),
        "pricingSha256": digest(pricing),
        "autoscalingEvidenceSha256": digest(autoscaling),
        "partitionCount": args.partition_count,
        "scenarios": scenarios,
    }
    case_catalog = {"formatVersion": 1, "scenarios": dict(sorted(catalog.items()))}
    for path, value in ((args.plan_output, plan), (args.catalog_output, case_catalog)):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical(value) + b"\n")
    print(json.dumps({"runId": plan["runId"], "scenarios": len(scenarios), "planSha256": digest(plan)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
