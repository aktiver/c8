#!/usr/bin/env python3
"""Build a closed, content-bound Phase 40.13.24 qualification plan."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from typing import Any

import yaml

PROVIDERS = {"rke", "rke2", "eks", "aks", "gke"}
GATES = {"semantic-context-graph", "multinode-soak", "compute-chaos", "network-chaos", "storage-chaos", "upgrade", "rollback", "backup-restore", "helm", "image-provenance", "sbom", "cve", "license", "reproducible-build", "provider-portability"}
DISRUPTIVE = {"compute-chaos", "network-chaos", "storage-chaos", "upgrade", "rollback", "backup-restore"}
SHA = set("0123456789abcdef")


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


def validate_semantic(value: dict[str, Any]) -> None:
    hashes = ["owl2DlQualificationSha256", "snapshotSha256", "authorizedGraphSetSha256", "querySha256", "resultGraphSha256", "scalarOracleGraphSha256", "reasoningCertificateSha256"]
    if any(not valid_sha(value.get(name)) for name in hashes):
        raise ValueError("semantic prerequisite has an invalid identity")
    if value["resultGraphSha256"] != value["scalarOracleGraphSha256"] or value.get("domainCount", 0) < 3 or value.get("hopCount", 0) < 2 or value.get("reasonedOutputTriples", 0) < 1 or value.get("activatedNodes", 0) < 2 or value.get("activatedCpuMillis", 0) < 2000 or value.get("activatedMemoryBytes", 0) < 1 or value.get("queryForm") not in {"CONSTRUCT", "DESCRIBE"} or value.get("complete") is not True or value.get("proofCoverage") != "complete":
        raise ValueError("cross-domain OWL 2 DL context graph is not complete and oracle-equal")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=pathlib.Path, required=True)
    parser.add_argument("--definitions", type=pathlib.Path, required=True)
    parser.add_argument("--performance-certificate", type=pathlib.Path, required=True)
    parser.add_argument("--semantic-evidence", type=pathlib.Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--partition-count", type=int, required=True)
    parser.add_argument("--plan-output", type=pathlib.Path, required=True)
    parser.add_argument("--catalog-output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if not valid_sha(args.release_sha256) or not 1 <= args.partition_count <= 65536:
        raise ValueError("release identity or partition count is invalid")
    inventory = load(args.inventory)
    definitions = load(args.definitions)
    performance = load(args.performance_certificate)
    semantic = load(args.semantic_evidence)
    spec = inventory.get("spec", {})
    if set(spec.get("kubernetes", {}).get("providers", [])) != PROVIDERS or set(spec.get("requiredGates", [])) != GATES:
        raise ValueError("inventory omits a required provider or release gate")
    if spec["kubernetes"].get("autoscalingCpuPercent") != 80 or spec["kubernetes"].get("autoscalingMemoryPercent") != 80 or spec["kubernetes"].get("minimumWorkerNodes", 0) < 3:
        raise ValueError("inventory does not require three-node HA and 80-percent autoscaling")
    if performance.get("complete") is not True or performance.get("failedThresholdCount") != 0:
        raise ValueError("a complete Phase 40.13.23 performance certificate is required")
    validate_semantic(semantic)
    if set(definitions) != {"formatVersion", "runId", "scenarios"} or definitions["formatVersion"] != 1:
        raise ValueError("scenario definitions header is invalid")
    required = {"scenarioId", "provider", "gate", "expectedOutputSha256", "minimumNodes", "minimumCpuMillis", "minimumMemoryBytes", "minimumDurationSeconds", "disruptive", "approvalEvidenceSha256", "descriptor"}
    scenarios, catalog, seen, coverage = [], {}, set(), set()
    for item in definitions["scenarios"]:
        if set(item) != required or item["scenarioId"] in seen:
            raise ValueError("scenario fields are not closed or identity is duplicated")
        seen.add(item["scenarioId"])
        if item["provider"] not in PROVIDERS or item["gate"] not in GATES or not valid_sha(item["expectedOutputSha256"]):
            raise ValueError("scenario provider, gate, or result identity is invalid")
        disruptive = item["gate"] in DISRUPTIVE
        if item["disruptive"] is not disruptive:
            raise ValueError("scenario disruption classification is invalid")
        approval = item["approvalEvidenceSha256"]
        if disruptive != valid_sha(approval) or (not disruptive and approval is not None):
            raise ValueError("disruptive scenario lacks exact approval evidence")
        if item["minimumNodes"] < 3 or item["minimumCpuMillis"] < 3000 or item["minimumMemoryBytes"] < 1:
            raise ValueError("scenario does not exercise multi-node, multi-core resources")
        if item["gate"] == "multinode-soak" and item["minimumDurationSeconds"] < spec["soak"]["minimumDurationSeconds"]:
            raise ValueError("soak scenario is shorter than the release policy")
        descriptor_sha = digest(item["descriptor"])
        scenario = {key: item[key] for key in required - {"descriptor"}}
        scenario["inputSha256"] = descriptor_sha
        scenario["partition"] = stable_partition(item["scenarioId"], descriptor_sha, args.partition_count)
        scenarios.append(scenario)
        catalog[item["scenarioId"]] = item["descriptor"]
        coverage.add((item["provider"], item["gate"]))
    required_coverage = {(provider, gate) for provider in PROVIDERS for gate in GATES}
    if coverage != required_coverage:
        raise ValueError("every release gate must execute on every supported Kubernetes provider")
    scenarios.sort(key=lambda item: item["scenarioId"])
    plan = {
        "formatVersion": 1, "runId": definitions["runId"], "releaseSha256": args.release_sha256,
        "performanceCertificateSha256": hashlib.sha256(canonical(performance)).hexdigest(),
        "semanticEvidenceSha256": hashlib.sha256(canonical(semantic)).hexdigest(),
        "inventorySha256": hashlib.sha256(canonical(inventory)).hexdigest(),
        "partitionCount": args.partition_count, "scenarios": scenarios,
    }
    outputs = ((args.plan_output, plan), (args.catalog_output, {"formatVersion": 1, "scenarios": dict(sorted(catalog.items()))}))
    for path, value in outputs:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical(value) + b"\n")
    print(json.dumps({"planSha256": digest(plan), "scenarios": len(scenarios), "providers": len(PROVIDERS), "gates": len(GATES)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
