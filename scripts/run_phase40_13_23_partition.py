#!/usr/bin/env python3
"""Run one isolated Phase 40.13.23 scenario partition without dropping trials."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import shlex
import subprocess
import tempfile
import time
from typing import Any

import yaml

SHA = set("0123456789abcdef")
OBSERVATION_FIELDS = {
    "formatVersion", "engine", "engineVersion", "scenarioId", "trialPhase", "trial",
    "durationNanoseconds", "operations", "workItems", "inputBytes", "outputItems", "cpuTimeNanoseconds",
    "peakRssBytes", "bytesRead", "bytesWritten", "nodesActivated", "cpuMillisActivated",
    "ramBytesActivated", "resultSha256", "artifactRootSha256", "autoscalingEvidenceSha256",
    "costMicroUsd", "complete", "errorClass",
}


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def valid_sha(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= SHA


def load(path: pathlib.Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream) if path.suffix in {".yaml", ".yml"} else json.load(stream)


def resolve_paths(value: Any, base: pathlib.Path, key: str = "") -> Any:
    if isinstance(value, dict):
        return {name: resolve_paths(item, base, name) for name, item in value.items()}
    if isinstance(value, list):
        return [resolve_paths(item, base, key) for item in value]
    if isinstance(value, str) and key.endswith("Path") and not pathlib.Path(value).is_absolute():
        return str((base / value).resolve())
    return value


def invoke(command: str, request: dict[str, Any], engine: str, version: str, timeout: int, max_bytes: int) -> dict[str, Any]:
    argv = shlex.split(command)
    if not argv:
        raise RuntimeError("benchmark driver command is empty")
    environment = os.environ.copy()
    environment.update({"OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "VECLIB_MAXIMUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1"})
    with tempfile.TemporaryDirectory(prefix="ngkg-performance-") as directory:
        request_path = pathlib.Path(directory) / "request.json"
        request_path.write_bytes(canonical(request))
        started = time.monotonic_ns()
        process = subprocess.run([*argv, str(request_path)], capture_output=True, check=False, timeout=timeout, env=environment)
        wall = time.monotonic_ns() - started
    if len(process.stdout) > max_bytes or len(process.stderr) > max_bytes:
        raise RuntimeError("benchmark driver output exceeds its byte ceiling")
    if process.returncode:
        raise RuntimeError(f"benchmark driver exited {process.returncode}: {process.stderr[:512]!r}")
    try:
        row = json.loads(process.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("benchmark driver did not emit one UTF-8 JSON object") from error
    if set(row) != OBSERVATION_FIELDS or row["formatVersion"] != 1:
        raise RuntimeError("benchmark observation has unknown or missing fields")
    if row["engine"] != engine or row["engineVersion"] != version or row["scenarioId"] != request["scenarioId"] or row["trialPhase"] != request["trialPhase"] or row["trial"] != request["trial"]:
        raise RuntimeError("benchmark observation identity or version differs from its request")
    if row["complete"] is not True or row["errorClass"] is not None:
        raise RuntimeError("failed or partial trials may not be converted to measurements")
    integers = OBSERVATION_FIELDS - {"engine", "engineVersion", "scenarioId", "trialPhase", "resultSha256", "artifactRootSha256", "autoscalingEvidenceSha256", "complete", "errorClass"}
    if any(not isinstance(row[name], int) or row[name] < 0 for name in integers):
        raise RuntimeError("benchmark observation has an invalid counter")
    if row["durationNanoseconds"] <= 0 or row["operations"] <= 0 or row["workItems"] <= 0 or row["nodesActivated"] <= 0:
        raise RuntimeError("benchmark observation lacks finite work and resource evidence")
    if row["durationNanoseconds"] > wall + 5_000_000_000:
        raise RuntimeError("reported duration exceeds coordinator wall time")
    if not valid_sha(row["resultSha256"]) or not valid_sha(row["autoscalingEvidenceSha256"]):
        raise RuntimeError("benchmark observation digest is invalid")
    if row["artifactRootSha256"] is not None and not valid_sha(row["artifactRootSha256"]):
        raise RuntimeError("artifact-root digest is invalid")
    row.pop("formatVersion")
    row.pop("engineVersion")
    row.pop("errorClass")
    return row


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=pathlib.Path, required=True)
    parser.add_argument("--inventory", type=pathlib.Path, required=True)
    parser.add_argument("--catalog", type=pathlib.Path, required=True)
    parser.add_argument("--pricing", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--partition", type=int)
    parser.add_argument("--worker-id", default=os.environ.get("HOSTNAME", "local-worker"))
    parser.add_argument("--ngkg-driver", required=True)
    parser.add_argument("--external-jena-driver")
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--max-output-bytes", type=int, default=4 * 1024 * 1024)
    args = parser.parse_args()
    if not 1 <= args.timeout_seconds <= 86400 or not 1024 <= args.max_output_bytes <= 64 * 1024 * 1024:
        raise ValueError("timeout or output-byte ceiling is invalid")
    args.catalog = args.catalog.resolve()
    plan, inventory, catalog, pricing = load(args.plan), load(args.inventory), load(args.catalog), load(args.pricing)
    if digest(inventory) != plan["benchmarkInventorySha256"] or set(catalog) != {"formatVersion", "scenarios"}:
        raise ValueError("inventory or catalog differs from the immutable plan")
    if digest(pricing) != plan["pricingSha256"] or pricing.get("complete") is not True:
        raise ValueError("pricing evidence differs from the immutable plan")
    partition = args.partition if args.partition is not None else int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
    if not 0 <= partition < plan["partitionCount"]:
        raise ValueError("partition is outside the dense plan")
    versions = {"ngkg-rust": inventory["spec"]["ngkgRuntime"]["version"], "external-apache-jena": inventory["spec"]["externalBaselines"]["apacheJena"]["version"]}
    assigned = [scenario for scenario in plan["scenarios"] if scenario["partition"] == partition]
    observations = []
    for scenario in assigned:
        descriptor = catalog["scenarios"].get(scenario["scenarioId"])
        if digest(descriptor) != scenario["inputSha256"]:
            raise ValueError("scenario descriptor differs from its plan digest")
        descriptor = resolve_paths(descriptor, args.catalog.parent)
        engines = ["ngkg-rust"] + (["external-apache-jena"] if scenario["requireExternalJena"] else [])
        if "external-apache-jena" in engines and not args.external_jena_driver:
            raise ValueError("external Jena comparison was required but no isolated driver was supplied")
        commands = {"ngkg-rust": args.ngkg_driver, "external-apache-jena": args.external_jena_driver}
        for trial_phase, count in (("warmup", scenario["warmupTrials"]), ("measured", scenario["measuredTrials"])):
            for trial in range(count):
                order = engines if int(hashlib.sha256(f"{scenario['scenarioId']}:{trial_phase}:{trial}".encode()).hexdigest(), 16) % 2 == 0 else list(reversed(engines))
                for engine in order:
                    request = {"formatVersion": 1, "runId": plan["runId"], "scenarioId": scenario["scenarioId"], "family": scenario["family"], "trialPhase": trial_phase, "trial": trial, "cacheState": scenario["cacheState"], "concurrency": scenario["concurrency"], "resourceEnvelope": {"nodes": scenario["requestedNodes"], "cpuMillis": scenario["requestedCpuMillis"], "memoryBytes": scenario["requestedMemoryBytes"]}, "hardwareSha256": plan["hardwareSha256"], "pricingSha256": plan["pricingSha256"], "pricing": pricing, "autoscalingEvidenceSha256": plan["autoscalingEvidenceSha256"], "descriptor": descriptor}
                    row = invoke(commands[engine], request, engine, versions[engine], args.timeout_seconds, args.max_output_bytes)
                    if row["resultSha256"] != scenario["expectedResultSha256"] or row["autoscalingEvidenceSha256"] != plan["autoscalingEvidenceSha256"]:
                        raise RuntimeError("semantic result or scaling evidence changed during measurement")
                    if row["nodesActivated"] > scenario["requestedNodes"] or row["cpuMillisActivated"] > scenario["requestedCpuMillis"] or row["ramBytesActivated"] > scenario["requestedMemoryBytes"]:
                        raise RuntimeError("measurement exceeded its immutable resource envelope")
                    observations.append(row)
    observations.sort(key=lambda row: (row["scenarioId"], row["engine"], row["trialPhase"], row["trial"]))
    report = {"formatVersion": 1, "planSha256": digest(plan), "partition": partition, "workerId": args.worker_id, "observations": observations, "complete": True}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_bytes(canonical(report) + b"\n")
    os.replace(temporary, args.output)
    print(json.dumps({"partition": partition, "scenarios": len(assigned), "observations": len(observations), "reportSha256": digest(report)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
