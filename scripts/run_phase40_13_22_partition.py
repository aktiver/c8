#!/usr/bin/env python3
"""Run one deterministic Phase 40.13.22 standards partition.

Drivers receive one JSON request path as their final argument and must emit one
JSON observation on stdout.  The coordinator bounds concurrency, time and
output, disables nested native thread pools, and atomically writes a report only
after every assigned case has produced a complete NGKG/oracle pair.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import pathlib
import shlex
import subprocess
import tempfile
import time
from typing import Any

FORMAT_VERSION = 1
SHA256 = set("0123456789abcdef")
ORACLE_KEYS = {"w3c-expected": "w3c", "apache-jena": "jena", "hermit": "hermit"}


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def valid_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= SHA256


def cpu_limit() -> int:
    limits = [os.cpu_count() or 1]
    if hasattr(os, "sched_getaffinity"):
        limits.append(len(os.sched_getaffinity(0)))
    cpu_max = pathlib.Path("/sys/fs/cgroup/cpu.max")
    if cpu_max.is_file():
        quota, period, *_ = cpu_max.read_text(encoding="ascii").split()
        if quota != "max":
            limits.append(max(1, math.ceil(int(quota) / int(period))))
    return max(1, min(limits))


def stable_partition(case: dict[str, Any], count: int) -> int:
    identity = f"{case['caseId']}\0{case['inputSha256']}".encode()
    return int.from_bytes(hashlib.sha256(identity).digest()[:8], "big") % count


def load_json(path: pathlib.Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def validate_plan(plan: dict[str, Any], catalog: dict[str, Any]) -> None:
    required = {"formatVersion", "suiteInventorySha256", "partitionCount", "cases"}
    if set(plan) != required or plan["formatVersion"] != FORMAT_VERSION:
        raise ValueError("invalid or non-canonical plan header")
    count = plan["partitionCount"]
    if not isinstance(count, int) or not 1 <= count <= 65536:
        raise ValueError("partitionCount must be in [1,65536]")
    if not valid_sha256(plan["suiteInventorySha256"]):
        raise ValueError("suite inventory digest is invalid")
    if not isinstance(catalog, dict) or set(catalog) != {"formatVersion", "cases"}:
        raise ValueError("invalid case catalog")
    catalog_cases = catalog["cases"]
    if catalog["formatVersion"] != 1 or not isinstance(catalog_cases, dict):
        raise ValueError("invalid case catalog version or case map")
    previous = ""
    for case in plan["cases"]:
        case_id = case.get("caseId")
        if not isinstance(case_id, str) or case_id <= previous or case_id not in catalog_cases:
            raise ValueError("case IDs must be unique, sorted and present in the catalog")
        if digest(catalog_cases[case_id]) != case.get("inputSha256"):
            raise ValueError(f"case descriptor digest differs for {case_id}")
        if stable_partition(case, count) != case.get("partition"):
            raise ValueError(f"stable partition differs for {case_id}")
        previous = case_id


def run_driver(
    command: str,
    request: dict[str, Any],
    expected_engine: str,
    expected_version: str,
    timeout_seconds: int,
    max_output_bytes: int,
) -> dict[str, Any]:
    argv = shlex.split(command)
    if not argv:
        raise RuntimeError("driver command is empty")
    environment = os.environ.copy()
    environment.update({
        "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1",
        "VECLIB_MAXIMUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1", "RAYON_NUM_THREADS": "1",
    })
    with tempfile.TemporaryDirectory(prefix="ngkg-standards-") as directory:
        request_path = pathlib.Path(directory) / "request.json"
        request_path.write_bytes(canonical_bytes(request))
        started = time.monotonic_ns()
        process = subprocess.run(
            [*argv, str(request_path)], check=False, capture_output=True,
            timeout=timeout_seconds, env=environment,
        )
        duration_ms = (time.monotonic_ns() - started) // 1_000_000
    if len(process.stdout) > max_output_bytes or len(process.stderr) > max_output_bytes:
        raise RuntimeError("driver output exceeds the configured byte ceiling")
    if process.returncode != 0:
        detail = process.stderr.decode("utf-8", errors="replace")[:512]
        raise RuntimeError(f"driver exited {process.returncode}: {detail}")
    try:
        observation = json.loads(process.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("driver did not emit one UTF-8 JSON object") from error
    required = {"formatVersion", "engine", "engineVersion", "caseId", "outcome", "resultSha256", "errorClass", "complete"}
    if set(observation) != required or observation["formatVersion"] != 1:
        raise RuntimeError("driver observation has an unknown or missing field")
    if observation["engine"] != expected_engine or observation["engineVersion"] != expected_version or observation["caseId"] != request["caseId"]:
        raise RuntimeError("driver engine, version, or case identity differs from its request")
    if not observation["complete"] or observation["outcome"] not in {"success", "failure"}:
        raise RuntimeError("partial or invalid driver outcome")
    if observation["outcome"] == "success":
        if not valid_sha256(observation["resultSha256"]) or observation["errorClass"] is not None:
            raise RuntimeError("successful driver output lacks one canonical digest")
    elif observation["resultSha256"] is not None or not isinstance(observation["errorClass"], str):
        raise RuntimeError("failed driver output lacks one stable error class")
    observation["durationMilliseconds"] = duration_ms
    return observation


def resolve_descriptor_paths(value: Any, base: pathlib.Path, key: str = "") -> Any:
    if isinstance(value, dict):
        return {name: resolve_descriptor_paths(item, base, name) for name, item in value.items()}
    if isinstance(value, list):
        return [resolve_descriptor_paths(item, base, key) for item in value]
    if isinstance(value, str) and key.endswith("Path") and not pathlib.Path(value).is_absolute():
        return str((base / value).resolve())
    return value


def run_case(case: dict[str, Any], descriptor: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    descriptor = resolve_descriptor_paths(descriptor, args.case_catalog.parent)
    request = {"formatVersion": 1, "caseId": case["caseId"], "family": case["family"], "descriptor": descriptor}
    ngkg = run_driver(args.ngkg_driver, request, "ngkg", args.engine_versions["ngkg"], args.timeout_seconds, args.max_output_bytes)
    oracle_name = case["oracle"]
    oracle = run_driver(getattr(args, f"{ORACLE_KEYS[oracle_name]}_driver"), request, oracle_name, args.engine_versions[oracle_name], args.timeout_seconds, args.max_output_bytes)
    expected = case["expectedOutcome"]
    if ngkg["outcome"] != expected or oracle["outcome"] != expected:
        raise RuntimeError(f"terminal outcome mismatch for {case['caseId']}")
    if expected == "success":
        if ngkg["resultSha256"] != oracle["resultSha256"]:
            raise RuntimeError(f"canonical differential mismatch for {case['caseId']}")
        expected_hash = case.get("expectedResultSha256")
        if expected_hash is not None and ngkg["resultSha256"] != expected_hash:
            raise RuntimeError(f"normative result mismatch for {case['caseId']}")
    else:
        expected_class = case.get("expectedErrorClass")
        if ngkg["errorClass"] != expected_class or oracle["errorClass"] != expected_class:
            raise RuntimeError(f"failure-class mismatch for {case['caseId']}")
    return {
        "caseId": case["caseId"], "family": case["family"], "partition": case["partition"],
        "ngkgOutcome": ngkg["outcome"], "oracleOutcome": oracle["outcome"],
        "ngkgResultSha256": ngkg["resultSha256"], "oracleResultSha256": oracle["resultSha256"],
        "ngkgErrorClass": ngkg["errorClass"], "oracleErrorClass": oracle["errorClass"],
        "complete": True, "durationMilliseconds": ngkg["durationMilliseconds"] + oracle["durationMilliseconds"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=pathlib.Path, required=True)
    parser.add_argument("--suite-inventory", type=pathlib.Path, required=True)
    parser.add_argument("--case-catalog", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--partition", type=int, default=None)
    parser.add_argument("--worker-id", default=os.environ.get("HOSTNAME", "local-worker"))
    parser.add_argument("--ngkg-driver", required=True)
    parser.add_argument("--w3c-driver", required=True)
    parser.add_argument("--jena-driver", required=True)
    parser.add_argument("--hermit-driver", required=True)
    parser.add_argument("--jobs", type=int, default=max(1, cpu_limit() - 1))
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--max-output-bytes", type=int, default=4 * 1024 * 1024)
    args = parser.parse_args()
    args.case_catalog = args.case_catalog.resolve()
    if args.jobs < 1 or not 1 <= args.timeout_seconds <= 86400 or not 1024 <= args.max_output_bytes <= 64 * 1024 * 1024:
        raise ValueError("jobs, timeout, or output-byte ceiling is invalid")
    plan, catalog = load_json(args.plan), load_json(args.case_catalog)
    suite = load_json(args.suite_inventory)
    if digest(suite) != plan.get("suiteInventorySha256"):
        raise ValueError("suite inventory differs from the immutable plan")
    args.engine_versions = {
        "ngkg": suite["oracles"]["ngkg"]["version"],
        "w3c-expected": suite["oracles"]["w3cExpected"]["version"],
        "apache-jena": suite["oracles"]["apacheJena"]["version"],
        "hermit": suite["oracles"]["hermit"]["version"],
    }
    validate_plan(plan, catalog)
    partition = args.partition
    if partition is None:
        partition = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
    if not 0 <= partition < plan["partitionCount"]:
        raise ValueError("partition is outside the dense plan")
    assigned = [case for case in plan["cases"] if case["partition"] == partition]
    jobs = max(1, min(args.jobs, cpu_limit(), len(assigned) or 1, 64))
    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
        futures = [pool.submit(run_case, case, catalog["cases"][case["caseId"]], args) for case in assigned]
        observations = [future.result() for future in futures]
    observations.sort(key=lambda item: item["caseId"])
    report = {
        "formatVersion": 1, "planSha256": digest(plan), "partition": partition,
        "workerId": args.worker_id, "observations": observations, "complete": True,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_bytes(canonical_bytes(report) + b"\n")
    os.replace(temporary, args.output)
    print(json.dumps({"partition": partition, "cases": len(observations), "reportSha256": digest(report)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
