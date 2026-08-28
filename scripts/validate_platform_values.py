#!/usr/bin/env python3
"""Cross-field validation for NGKG control-plane Helm values."""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

import yaml


def binary_quantity(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)(Ki|Mi|Gi|Ti)", value)
    if not match:
        raise ValueError(f"unsupported binary Kubernetes quantity: {value}")
    shifts = {"Ki": 10, "Mi": 20, "Gi": 30, "Ti": 40}
    return int(match.group(1)) << shifts[match.group(2)]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("values", type=pathlib.Path)
    args = parser.parse_args()
    values = yaml.safe_load(args.values.read_text(encoding="utf-8"))
    errors: list[str] = []

    api = values["api"]
    autoscaling = api["autoscaling"]
    replicas = int(api["replicas"])
    minimum = int(autoscaling["minReplicas"])
    maximum = int(autoscaling["maxReplicas"])
    if minimum > maximum:
        errors.append("api.autoscaling.minReplicas cannot exceed maxReplicas")
    if autoscaling["enabled"] and not minimum <= replicas <= maximum:
        errors.append("api.replicas must be inside the HPA min/max range")
    for metric in ("cpuUtilizationTargetPercent", "memoryUtilizationTargetPercent"):
        target = int(autoscaling[metric])
        if target < 1 or target > 80:
            errors.append(f"api.autoscaling.{metric} must remain in the 1..80 headroom envelope")

    upload = api["sourceUpload"]
    max_bytes = int(upload["maxBytes"])
    max_in_flight = int(upload["maxInFlight"])
    scratch_bytes = binary_quantity(upload["scratchSizeLimit"])
    required_scratch = max_bytes * max_in_flight
    if required_scratch > scratch_bytes:
        errors.append(
            "api.sourceUpload.scratchSizeLimit must cover maxBytes multiplied by maxInFlight"
        )
    if int(upload["singlePutMaxBytes"]) > max_bytes:
        errors.append("api.sourceUpload.singlePutMaxBytes cannot exceed maxBytes")
    if int(upload["maxNamedGraphs"]) > 1_000_000:
        errors.append("api.sourceUpload.maxNamedGraphs cannot exceed the reviewed ceiling of 1000000")
    if max_in_flight > 128:
        errors.append("api.sourceUpload.maxInFlight cannot exceed the reviewed ceiling of 128")
    multipart_concurrency = int(upload["multipartConcurrency"])
    if multipart_concurrency > 64:
        errors.append("api.sourceUpload.multipartConcurrency cannot exceed the reviewed ceiling of 64")

    resources = values["resources"]["api"]
    if resources["requests"] != resources["limits"]:
        errors.append("API requests and limits must match so upload/parser pods receive deterministic resources")
    ephemeral = resources["requests"].get("ephemeral-storage")
    if ephemeral is None:
        errors.append("API resources must reserve ephemeral-storage for streaming TriG uploads")
    elif binary_quantity(ephemeral) < scratch_bytes:
        errors.append("API ephemeral-storage request must cover api.sourceUpload.scratchSizeLimit")
    memory = resources["requests"].get("memory")
    if memory is None:
        errors.append("API resources must reserve memory")
    else:
        multipart_memory = int(upload["multipartBufferBytes"]) * multipart_concurrency
        if multipart_memory > binary_quantity(memory) // 2:
            errors.append(
                "multipartBufferBytes multiplied by multipartConcurrency must remain below half the API memory request"
            )

    recovery = values["storageRecovery"]
    recovery_targets = json.loads(recovery["targetsJson"])["targets"]
    target_names = {target["name"] for target in recovery_targets}
    failure_domains = {target["failureDomain"] for target in recovery_targets if target["writable"]}
    if recovery["primaryTarget"] not in target_names:
        errors.append("storageRecovery.primaryTarget must exist in the trusted target registry")
    if len(target_names) != len(recovery_targets) or len(failure_domains) < 2:
        errors.append("storage recovery requires unique targets across at least two writable failure domains")
    recovery_scratch = binary_quantity(recovery["workerScratchSize"])
    recovery_task_bytes = int(recovery["maxTaskBytes"])
    recovery_in_flight = int(recovery["maxInFlightBytes"])
    if recovery_task_bytes > recovery_scratch:
        errors.append("storageRecovery.maxTaskBytes must fit workerScratchSize")
    if recovery_task_bytes > recovery_in_flight:
        errors.append("storageRecovery.maxTaskBytes cannot exceed maxInFlightBytes")
    if int(recovery["maxParallelism"]) > 4096:
        errors.append("storageRecovery.maxParallelism exceeds the reviewed Indexed Job ceiling")
    recovery_buffer_bytes = int(recovery["transfer"]["multipartBufferBytes"])
    recovery_buffer_count = int(recovery["transfer"]["multipartConcurrency"])
    recovery_memory = binary_quantity(recovery["workerMemory"])
    if recovery_buffer_bytes * recovery_buffer_count > recovery_memory // 2:
        errors.append("storage recovery multipart buffers exceed half the worker memory budget")

    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
