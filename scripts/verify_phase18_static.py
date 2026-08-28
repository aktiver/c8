#!/usr/bin/env python3
"""Static Phase 18 contract checks for environments without Rust or Helm."""

from __future__ import annotations

import pathlib
import sys

import yaml


def require(text: str, token: str, errors: list[str], source: str) -> None:
    if token not in text:
        errors.append(f"{source} is missing {token!r}")


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    errors: list[str] = []
    locator = (root / "crates/ngkg-locator/src/lib.rs").read_text(encoding="utf-8")
    hydration = (root / "crates/ngkg-hydration/src/lib.rs").read_text(encoding="utf-8")
    hpa = (root / "charts/ngkg-workloads/templates/autoscaling.yaml").read_text(encoding="utf-8")
    plane = (root / "charts/ngkg-workloads/templates/online-data-plane.yaml").read_text(encoding="utf-8")
    worker = (root / "services/distributed-worker/src/main.rs").read_text(encoding="utf-8")
    values = yaml.safe_load((root / "charts/ngkg-workloads/values.yaml").read_text(encoding="utf-8"))

    for token in (
        "compile_sharded_locator",
        "MmapMut::map_anon",
        "make_read_only",
        "partition_point",
        "ChecksumMismatch",
    ):
        require(locator, token, errors, "locator kernel")
    for token in (
        "hydrate_sharded_payload",
        "with_row_groups",
        "std::thread::scope",
        "PayloadChecksumMismatch",
        "query_ordinal",
        "multiplicity",
    ):
        require(hydration, token, errors, "hydration kernel")
    require(worker, '"compile-mmap-locator"', errors, "distributed worker")
    require(hpa, "averageUtilization: {{ .Values.metrics.cpuUtilizationTargetPercent }}", errors, "HPA")
    require(hpa, "averageUtilization: {{ .Values.metrics.memoryUtilizationTargetPercent }}", errors, "HPA")
    require(plane, "requiredDuringSchedulingIgnoredDuringExecution", errors, "online data plane")
    require(plane, "NGKG_HYDRATION_WORKER_THREADS", errors, "online data plane")
    require(plane, "NGKG_LOCATOR_MMAP_MODE", errors, "online data plane")

    saturation = values["hpcRuntime"]["nodeSaturationTargetPercent"]
    cpu = values["metrics"]["cpuUtilizationTargetPercent"]
    memory = values["metrics"]["memoryUtilizationTargetPercent"]
    if saturation != 80 or cpu > saturation or memory > saturation:
        errors.append("default CPU/memory saturation policy must be capped at 80 percent")

    for error in errors:
        print(error, file=sys.stderr)
    if not errors:
        print("Phase 18 static contracts passed")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())

