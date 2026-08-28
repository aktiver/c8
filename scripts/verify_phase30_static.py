#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 30 bounded worker Grace joins."""

from __future__ import annotations

import pathlib
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, tokens: tuple[str, ...] = ()) -> str:
    target = ROOT / path
    if not target.is_file():
        raise RuntimeError(f"missing required file: {path}")
    text = target.read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{path} is missing required token: {token}")
    return text


def main() -> int:
    grace = require(
        "crates/ngkg-grace-join/src/lib.rs",
        (
            "struct GraceJoinEngine",
            "struct GraceJoinIdentity",
            "ngkg-worker-grace-identity-v2",
            "grace_partition_for_binding",
            "RequestSpillLimit",
            "ProcessSpillLimit",
            "CorruptSpill",
            "create_new(true)",
            "sync_all",
            "VerifiedRecordReader",
            "max_build_rows",
            "max_probe_rows",
            "out_of_core_join_matches_exact_hot_key_bag_and_cleans_up",
            "output_limit_fails_closed_and_releases_spill_budget",
            "appended_spill_corruption_is_rejected_and_cleaned",
        ),
    )
    if "unsafe {" in grace:
        raise RuntimeError("Grace join introduces unsafe code")
    query_executor = require(
        "crates/ngkg-query-executor/src/lib.rs",
        ("ngkg-worker-grace-key-v1", "grace_partition_for_binding"),
    )
    if "ngkg-shuffle-key-v1" not in query_executor:
        raise RuntimeError("primary shuffle domain was removed")

    serving = require(
        "services/online-serving/src/main.rs",
        (
            "GraceJoinEngine::open",
            "validate_cached_shuffle_result",
            "format_version: 2",
            "worker_join_evidence",
            "x-ngkg-worker-join-mode",
            "x-ngkg-worker-join-spill-bytes",
            "x-ngkg-worker-join-buckets",
            "x-ngkg-worker-join-max-build-rows",
            "ngkg_worker_join_executions_total",
            "ngkg_worker_join_active_spill_bytes",
            "worker_join_execution_is_valid",
        ),
    )
    join = serving.index(".join(identity")
    partition_validation = serving.index("validate_shuffle_partition_rows", join)
    checksum = serving.index("canonical_sparql_multiset_sha256", partition_validation)
    if not join < partition_validation < checksum:
        raise RuntimeError("worker output is not partition- and checksum-validated after Grace join")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    online = values["onlineServing"]
    for key in (
        "workerJoinSpillSizeLimit",
        "maxWorkerJoinSpillBytes",
        "maxWorkerJoinSpillBytesPerRequest",
        "workerJoinBuckets",
        "maxWorkerJoinOpenFiles",
        "maxWorkerJoinBuildRows",
        "maxWorkerJoinProbeRows",
        "maxWorkerJoinRowBytes",
        "inMemoryJoinBuildRows",
    ):
        if key not in online:
            raise RuntimeError(f"Helm values omit {key}")
    fragment = values["resources"]["fragment"]
    if fragment["requests"] != fragment["limits"] or "ephemeral-storage" not in fragment["requests"]:
        raise RuntimeError("fragment worker does not retain Guaranteed QoS and ephemeral storage")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("resource autoscaling exceeds the 80-percent boundary")

    template = require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        (
            "NGKG_WORKER_JOIN_SPILL_ROOT",
            "NGKG_MAX_WORKER_JOIN_SPILL_BYTES",
            "NGKG_MAX_WORKER_JOIN_SPILL_BYTES_PER_REQUEST",
            "NGKG_WORKER_JOIN_BUCKETS",
            "NGKG_MAX_WORKER_JOIN_OPEN_FILES",
            "NGKG_MAX_WORKER_JOIN_BUILD_ROWS",
            "NGKG_MAX_WORKER_JOIN_PROBE_ROWS",
            "NGKG_MAX_WORKER_JOIN_ROW_BYTES",
            "NGKG_IN_MEMORY_JOIN_BUILD_ROWS",
            "worker-join-spill",
            "sparql-fragment-processing",
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
        ),
    )
    if template.count("NGKG_WORKER_JOIN_SPILL_ROOT") != 1:
        raise RuntimeError("worker Grace spill root must be mounted only by fragment workers")
    require(
        "scripts/validate_helm_values.py",
        (
            "maxWorkerJoinSpillBytes cannot exceed workerJoinSpillSizeLimit",
            "maxWorkerJoinOpenFiles must allow two writers per worker join bucket",
            "inMemoryJoinBuildRows cannot exceed maxWorkerJoinBuildRows",
            "fragment ephemeral-storage request must cover cacheSizeLimit plus shuffleCacheSizeLimit plus workerJoinSpillSizeLimit",
        ),
    )

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    version = tuple(int(part) for part in str(openapi["info"]["version"]).split("."))
    if version != (1, 0, 0) and version < (1, 4, 0):
        raise RuntimeError("online OpenAPI was not advanced for worker join evidence")
    execution = openapi["components"]["schemas"]["Execution"]
    required = set(execution["required"])
    if not {
        "workerJoinMode",
        "workerJoinSpillBytes",
        "workerJoinGracePartitions",
        "workerJoinMaxBuildRows",
    }.issubset(required):
        raise RuntimeError("public execution response omits worker join evidence")
    shuffle_headers = openapi["paths"][
        "/v1/datasets/{datasetId}/shuffles/{querySha256}/{stage}/{partition}/join"
    ]["post"]["responses"]["200"]["headers"]
    if len([name for name in shuffle_headers if name.startswith("x-ngkg-worker-join-")]) != 4:
        raise RuntimeError("internal worker response omits required join evidence headers")

    require(
        "scripts/qualify_phase30.sh",
        (
            "grace_hash_nvme_v1",
            "workerJoinMaxBuildRows",
            "ngkg_worker_join_active_spill_bytes",
            "cmp",
            "averageUtilization <= 80",
        ),
    )
    require(
        "docs/phases/PHASE_30.md",
        (
            "Acceptance criteria",
            "Intentional boundary",
            "OWL/SPARQL",
            "Parquet",
            "mmap",
            "OpenMP",
            "BLAS",
            "local-NVMe",
        ),
    )
    require("verification/phase-30.json")
    print("Phase 30 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 30 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
