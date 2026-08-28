#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 25 bounded shuffle spill."""

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
    serving = require(
        "services/online-serving/src/main.rs",
        (
            'const SPILL_MAGIC: &[u8; 8] = b"NGKGSP25"',
            "struct ShuffleSpillStage",
            "struct SpillPartition",
            "fn prepare_shuffle_spill_root",
            "fn spill_rows",
            "fn read_spill_partition",
            "create_new(true)",
            "sync_all()",
            "shuffle_partition_for_binding",
            "shuffle spill checksum or file boundary is invalid",
            "NGKG_SHUFFLE_SPILL_ROOT",
            "NGKG_MAX_SHUFFLE_SPILL_BYTES",
            "NGKG_MAX_SHUFFLE_OPEN_FILES",
            '"bounded_local_nvme_v1"',
            "spill_partitions_round_trip_and_cleanup_exact_rows",
            "spill_partition_rejects_post_write_corruption",
        ),
    )
    require(
        "crates/ngkg-query-executor/src/lib.rs",
        ("HashMap::<Vec<String>", "pub fn inner_join_sparql_json"),
    )
    shuffle_runtime = serving.index("async fn execute_partitioned_shuffle")
    create = serving.index("create_primary_shuffle_spill(", shuffle_runtime)
    request = serving.index("write_shuffle_join_stream_iter", create)
    if create > request:
        raise RuntimeError("shuffle request is encoded before its spill stage is replayed")
    dispatch = serving.index("execute_partitioned_shuffle(", serving.index("async fn execute_distributed_query"))
    final_hash = serving.index("canonical_sparql_multiset_sha256", dispatch)
    response = serving.index("ExecutionResponse", final_hash)
    if not dispatch < final_hash < response:
        raise RuntimeError("spill-backed output can become visible before final certification")
    reader = serving.index("impl SpillPartitionReader")
    header = serving.index("spill_header(identity", reader)
    checksum = serving.index("expected_sha256", header)
    partition = serving.index("shuffle_partition_for_binding", checksum)
    iterator = serving.index("impl Iterator for SpillPartitionReader", partition)
    if not reader < header < checksum < partition < iterator:
        raise RuntimeError("incremental spill replay can bypass identity, checksum or ownership validation")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    execution = openapi["components"]["schemas"]["Execution"]
    if not {"shuffleSpillMode", "shuffleSpillBytes"}.issubset(set(execution["required"])):
        raise RuntimeError("OpenAPI omits mandatory spill evidence")
    if "bounded_local_nvme_v1" not in execution["properties"]["shuffleSpillMode"]["enum"]:
        raise RuntimeError("OpenAPI omits the implemented spill mode")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    online = values["onlineServing"]
    partitions = int(online["shufflePartitions"])
    if int(online["maxShuffleOpenFiles"]) < 2 * partitions:
        raise RuntimeError("Helm open-file bound cannot hold both relation sides")
    query = values["resources"]["query"]
    if query["requests"] != query["limits"] or "ephemeral-storage" not in query["requests"]:
        raise RuntimeError("query pod lacks Guaranteed-QoS ephemeral storage")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("online scaling target exceeds 80 percent")

    require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        (
            "NGKG_SHUFFLE_SPILL_ROOT",
            "NGKG_MAX_SHUFFLE_SPILL_BYTES",
            "NGKG_MAX_SHUFFLE_OPEN_FILES",
            "shuffle-spill",
            "sparql-query-processing",
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
        ),
    )
    require(
        "scripts/validate_helm_values.py",
        (
            "maxShuffleSpillBytes cannot exceed shuffleSpillSizeLimit",
            "query ephemeral-storage request must cover cacheSizeLimit plus shuffleSpillSizeLimit",
        ),
    )
    require("scripts/qualify_phase25.sh", ("bounded_local_nvme_v1", "shuffleSpillBytes > 0", "cmp"))
    require("docs/phases/PHASE_25.md", ("Acceptance criteria", "Intentional boundary", "80 percent", "BLAS", "mmap"))
    require("verification/phase-25.json")
    print("Phase 25 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 25 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
