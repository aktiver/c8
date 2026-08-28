#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 24 partitioned hash shuffle."""

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
    codec = require(
        "crates/ngkg-query-executor/src/lib.rs",
        (
            "pub struct ShuffleJoinMetadata",
            "pub struct ShuffleJoinInput",
            "pub fn write_shuffle_join_stream",
            "pub fn read_shuffle_join_stream",
            "pub fn partition_sparql_json",
            "pub fn shuffle_partition_for_binding",
            "ngkg-shuffle-key-v1",
            "validate_shuffle_metadata_keys",
            "UnboundShuffleKey",
            "shuffle_stream_round_trip_preserves_partitioned_bags",
        ),
    )
    decoder = codec.index("impl<R: Read> ShuffleJoinStream<R>")
    ownership = codec.index("shuffle row was sent to the wrong partition", decoder)
    yielded = codec.index("return Ok(Some((side, binding)))", ownership)
    if ownership > yielded:
        raise RuntimeError("decoded shuffle row can escape before partition validation")

    serving = require(
        "services/online-serving/src/main.rs",
        (
            "async fn execute_partitioned_shuffle",
            "async fn execute_shuffle_partition",
            "shuffle_plan_is_eligible",
            "shuffle_partition_for_binding",
            "reserve_exchange_bytes",
            "NGKG_SHUFFLE_PARTITIONS",
            "NGKG_MAX_SHUFFLE_REQUEST_BYTES",
            "NGKG_MAX_SHUFFLE_RESPONSE_BYTES",
            "NGKG_MAX_SHUFFLE_EXCHANGE_BYTES",
            "NGKG_SHUFFLE_EXCHANGE_CONCURRENCY",
            '"certified_partitioned_shuffle"',
            '"distributed final multiset differs from offline certification"',
        ),
    )
    dispatch = serving.index("execute_partitioned_shuffle(")
    final_hash = serving.index("canonical_sparql_multiset_sha256", dispatch)
    response = serving.index("ExecutionResponse", final_hash)
    if not dispatch < final_hash < response:
        raise RuntimeError("shuffle output can become visible before final offline validation")
    worker = serving.index("async fn execute_shuffle_partition")
    request_decode = serving.index("inspect_shuffle_spool", worker)
    direct_join = serving.find("inner_join_sparql_json", worker)
    delegated_join = serving.find(".join(identity", worker)
    streamed_join = serving.find(".join_stream(identity", worker)
    join_candidates = [index for index in (direct_join, delegated_join, streamed_join) if index >= 0]
    if not join_candidates:
        raise RuntimeError("shuffle worker has no exact local join implementation")
    join = min(join_candidates)
    output_partition_check = serving.index("validate_shuffle_partition_rows", join)
    if not request_decode < join < output_partition_check:
        raise RuntimeError("shuffle worker bypasses request decode or output partition validation")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    shuffle_path = "/v1/datasets/{datasetId}/shuffles/{querySha256}/{stage}/{partition}/join"
    shuffle = openapi["paths"][shuffle_path]["post"]
    if set(shuffle["requestBody"]["content"]) != {"application/vnd.apache.arrow.stream"}:
        raise RuntimeError("OpenAPI shuffle request is not Arrow-only")
    if set(shuffle["responses"]["200"]["content"]) != {"application/vnd.apache.arrow.stream"}:
        raise RuntimeError("OpenAPI shuffle response is not Arrow-only")
    execution = openapi["components"]["schemas"]["Execution"]
    required = set(execution["required"])
    if not {"shufflePartitionCount", "shuffleWorkerCount"}.issubset(required):
        raise RuntimeError("OpenAPI omits required shuffle execution evidence")
    if "certified_partitioned_shuffle" not in execution["properties"]["mode"]["enum"]:
        raise RuntimeError("OpenAPI omits the implemented shuffle mode")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    online = values["onlineServing"]
    if int(online["shufflePartitions"]) < 2:
        raise RuntimeError("Helm configures fewer than two shuffle partitions")
    if int(online["shuffleExchangeConcurrency"]) > int(online["shufflePartitions"]):
        raise RuntimeError("shuffle concurrency exceeds logical partitions")
    if int(online["maxShuffleRequestBytes"]) > int(online["maxShuffleExchangeBytes"]):
        raise RuntimeError("one shuffle request can exceed the total exchange bound")
    if int(online["maxShuffleResponseBytes"]) > int(online["maxShuffleExchangeBytes"]):
        raise RuntimeError("one shuffle response can exceed the total exchange bound")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("shuffle worker scaling target exceeds 80 percent")

    require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        (
            "NGKG_SHUFFLE_PARTITIONS",
            "NGKG_MAX_SHUFFLE_REQUEST_BYTES",
            "NGKG_MAX_SHUFFLE_RESPONSE_BYTES",
            "NGKG_MAX_SHUFFLE_EXCHANGE_BYTES",
            "NGKG_SHUFFLE_EXCHANGE_CONCURRENCY",
            "sparql-fragment-processing",
            "requiredDuringSchedulingIgnoredDuringExecution",
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
        ),
    )
    require("scripts/validate_helm_values.py", ("shufflePartitions must be at least two",))
    require("scripts/qualify_phase24.sh", ("certified_partitioned_shuffle", "shuffleWorkerCount >= 2", "cmp"))
    require("docs/phases/PHASE_24.md", ("Acceptance criteria", "Intentional boundary", "80 percent", "fail closed"))
    require("verification/phase-24.json")
    print("Phase 24 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 24 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
