#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 32 coordinator streaming."""

from __future__ import annotations

import pathlib
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, tokens: tuple[str, ...]) -> str:
    target = ROOT / path
    if not target.is_file():
        raise RuntimeError(f"missing required file: {path}")
    text = target.read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{path} is missing required token: {token}")
    return text


def main() -> int:
    codec = require("crates/ngkg-query-executor/src/lib.rs", (
        "pub fn write_shuffle_join_stream_iter", "fn write_shuffle_relation",
        "fn write_shuffle_batch", "shuffle relation count differs from its declaration",
        "incremental_shuffle_writer_matches_owned_writer_and_fails_on_source_error",
    ))
    iterator = codec.index("pub fn write_shuffle_join_stream_iter")
    owner = codec.index("shuffle_partition_for_binding", iterator)
    batch = codec.index("write_shuffle_batch", owner)
    if not iterator < owner < batch:
        raise RuntimeError("incremental Arrow writer bypasses owner validation")

    serving = require("services/online-serving/src/main.rs", (
        "struct SpillPartitionReader", "impl Iterator for SpillPartitionReader",
        "struct ArrowRequestWriter", "reqwest::Body::wrap_stream",
        "write_shuffle_join_stream_iter", "producer terminated without evidence",
        "coordinator_streamed_requests", "ngkg_coordinator_streamed_shuffle_bytes_total",
        '"streamed_from_spill_v1"',
        "arrow_request_writer_streams_exact_chunks_and_emits_online_evidence",
    ))
    shuffle = serving.index("async fn execute_partitioned_shuffle")
    worker = serving.index("async fn execute_shuffle_partition", shuffle)
    production = serving[shuffle:worker]
    if ".read_pair(" in production or "BoundedBuffer::new(max_request_bytes)" in production:
        raise RuntimeError("production coordinator still materializes a partition or request")
    opened = production.index(".open_pair(")
    encoded = production.index("write_shuffle_join_stream_iter", opened)
    streamed = production.index("reqwest::Body::wrap_stream", encoded)
    evidence = production.index("worker_join_evidence", streamed)
    if not opened < encoded < streamed < evidence:
        raise RuntimeError("coordinator does not open, encode, stream and validate in order")

    openapi = yaml.safe_load(require("api/online-openapi.yaml", ("streamed_from_spill_v1",)))
    version = tuple(int(part) for part in str(openapi["info"]["version"]).split("."))
    if version != (1, 0, 0) and version < (1, 6, 0):
        raise RuntimeError("online OpenAPI was not advanced for coordinator streaming")
    required = set(openapi["components"]["schemas"]["Execution"]["required"])
    if not {"coordinatorRequestMode", "coordinatorRequestBytes"}.issubset(required):
        raise RuntimeError("public execution evidence omits coordinator request streaming")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml", (
        "fragmentArrowBatchRows", "fragmentArrowHttpChunkBytes",
        "fragmentArrowChannelCapacity",
    )))
    online = values["onlineServing"]
    buffered = int(online["fragmentArrowHttpChunkBytes"]) * int(online["fragmentArrowChannelCapacity"])
    if buffered > int(online["maxShuffleRequestBytes"]):
        raise RuntimeError("coordinator channel can buffer more than one request ceiling")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("RKE2 scaling target exceeds 80 percent")
    require("scripts/validate_helm_values.py", (
        "Arrow HTTP chunk bytes multiplied by channel capacity cannot exceed maxShuffleRequestBytes",
    ))
    require("scripts/qualify_phase32.sh", (
        "streamed_from_spill_v1", "coordinatorRequestBytes", "cmp",
    ))
    require("verification/phase-32.json", ())
    print("Phase 32 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"phase 32 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
