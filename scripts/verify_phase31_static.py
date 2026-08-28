#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 31 streamed worker shuffle input."""

from __future__ import annotations

import pathlib
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, tokens: tuple[str, ...]) -> str:
    text = (ROOT / path).read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{path} is missing required token: {token}")
    return text


def main() -> int:
    codec = require("crates/ngkg-query-executor/src/lib.rs", (
        "ngkg.shuffle-join.v2", "pub struct ShuffleJoinStream<R: Read>",
        "ngkg.left-row-count", "ngkg.right-row-count",
        "left relation row follows the right relation",
        "shuffle_stream_exposes_counts_and_decodes_incrementally",
    ))
    if codec.index("shuffle row was sent to the wrong partition") > codec.index("return Ok(Some((side, binding)))"):
        raise RuntimeError("streaming decoder yields a row before validating ownership")

    grace = require("crates/ngkg-grace-join/src/lib.rs", (
        "pub enum GraceJoinSide", "pub fn join_stream<I>",
        "stage.write_row", "execute_spilled", "streamed_join_matches_independent_bag",
    ))
    if grace.index("stage.write_row", grace.index("pub fn join_stream<I>")) > grace.index("execute_spilled", grace.index("pub fn join_stream<I>")):
        raise RuntimeError("streaming Grace execution bypasses immediate partitioning")

    serving = require("services/online-serving/src/main.rs", (
        "struct StreamingRequestSpool", "async fn receive(", "ARROW_STREAM_EOS",
        "inspect_shuffle_spool", "compute_streaming_shuffle_result",
        "x-ngkg-worker-input-sha256", "streamed_spool_v1",
        "NGKG_MAX_STREAMING_REQUEST_SPOOL_BYTES",
        "ngkg_streaming_request_spool_active_bytes",
        "streamed_request_spool_verifies_checksum_eos_limits_and_cleanup",
        "coordinator_rejects_worker_input_evidence_that_differs_from_sent_body",
    ))
    handler = serving.index("async fn execute_shuffle_partition")
    spool = serving.index(".receive(body", handler)
    inspect = serving.index("inspect_shuffle_spool", spool)
    join = serving.index("compute_streaming_shuffle_result", inspect)
    if not handler < spool < inspect < join:
        raise RuntimeError("worker does not spool, validate and then stream the request into Grace")
    if "body: Bytes" in serving[handler:serving.index("fn inspect_shuffle_spool", handler)]:
        raise RuntimeError("shuffle handler still extracts the complete body as Bytes")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml", (
        "streamingRequestSpoolSizeLimit", "maxStreamingRequestSpoolBytes",
    )))
    online = values["onlineServing"]
    if int(online["maxShuffleRequestBytes"]) > int(online["maxStreamingRequestSpoolBytes"]):
        raise RuntimeError("one admitted request cannot fit the process request-spool budget")
    require("charts/ngkg-workloads/templates/online-data-plane.yaml", (
        "NGKG_STREAMING_REQUEST_SPOOL_ROOT", "streaming-request-spool",
    ))
    require("scripts/validate_helm_values.py", (
        "maxStreamingRequestSpoolBytes cannot exceed streamingRequestSpoolSizeLimit",
        "plus streamingRequestSpoolSizeLimit",
    ))

    openapi = yaml.safe_load(require("api/online-openapi.yaml", ("streamed_spool_v1",)))
    version = tuple(int(part) for part in str(openapi["info"]["version"]).split("."))
    if version != (1, 0, 0) and version < (1, 5, 0):
        raise RuntimeError("online OpenAPI was not advanced for streamed-input evidence")
    required = set(openapi["components"]["schemas"]["Execution"]["required"])
    if not {"workerInputMode", "workerInputBytes"}.issubset(required):
        raise RuntimeError("public execution evidence omits worker streamed input")
    shuffle_path = "/v1/datasets/{datasetId}/shuffles/{querySha256}/{stage}/{partition}/join"
    headers = openapi["paths"][shuffle_path]["post"]["responses"]["200"]["headers"]
    if not {"x-ngkg-worker-input-mode", "x-ngkg-worker-input-bytes", "x-ngkg-worker-input-sha256"}.issubset(headers):
        raise RuntimeError("internal shuffle contract omits cryptographic input evidence")
    print("Phase 31 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"phase 31 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
