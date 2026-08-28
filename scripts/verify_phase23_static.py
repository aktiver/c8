#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 23 Arrow IPC exchange."""

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
    workspace = require("Cargo.toml", ('arrow-ipc = "=58.0.0"',))
    if "arrow-flight" in workspace:
        raise RuntimeError("Phase 23 must not claim or pull in unimplemented Arrow Flight")

    codec = require(
        "crates/ngkg-query-executor/src/lib.rs",
        (
            "pub const ARROW_STREAM_MEDIA_TYPE",
            "pub fn write_fragment_arrow_stream",
            "bindings.chunks(max_batch_rows)",
            "pub fn read_fragment_arrow_stream",
            "validate_arrow_schema",
            "decode_arrow_term",
            "IntermediateRowLimit",
            "arrow_fragment_round_trip_preserves_terms_unbound_values_and_bag_rows",
        ),
    )
    ceiling = codec.index("checked_add(batch.num_rows())")
    retained = codec.index("self.current_batch = Some(batch)", ceiling)
    if ceiling > retained:
        raise RuntimeError("Arrow decoder retains a batch before enforcing its total row ceiling")

    serving = require(
        "services/online-serving/src/main.rs",
        (
            ".header(ACCEPT, ARROW_STREAM_MEDIA_TYPE)",
            "require_arrow_content_type(&response)",
            "response_spool.receive(response, max_response_bytes)",
            "ValidatedFragmentSpool::validate",
            "require_arrow_accept(&headers)",
            "write_fragment_arrow_stream",
            "arrow_ipc_stream_v1",
            "NGKG_FRAGMENT_ARROW_BATCH_ROWS",
            "NGKG_FRAGMENT_ARROW_HTTP_CHUNK_BYTES",
            "NGKG_FRAGMENT_ARROW_CHANNEL_CAPACITY",
            "Body::from_stream(stream)",
            "mpsc::channel(channel_capacity)",
            "canonical_sparql_multiset_sha256",
        ),
    )
    accept = serving.index("require_arrow_content_type(&response)")
    decode = serving.index("ValidatedFragmentSpool::validate", accept)
    final_hash = serving.index("canonical_sparql_multiset_sha256", decode)
    if not accept < decode < final_hash:
        raise RuntimeError("Arrow response can bypass media-type, decode, or final certificate order")
    if "FragmentExecutionResponse" in serving or "serde_json::from_slice::<FragmentExecutionResponse>" in serving:
        raise RuntimeError("legacy JSON fragment exchange remains in the serving path")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    fragment = openapi["paths"]["/v1/datasets/{datasetId}/fragments/{querySha256}/{fragmentId}/execute"]
    content = fragment["post"]["responses"]["200"]["content"]
    if set(content) != {"application/vnd.apache.arrow.stream"}:
        raise RuntimeError("OpenAPI fragment response is not Arrow-only")
    exchange = openapi["components"]["schemas"]["Execution"]["properties"]["exchangeFormat"]["enum"]
    if "arrow_ipc_stream_v1" not in exchange:
        raise RuntimeError("OpenAPI omits Arrow exchange evidence")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    if values["networking"]["internalExchange"] != "certified-arrow-ipc-rest":
        raise RuntimeError("Helm values do not select the implemented Phase 23 transport")
    if int(values["onlineServing"]["fragmentArrowBatchRows"]) > int(values["onlineServing"]["maxDistributedIntermediateRows"]):
        raise RuntimeError("Arrow record batch exceeds the distributed row ceiling")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("online scaling target exceeds 80 percent")

    require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        (
            "NGKG_FRAGMENT_ARROW_BATCH_ROWS",
            "NGKG_FRAGMENT_ARROW_HTTP_CHUNK_BYTES",
            "NGKG_FRAGMENT_ARROW_CHANNEL_CAPACITY",
            "sparql-fragment-processing",
            "requiredDuringSchedulingIgnoredDuringExecution",
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
        ),
    )
    require("scripts/qualify_phase23.sh", ("arrow_ipc_stream_v1", "workerCount >= 2", "cmp"))
    require("docs/phases/PHASE_23.md", ("Acceptance criteria", "Intentional boundary", "Arrow IPC", "80 percent"))
    require("verification/phase-23.json")
    print("Phase 23 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 23 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
