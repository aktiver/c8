#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 33 fragment ingress."""

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
    executor = require("crates/ngkg-query-executor/src/lib.rs", (
        "pub struct FragmentBindingStream", "pub fn try_new(input: R, max_rows: usize)",
        "fn next_binding", "impl<R: Read> Iterator for FragmentBindingStream<R>",
        "incremental_fragment_decoder_exposes_metadata_and_enforces_rows",
    ))
    stream = executor.index("pub struct FragmentBindingStream")
    schema = executor.index("validate_schema_metadata_keys", stream)
    rows = executor.index("decoded_rows", schema)
    iterator = executor.index("impl<R: Read> Iterator for FragmentBindingStream<R>", rows)
    if not stream < schema < rows < iterator:
        raise RuntimeError("incremental fragment decoder bypasses schema or row validation")

    serving = require("services/online-serving/src/main.rs", (
        "struct FragmentResponseSpool", "struct FragmentResponseLease",
        "prepare_fragment_response_spool_root", "fragment Arrow response is truncated",
        "fragment response spool checksum changed", "FragmentResponseSpool::open",
        "response_spool.receive(response", "streamed_nvme_spool_v1",
        "ngkg_fragment_response_spool_active_bytes",
        "fragment_response_spool_streams_exact_rows_and_rejects_corruption",
    ))
    distributed = serving.index("async fn execute_distributed_query")
    shuffle = serving.index("async fn execute_partitioned_shuffle", distributed)
    ingress = serving[distributed:shuffle]
    received = ingress.index("response_spool.receive(response")
    decoded = ingress.index("ValidatedFragmentSpool::validate", received)
    certified = ingress.index("fragment response differs from its offline certificate", decoded)
    if "read_bounded_response(response, max_response_bytes)" in ingress:
        raise RuntimeError("initial fragment response is still accumulated into a complete byte vector")
    if not received < decoded < certified:
        raise RuntimeError("fragment ingress does not spool, incrementally validate and certify in order")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml", (
        "fragmentResponseSpoolSizeLimit", "maxFragmentResponseSpoolBytes",
    )))
    online = values["onlineServing"]
    if int(online["maxDistributedExchangeBytes"]) > int(online["maxFragmentResponseSpoolBytes"]):
        raise RuntimeError("fragment spool cannot hold the admitted distributed exchange")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("RKE2 scaling target exceeds 80 percent")
    require("charts/ngkg-workloads/templates/online-data-plane.yaml", (
        "fragment-response-spool", "NGKG_FRAGMENT_RESPONSE_SPOOL_ROOT",
        "NGKG_MAX_FRAGMENT_RESPONSE_SPOOL_BYTES",
    ))
    require("scripts/validate_helm_values.py", (
        "maxDistributedExchangeBytes cannot exceed maxFragmentResponseSpoolBytes",
        "fragmentResponseSpoolSizeLimit",
    ))

    openapi = yaml.safe_load(require("api/online-openapi.yaml", (
        "streamed_nvme_spool_v1", "fragmentIngressBytes",
    )))
    version = tuple(int(part) for part in str(openapi["info"]["version"]).split("."))
    if version != (1, 0, 0) and version < (1, 7, 0):
        raise RuntimeError("online OpenAPI was not advanced for fragment ingress")
    required = set(openapi["components"]["schemas"]["Execution"]["required"])
    if not {"fragmentIngressMode", "fragmentIngressBytes"}.issubset(required):
        raise RuntimeError("public execution evidence omits fragment ingress")
    require("scripts/qualify_phase33.sh", (
        "streamed_nvme_spool_v1", "fragmentIngressBytes",
        "ngkg_fragment_response_spool_active_bytes", "cmp",
    ))
    require("verification/phase-33.json", ())
    print("Phase 33 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"phase 33 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
