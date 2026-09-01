#!/usr/bin/env python3
"""Fail-closed source/contract gate for Enterprise Stabilization Phase 5."""
from __future__ import annotations

import json
import pathlib
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, *needles: str) -> str:
    value = (ROOT / path).read_text(encoding="utf-8")
    if not value:
        raise RuntimeError(f"empty required file: {path}")
    for needle in needles:
        if needle not in value:
            raise RuntimeError(f"{path}: missing {needle!r}")
    return value


def main() -> int:
    cargo = require(
        "crates/ngkg-native-runtime/Cargo.toml",
        'name = "ngkg-native-runtime"',
        "ngkg-query-planner",
        "parquet.workspace = true",
    )
    if "oxigraph" in cargo or "ngkg-reference" in cargo:
        raise RuntimeError("native runtime depends on a forbidden scalar/reference evaluator")
    require(
        "crates/ngkg-native-runtime/src/lib.rs",
        "pub fn admit_native_plan",
        "ScalarOracleForbidden",
        "pub fn scan_verified_parquet_leaf",
        "allowed_graph_ids",
        "pub struct StageBarrier",
        "ConflictingCompletion",
        "require_complete_partition_set",
    )
    serving = require(
        "services/online-serving/src/main.rs",
        "require_native_cutover_admission",
        "NATIVE_CUTOVER_UNAVAILABLE",
        "execute_native_leaf_scan",
        "scan_verified_parquet_leaf",
        "lookup_dictionary_ids_available",
        "finalize_native_exact_select",
        "native_distributed_exact_bgp_algebra_v1",
        "native Parquet scan did not cover the complete certified partition",
    )
    admission = serving.index("require_native_cutover_admission(&state, &compiled_query")
    local_runtime = serving.index(".routed_runtime(Arc::clone(&semantic)")
    if admission > local_runtime:
        raise RuntimeError("native admission is not ordered before local runtime materialization")
    openapi = yaml.safe_load(require(
        "api/online-openapi.yaml",
        "/v1/datasets/{datasetId}/native/leaves/{querySha256}/{partition}/scan:",
        "NativeLeafScanRequest",
        "NativeLeafScanResponse",
    ))
    route = "/v1/datasets/{datasetId}/native/leaves/{querySha256}/{partition}/scan"
    if route not in openapi["paths"]:
        raise RuntimeError("native leaf REST route is absent from OpenAPI")
    schema = json.loads(require(
        "charts/ngkg-workloads/values.schema.json",
        '"nativeCutoverMode"',
    ))
    mode = schema["properties"]["onlineServing"]["properties"]["nativeCutoverMode"]
    if mode.get("enum") != ["disabled", "shadow", "required"]:
        raise RuntimeError("native cutover Helm enum drift")
    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml", "nativeCutoverMode: shadow"))
    enterprise = yaml.safe_load(require(
        "charts/ngkg-workloads/profiles/enterprise-secure.yaml",
        "nativeCutoverMode: required",
    ))
    if values["onlineServing"]["nativeCutoverMode"] != "shadow":
        raise RuntimeError("unqualified default must remain shadow")
    if enterprise["onlineServing"]["nativeCutoverMode"] != "required":
        raise RuntimeError("enterprise profile must fail closed")
    template = require("charts/ngkg-workloads/templates/online-data-plane.yaml", "NGKG_NATIVE_CUTOVER_MODE")
    if template.count("NGKG_NATIVE_CUTOVER_MODE") != 4:
        raise RuntimeError("native cutover mode is not wired to all four online roles")
    prerequisites = (ROOT.parent / "phase5/verify_live_prerequisites.py").read_text(encoding="utf-8")
    for name in ("phase3-certificate.json", "phase4-live-certificate.json", "phase5-live-certificate.json"):
        if name not in prerequisites:
            raise RuntimeError(f"live prerequisite verifier is missing {name}")
    print("Enterprise Stabilization Phase 5 source/contract gate passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        print(f"Phase 5 gate failed: {error}", file=sys.stderr)
        raise SystemExit(1)
