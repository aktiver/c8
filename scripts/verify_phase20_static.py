#!/usr/bin/env python3
"""Fail-closed static inspection for the Phase 20 online read plane."""

from __future__ import annotations

import json
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
    catalog = require(
        "crates/ngkg-catalog/src/lib.rs",
        (
            "pub struct ActiveServingSnapshot",
            "get_active_serving_snapshot",
            "s.state='PUBLISHED'",
            "reference_manifest_sha256 != snapshot.manifest_sha256",
            "serving_root.serving_root_sha256 != certification.serving_root_sha256",
        ),
    )
    if catalog.index("load_serving_root", catalog.index("get_active_serving_snapshot")) > catalog.index(
        "load_serving_certification", catalog.index("get_active_serving_snapshot")
    ):
        raise RuntimeError("active snapshot resolution loads certification before its serving root")

    runtime = require(
        "crates/ngkg-reference/src/lib.rs",
        (
            "pub struct CertifiedSemanticRuntime",
            "expected_snapshot_manifest_sha256",
            "UncertifiedQuery",
            "reasoner_report_sha256",
            "let store = build_store(",
        ),
    )
    service = require(
        "services/online-serving/src/main.rs",
        (
            "get_active_serving_snapshot",
            "CertifiedSemanticRuntime::open",
            "runtime.execute",
            "guid_for_canonical_iri",
            "MmapLocatorIndex::open",
            "hydrate_sharded_payload",
            "materialize_verified",
            "NGKG_MAX_PAYLOAD_CACHE_BYTES",
            "NGKG_RUST_COMPUTE_THREADS",
            "NGKG_HYDRATION_WORKER_THREADS",
        ),
    )
    if ".list(" in service or "list_with_delimiter" in service:
        raise RuntimeError("online service contains forbidden object-store discovery")
    query_start = service.index("async fn query")
    if service.index("runtime.execute", query_start) > service.index("qualify_entities(&result", query_start):
        raise RuntimeError("query hydration may precede semantic qualification")
    if service.index("verify_qualified_identities", service.index("async fn hydrate")) > service.index(
        "hydrate_sharded_payload", service.index("async fn hydrate")
    ):
        raise RuntimeError("hydration can run before IRI/GUID revalidation")

    auth = require("services/online-serving/src/auth.rs", ("queries:execute", "Sha256::digest"))
    if "tenant_id" not in auth or "principal_id" not in auth:
        raise RuntimeError("online authentication omits tenant or principal identity")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    paths = openapi.get("paths", {})
    for path in (
        "/v1/datasets/{datasetId}/query",
        "/v1/datasets/{datasetId}/locate",
        "/v1/datasets/{datasetId}/hydrate",
    ):
        if path not in paths:
            raise RuntimeError(f"online OpenAPI omits {path}")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    if values["hpcRuntime"]["nodeSaturationTargetPercent"] != 80:
        raise RuntimeError("node saturation policy must remain exactly 80%")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("online HPA resource target exceeds 80%")

    plane = require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        (
            "args: [query]",
            "args: [locator]",
            "args: [hydration]",
            "NGKG_AUTH_TOKENS_FILE",
            "NGKG_ARTIFACT_BASE_URL",
            "startupProbe",
            "readinessProbe",
            "livenessProbe",
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
        ),
    )
    if "averageUtilization: {{ .Values.metrics.cpuUtilizationTargetPercent }}" not in require(
        "charts/ngkg-workloads/templates/autoscaling.yaml"
    ):
        raise RuntimeError("query/hydration HPA is not driven by the bounded CPU target")
    for name in ("query", "locator", "hydration"):
        if f"images.{name}.digest is required" not in plane:
            raise RuntimeError(f"{name} image is not digest pinned")

    schema = json.loads(require("charts/ngkg-workloads/values.schema.json"))
    if schema["properties"]["hpcRuntime"]["properties"]["nodeSaturationTargetPercent"]["maximum"] != 80:
        raise RuntimeError("Helm schema allows node saturation above 80%")

    require("deploy/online-serving/Dockerfile", ("--locked", "ngkg-online-serving", "USER 65532:65532"))
    require("docs/phases/PHASE_20.md", ("Acceptance criteria", "Intentional boundary", "80%"))
    require("scripts/qualify_phase20.sh", ("NGKG_ONLINE_QUERY_URL", "ngkg-query-shard", "averageUtilization"))
    require("verification/phase-20.json")
    print("Phase 20 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 20 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
