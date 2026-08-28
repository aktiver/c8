#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 29 certified complete-result caching."""

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
    cache = require(
        "crates/ngkg-query-cache/src/lib.rs",
        (
            "struct QueryCacheKey",
            "tenant_id: Uuid",
            "dataset_id: Uuid",
            "snapshot_id: Uuid",
            "manifest_sha256: String",
            "serving_root_sha256: String",
            "query_sha256: String",
            "hydrate: bool",
            "MmapMut::map_anon",
            "make_read_only",
            "Sha256::digest(payload)",
            "fs::hard_link",
            "File::open(&self.root).and_then(|directory| directory.sync_all())",
            "corruption_is_removed_and_never_served",
            "lru_bounds_are_enforced",
        ),
    )
    if "unsafe {" in cache:
        raise RuntimeError("query cache introduces unsafe mmap code")

    serving = require(
        "services/online-serving/src/main.rs",
        (
            "validate_cached_query_response",
            "validate_cached_execution",
            "canonical_sparql_multiset_sha256",
            "verify_qualified_identities",
            "validate_hydrated_rows",
            "query_cache_flight",
            "x-ngkg-query-cache",
            "ngkg_query_cache_events_total",
            "ngkg_query_cache_entries",
            "NGKG_QUERY_RESULT_CACHE_ROOT",
            "query_cache_revalidates_form_aware_result_and_guid",
            "canonical_query_payload_sha256",
            "expected_result_sha256",
        ),
    )
    auth = serving.index(".semantic_state(identity.tenant_id, dataset_id)", serving.index("async fn query"))
    certificate = serving.index(".certified_queries", auth)
    lookup = serving.index("cache_for_read.get", certificate)
    validate = serving.index("validate_cached_query_response", lookup)
    return_hit = serving.index("return Ok(query_json_response(bytes, true))", validate)
    if not auth < certificate < lookup < validate < return_hit:
        raise RuntimeError("cache lookup or validation bypasses semantic authorization/certification")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    online = values["onlineServing"]
    for key in (
        "queryResultCacheSizeLimit",
        "maxQueryResultCacheBytes",
        "maxQueryResultCacheEntries",
        "maxQueryResultCacheEntryBytes",
    ):
        if key not in online:
            raise RuntimeError(f"Helm values omit {key}")
    template = require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        (
            "NGKG_QUERY_RESULT_CACHE_ROOT",
            "NGKG_MAX_QUERY_RESULT_CACHE_BYTES",
            "NGKG_MAX_QUERY_RESULT_CACHE_ENTRIES",
            "NGKG_MAX_QUERY_RESULT_CACHE_ENTRY_BYTES",
            "query-result-cache",
        ),
    )
    if template.count("NGKG_QUERY_RESULT_CACHE_ROOT") != 1:
        raise RuntimeError("complete-result cache must be mounted only by the query role")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("resource autoscaling exceeds the 80-percent boundary")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    version = tuple(int(part) for part in str(openapi["info"]["version"]).split("."))
    if version != (1, 0, 0) and version < (1, 3, 0):
        raise RuntimeError("online OpenAPI version does not preserve result-cache observability")
    query_response = openapi["paths"]["/v1/datasets/{datasetId}/query"]["post"]["responses"]["200"]
    if "x-ngkg-query-cache" not in query_response.get("headers", {}):
        raise RuntimeError("query response omits its cache outcome header")

    require(
        "scripts/qualify_phase29.sh",
        (
            "freshly started query pod",
            "x-ngkg-query-cache",
            "cmp",
            "ngkg_query_cache_events_total",
            "averageUtilization <= 80",
        ),
    )
    require(
        "docs/phases/PHASE_29.md",
        (
            "Acceptance criteria",
            "Intentional boundary",
            "OWL/SPARQL",
            "Parquet",
            "mmap",
            "OpenMP",
            "local-NVMe",
        ),
    )
    require("verification/phase-29.json")
    print("Phase 29 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 29 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
