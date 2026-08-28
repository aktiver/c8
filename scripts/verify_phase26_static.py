#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 26 immutable shuffle caching."""

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
        "crates/ngkg-shuffle-cache/src/lib.rs",
        (
            'const CACHE_MAGIC: &[u8; 8] = b"NGKGSC26"',
            "pub struct ShuffleCacheKey",
            "left_input_sha256",
            "right_input_sha256",
            "pub struct ShuffleResultCache",
            "create_new(true)",
            "sync_all()",
            "fs::hard_link",
            "fn validate_header",
            "fn read_verified",
            "fn evict_for_insert",
            "cache_round_trip_and_reopen_preserve_exact_bytes",
            "corruption_is_removed_and_becomes_a_miss",
            "lru_eviction_enforces_entry_and_byte_bounds",
            "every_semantic_identity_field_changes_the_key",
            "invalid_or_oversized_entries_are_never_published",
            "truncation_extension_and_wrong_key_become_misses",
            "abandoned_owned_temp_is_removed_on_reopen",
            "unmanaged_objects_make_the_root_fail_closed",
            "symlinked_cache_entry_makes_the_root_fail_closed",
        ),
    )
    header = cache.index("fn validate_header")
    payload = cache.index("Sha256::digest(&payload)", header)
    hit = cache.index("Ok(payload)", payload)
    if not header < payload < hit:
        raise RuntimeError("cache payload can escape before key and checksum validation")

    serving = require(
        "services/online-serving/src/main.rs",
        (
            "ShuffleResultCache::open",
            "ShuffleCacheKey",
            "canonical_sparql_multiset_sha256",
            "shuffle_cache_flight",
            "validate_cached_shuffle_result",
            "validate_shuffle_partition_rows",
            '"x-ngkg-shuffle-cache"',
            '"snapshot_checksum_local_nvme_v1"',
            "shuffle_cache_hits",
            "cached_shuffle_result_is_revalidated_before_reuse",
        ),
    )
    worker = serving.index("async fn execute_shuffle_partition")
    left_hash = serving.index("left_input_sha256", worker)
    key = serving.index("let cache_key = ShuffleCacheKey", left_hash)
    lookup = serving.index("cache_for_read.get", key)
    logical_validation = serving.index("validate_cached_shuffle_result", lookup)
    response = serving.index("arrow_binding_response", logical_validation)
    if not worker < left_hash < key < lookup < logical_validation < response:
        raise RuntimeError("shuffle cache bypasses immutable input identity or logical validation")
    coordinator = serving.index("async fn execute_partitioned_shuffle")
    serving.index('get("x-ngkg-shuffle-cache")', coordinator)
    distributed_execution = serving.index("async fn execute_distributed_query")
    dispatch = serving.index("execute_partitioned_shuffle(", distributed_execution)
    final_certificate = serving.index(
        "distributed final multiset differs from offline certification",
        dispatch,
    )
    if dispatch > final_certificate:
        raise RuntimeError("cache evidence is not followed by final offline certification")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    execution = openapi["components"]["schemas"]["Execution"]
    if not {"shuffleCacheMode", "shuffleCacheHits"}.issubset(set(execution["required"])):
        raise RuntimeError("OpenAPI omits mandatory shuffle-cache evidence")
    if "snapshot_checksum_local_nvme_v1" not in execution["properties"]["shuffleCacheMode"]["enum"]:
        raise RuntimeError("OpenAPI omits the implemented shuffle-cache mode")
    shuffle_path = "/v1/datasets/{datasetId}/shuffles/{querySha256}/{stage}/{partition}/join"
    headers = openapi["paths"][shuffle_path]["post"]["responses"]["200"]["headers"]
    if "x-ngkg-shuffle-cache" not in headers:
        raise RuntimeError("shuffle response omits cache hit/miss evidence")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    fragment = values["resources"]["fragment"]
    if fragment["requests"] != fragment["limits"] or "ephemeral-storage" not in fragment["requests"]:
        raise RuntimeError("fragment pod lacks Guaranteed-QoS ephemeral storage")
    if int(values["onlineServing"]["maxShuffleCacheEntries"]) < 1:
        raise RuntimeError("shuffle cache has no entry capacity")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("fragment scaling target exceeds 80 percent")

    require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        (
            "NGKG_SHUFFLE_CACHE_ROOT",
            "NGKG_MAX_SHUFFLE_CACHE_BYTES",
            "NGKG_MAX_SHUFFLE_CACHE_ENTRIES",
            "NGKG_MAX_SHUFFLE_CACHE_ENTRY_BYTES",
            "shuffle-cache",
            "sparql-fragment-processing",
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
        ),
    )
    require(
        "scripts/validate_helm_values.py",
        (
            "maxShuffleCacheBytes cannot exceed shuffleCacheSizeLimit",
            "fragment ephemeral-storage request must cover cacheSizeLimit plus shuffleCacheSizeLimit",
        ),
    )
    require(
        "scripts/qualify_phase26.sh",
        ("snapshot_checksum_local_nvme_v1", "second_hits < 1", "cmp"),
    )
    require(
        "docs/phases/PHASE_26.md",
        ("Acceptance criteria", "Intentional boundary", "80 percent", "BLAS", "mmap", "Parquet"),
    )
    require("verification/phase-26.json")
    print("Phase 26 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 26 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
