#!/usr/bin/env python3
"""Fail-closed static contract checks for Phase 40.13.17."""

from __future__ import annotations

import json
import pathlib
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(relative: str, *tokens: str) -> str:
    text = (ROOT / relative).read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{relative} is missing {token!r}")
    return text


def main() -> int:
    path_runtime = require(
        "crates/ngkg-query-executor/src/partition_path.rs",
        "PartitionAdjacencyIndex",
        "read_anchor_rows",
        "ADJACENCY_RECORD_BYTES",
        "execute_partition_path_batch",
        "expand_path_work_item_borrowed",
        "write_checkpoint_atomic",
        "max_rows_read",
        "hot_split_count",
        "corrupt_adjacency_checksum_fails_closed",
    )
    distributed = require(
        "crates/ngkg-query-executor/src/distributed_path.rs",
        "storage_partition",
        "graph_id",
        "complete_path_iteration",
        "actual != expected",
        "next_frontier.is_empty()",
        "validate_path_checkpoint",
    )
    serving = require(
        "services/online-serving/src/main.rs",
        "execute_partition_native_path_set",
        "execute_partition_path",
        "semantic_partition_files",
        "PartitionPathAction::Seed",
        "PartitionPathAction::Expand",
        "property-path partition barrier is incomplete or duplicated",
        "property-path checkpoint spill exceeded its admitted byte ceiling",
        "property_path_core_lanes",
        "acquire_many_owned",
        "ngkg_property_path_pending_work_items",
        "ngkg_property_path_active_frontier_items",
        "ngkg_property_path_checkpoint_bytes",
        "partition_native_distributed_frontier_v1",
    )
    compiler = require(
        "crates/ngkg-semantic-compiler/src/lib.rs",
        "adjacency-forward.tsv",
        "adjacency-reverse.tsv",
        "output.edge_count = checked_add",
        "Term::Literal",
    )
    schema = json.loads(
        (ROOT / "contracts/distributed-property-path-execution.schema.json").read_text(
            encoding="utf-8"
        )
    )
    if schema["properties"]["termination"]["properties"]["densePartitionBarrier"]["const"] is not True:
        raise RuntimeError("property-path schema does not require a dense partition barrier")
    openapi = yaml.safe_load((ROOT / "api/online-openapi.yaml").read_text(encoding="utf-8"))
    route = "/v1/datasets/{datasetId}/paths/{querySha256}/{pathId}/{iteration}/{partition}/expand"
    if route not in openapi["paths"]:
        raise RuntimeError("online OpenAPI omits the partition-native path worker route")
    values = yaml.safe_load((ROOT / "charts/ngkg-workloads/values.yaml").read_text(encoding="utf-8"))
    online = values["onlineServing"]
    if not online["partitionNativePathsEnabled"]:
        raise RuntimeError("partition-native path execution is disabled")
    if int(online["propertyPathWorkerThreads"]) > int(online["fragmentExchangeConcurrency"]):
        raise RuntimeError("property-path core lanes exceed fragment exchange concurrency")
    require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        "NGKG_PARTITION_NATIVE_PATHS_ENABLED",
        "NGKG_PROPERTY_PATH_WORKER_THREADS",
        "NGKG_PROPERTY_PATH_MAX_SCAN_ROWS",
        "NGKG_RUST_COMPUTE_THREADS",
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
    )
    require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        "ngkg_property_path_pending_work_items",
        "ngkg_property_path_active_frontier_items",
        "ngkg_property_path_checkpoint_bytes",
        "cpu",
        "memory",
    )
    matrix = json.loads(
        (ROOT / "test-corpus/distributed/phase40.13.17-property-paths.json").read_text(
            encoding="utf-8"
        )
    )
    if len(matrix["cases"]) < 4 or not any("GRAPH ?g" in case["query"] for case in matrix["cases"]):
        raise RuntimeError("property-path matrix omits required graph scopes")
    if "align_ontology" in serving or "raw_data_mapping" in serving:
        raise RuntimeError("ontology-alignment or raw-data mapping appeared in the query runtime")
    if not all((path_runtime, distributed, compiler)):
        raise RuntimeError("empty Phase 40.13.17 source")
    print("phase 40.13.17 static qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.17 static qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
