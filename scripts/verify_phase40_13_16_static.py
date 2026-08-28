#!/usr/bin/env python3
"""Fail-closed static contract checks for Phase 40.13.16."""

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
    serving = require(
        "services/online-serving/src/main.rs",
        "DistributedAlgebraExecutionRequest",
        "execute_distributed_algebra_replica",
        "execute_distributed_scalar_oracle",
        "complete_replica_barrier_v1",
        "distributed_scalar_oracle_equivalence_v1",
        "distributed algebra replica set is incomplete or scalar-unequal",
        "distributed algebra replicas did not execute on distinct workers",
        "canonical_query_payload_sha256",
        "resolved_dataset_is_authorized",
        "validate_resolved_dataset",
        "semantic_serving_identity",
        "is_snapshot_cacheable",
        "has_remote_service",
    )
    for query_form in ("QueryForm::Select", "QueryForm::Ask", "QueryForm::Construct", "QueryForm::Describe"):
        require("crates/ngkg-sparql-compiler/src/lib.rs", query_form)
    executor = require(
        "crates/ngkg-query-executor/src/distributed_algebra.rs",
        "left_join_sparql_json",
        "union_sparql_json",
        "minus_sparql_json",
        "distinct_sparql_json",
        "global_slice_sparql_json",
        "complete_algebra_partition_set",
    )
    openapi = yaml.safe_load((ROOT / "api/online-openapi.yaml").read_text(encoding="utf-8"))
    path = "/v1/datasets/{datasetId}/algebra/{querySha256}/{replica}/execute"
    if path not in openapi["paths"]:
        raise RuntimeError("online OpenAPI omits the distributed algebra worker route")
    schema = json.loads(
        (ROOT / "contracts/distributed-algebra-execution.schema.json").read_text(encoding="utf-8")
    )
    if schema["properties"]["replicaCount"]["minimum"] != 2:
        raise RuntimeError("distributed algebra schema permits a single replica")
    values = yaml.safe_load((ROOT / "charts/ngkg-workloads/values.yaml").read_text(encoding="utf-8"))
    online = values["onlineServing"]
    if not online["distributedAlgebraEnabled"] or int(online["distributedAlgebraReplicas"]) < 2:
        raise RuntimeError("production distributed algebra is not enabled with two replicas")
    autoscaling = require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        "algebraPendingAverageTarget",
        "ngkg_worker_join_active_spill_bytes",
        "cpu",
        "memory",
    )
    workload = require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        "NGKG_DISTRIBUTED_ALGEBRA_ENABLED",
        "NGKG_DISTRIBUTED_ALGEBRA_REPLICAS",
        "NGKG_RUST_COMPUTE_THREADS",
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
    )
    live = require(
        "scripts/qualify_phase40_13_16.sh",
        "distributed_scalar_oracle_equivalence_v1",
        "complete_replica_barrier_v1",
        "ngkg_admission_pending",
        "ngkg_worker_join_active_spill_bytes",
    )
    matrix = json.loads(
        (ROOT / "test-corpus/distributed/phase40.13.16-algebra-equivalence.json").read_text(
            encoding="utf-8"
        )
    )
    if {case["form"] for case in matrix["cases"]} != {"SELECT", "ASK", "CONSTRUCT", "DESCRIBE"}:
        raise RuntimeError("live matrix does not cover every SPARQL query form")
    if "align_ontology" in serving or "ngkg-mapping" in serving:
        raise RuntimeError("ontology-alignment code appeared in the database query path")
    if not all((executor, autoscaling, workload, live)):
        raise RuntimeError("empty Phase 40.13.16 source")
    print("phase 40.13.16 static qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.16 static qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
