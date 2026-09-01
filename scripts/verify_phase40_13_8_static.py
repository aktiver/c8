#!/usr/bin/env python3
"""Static contract checks for the Phase 40.13.8 distributed algebra foundation."""

from __future__ import annotations

import json
import pathlib
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(relative: str, *needles: str) -> str:
    text = (ROOT / relative).read_text(encoding="utf-8")
    for needle in needles:
        if needle not in text:
            raise RuntimeError(f"{relative} is missing {needle!r}")
    return text


def main() -> int:
    planner = require(
        "crates/ngkg-query-planner/src/lib.rs",
        "DistributedAlgebraPlan",
        "DistributedAlgebraStage",
        "AlgebraExecutionLane",
        "ScalarOraclePartitioned",
        "ExactReasonerPartitioned",
        "require_complete_partition_set",
        "require_scalar_equivalence",
        "algebra_execution_waves",
        "UnsafeLane",
    )
    compiler = require(
        "crates/ngkg-sparql-compiler/src/lib.rs",
        "distributed_algebra_plan",
        "GraphPattern::LeftJoin",
        "GraphPattern::Union",
        "GraphPattern::Minus",
        "GraphPattern::Group",
        "GraphPattern::OrderBy",
        "DistributedAlgebraOperator::Subquery",
        "AskFinalize",
        "ConstructFinalize",
        "DescribeFinalize",
        "Expression::Exists",
    )
    executor = require(
        "crates/ngkg-query-executor/src/distributed_algebra.rs",
        "left_join_sparql_json",
        "union_sparql_json",
        "minus_sparql_json",
        "distinct_sparql_json",
        "group_owned_partitions",
        "merge_ordered_partitions_by",
        "global_slice_sparql_json",
        "complete_algebra_partition_set",
        "execute_native_algebra_task",
        "UnsafeNativeAlgebraOperator",
        "shares at least one bound",
    )
    serving = require(
        "services/online-serving/src/main.rs",
        "distributed_algebra_plan",
        "algebra_execution_waves",
        "distributed_algebra_plan_sha256",
        "distributed_algebra_work_item_count",
        "distributed_algebra_scalar_equivalence_required",
    )
    openapi = require(
        "api/online-openapi.yaml",
        "ExactEntailmentEvidence",
        "distributedAlgebraPlanSha256",
        "distributedAlgebraWorkItemCount",
        "distributedAlgebraScalarEquivalenceRequired",
    )
    autoscaling = require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        "class: shuffle",
        "algebraPendingAverageTarget",
        "ngkg_worker_join_active_spill_bytes",
        "algebraActiveSpillBytesAverageTarget",
    )
    values = yaml.safe_load((ROOT / "charts/ngkg-workloads/values.yaml").read_text())
    metrics = values["metrics"]
    if not metrics.get("algebraPendingAverageTarget"):
        raise RuntimeError("algebra backlog HPA target is absent")
    if not metrics.get("algebraActiveSpillBytesAverageTarget"):
        raise RuntimeError("algebra spill-pressure HPA target is absent")
    schema = json.loads((ROOT / "charts/ngkg-workloads/values.schema.json").read_text())
    required = schema["properties"]["metrics"]["required"]
    for field in ["algebraPendingAverageTarget", "algebraActiveSpillBytesAverageTarget"]:
        if field not in required:
            raise RuntimeError(f"Helm values schema does not require {field}")
    cargo = require("services/online-serving/Cargo.toml", "ngkg-query-planner")
    require(
        "acceptance/phase-gates.yaml",
        "phase: '40.13.8'",
        "scripts/qualify_phase40_13_8.sh",
    )
    if "ngkg-mapping" in cargo or "align_ontology" in serving:
        raise RuntimeError("ontology-alignment code appeared in the query path")
    if not all((planner, compiler, executor, serving, openapi, autoscaling)):
        raise RuntimeError("empty Phase 40.13.8 source")
    print("phase 40.13.8 static qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.8 static qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
