#!/usr/bin/env python3
"""Static contracts for the Phase 40.13.9 distributed property-path foundation."""

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
        "DistributedPropertyPathPlan",
        "DistributedPathAutomaton",
        "PathTransitionKind",
        "NegatedPropertySet",
        "require_complete_partition_set",
        "require_scalar_equivalence",
        "max_checkpoint_bytes",
        "max_spill_bytes",
        "hot_vertex_degree",
    )
    compiler = require(
        "crates/ngkg-sparql-compiler/src/lib.rs",
        "distributed_property_path_plans",
        "PropertyPathExpression::Sequence",
        "PropertyPathExpression::Alternative",
        "PropertyPathExpression::Reverse",
        "PropertyPathExpression::ZeroOrMore",
        "PropertyPathExpression::OneOrMore",
        "PropertyPathExpression::ZeroOrOne",
        "PropertyPathExpression::NegatedPropertySet",
        "epsilon",
    )
    executor = require(
        "crates/ngkg-query-executor/src/distributed_path.rs",
        "origin_entity_id",
        "seed_path_frontier",
        "path_partition_owner",
        "path_expansion_work_items",
        "split_index",
        "split_count",
        "complete_path_iteration",
        "actual != expected",
        "next_frontier.is_empty()",
        "build_path_checkpoint",
        "validate_path_checkpoint",
        "state_sha256",
        "PropertyPathCheckpointLimit",
    )
    serving = require(
        "services/online-serving/src/main.rs",
        "DistributedPropertyPathLimits",
        "NGKG_PROPERTY_PATH_MAX_ITERATIONS",
        "NGKG_PROPERTY_PATH_MAX_FRONTIER_ITEMS",
        "NGKG_PROPERTY_PATH_MAX_VISITED_ITEMS",
        "NGKG_PROPERTY_PATH_MAX_CHECKPOINT_BYTES",
        "NGKG_PROPERTY_PATH_MAX_SPILL_BYTES",
        "NGKG_PROPERTY_PATH_HOT_VERTEX_DEGREE",
        "NGKG_PROPERTY_PATH_MAX_HOT_VERTEX_SPLITS",
        "distributed_property_path_plan_sha256",
        "distributed_property_path_automaton_sha256s",
    )
    online_data_plane = require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        "NGKG_PROPERTY_PATH_MAX_ITERATIONS",
        "NGKG_PROPERTY_PATH_MAX_CHECKPOINT_BYTES",
        "NGKG_PROPERTY_PATH_HOT_VERTEX_DEGREE",
    )
    if online_data_plane.count("NGKG_PROPERTY_PATH_MAX_ITERATIONS") != 4:
        raise RuntimeError("all four online-serving roles must receive property-path ceilings")
    autoscaling = require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        "ngkg_property_path_pending_work_items",
        "ngkg_property_path_active_frontier_items",
        "ngkg_property_path_checkpoint_bytes",
    )
    values = yaml.safe_load((ROOT / "charts/ngkg-workloads/values.yaml").read_text())
    online = values["onlineServing"]
    if int(online["propertyPathMaxVisitedItems"]) < int(online["propertyPathMaxFrontierItems"]):
        raise RuntimeError("visited ceiling is smaller than the frontier ceiling")
    if int(online["propertyPathMaxCheckpointBytes"]) > int(online["propertyPathMaxSpillBytes"]):
        raise RuntimeError("checkpoint ceiling exceeds path spill")
    if int(online["propertyPathMaxSpillBytes"]) > int(online["maxShuffleSpillBytes"]):
        raise RuntimeError("path spill exceeds the shared NVMe spill budget")
    schema = json.loads((ROOT / "charts/ngkg-workloads/values.schema.json").read_text())
    online_required = schema["properties"]["onlineServing"]["required"]
    metrics_required = schema["properties"]["metrics"]["required"]
    for field in [
        "propertyPathMaxIterations",
        "propertyPathMaxFrontierItems",
        "propertyPathMaxVisitedItems",
        "propertyPathMaxCheckpointBytes",
        "propertyPathMaxSpillBytes",
        "propertyPathHotVertexDegree",
        "propertyPathMaxHotVertexSplits",
    ]:
        if field not in online_required:
            raise RuntimeError(f"Helm schema does not require {field}")
    for field in [
        "propertyPathPendingWorkItemsAverageTarget",
        "propertyPathActiveFrontierItemsAverageTarget",
        "propertyPathCheckpointBytesAverageTarget",
    ]:
        if field not in metrics_required:
            raise RuntimeError(f"Helm metrics schema does not require {field}")
    openapi = require(
        "api/online-openapi.yaml",
        "distributedPropertyPathPlanSha256",
        "distributedPropertyPathAutomatonSha256s",
        "distributedPropertyPathScalarEquivalenceRequired",
    )
    require(
        "acceptance/phase-gates.yaml",
        "phase: '40.13.9'",
        "scripts/qualify_phase40_13_9.sh",
    )
    if "ngkg-mapping" in serving or "align_ontology" in executor:
        raise RuntimeError("ontology alignment or raw-data mapping entered the query path")
    if not all((planner, compiler, executor, serving, autoscaling, openapi)):
        raise RuntimeError("empty Phase 40.13.9 source")
    print("phase 40.13.9 static qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.9 static qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
