#!/usr/bin/env python3
"""Fail-closed static inspection for the Phase 17 durable artifact path."""

from __future__ import annotations

import json
import pathlib
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, tokens: list[str] | None = None) -> str:
    target = ROOT / path
    if not target.is_file():
        raise RuntimeError(f"missing required file: {path}")
    text = target.read_text(encoding="utf-8")
    for token in tokens or []:
        if token not in text:
            raise RuntimeError(f"{path} is missing required token: {token}")
    return text


def main() -> int:
    migration = require(
        "migrations/0004_distributed_artifacts.sql",
        [
            "ADD VALUE IF NOT EXISTS 'ARTIFACT'",
            "CREATE TABLE distributed_artifact_plan",
            "CREATE TABLE distributed_artifact_root",
            "FORCE ROW LEVEL SECURITY",
            "payload_row_count = locator_record_count",
        ],
    )
    if "DROP TABLE" in migration:
        raise RuntimeError("Phase 17 migration contains a destructive table drop")

    catalog = require(
        "crates/ngkg-catalog/src/lib.rs",
        [
            "register_artifact_plan",
            "commit_artifact_root",
            "DistributedWorkKind::Artifact",
            "succeeded_artifacts",
            "catalog migrations through version 9 are required",
        ],
    )
    if 'Self::Artifact => "ARTIFACT"' not in catalog:
        raise RuntimeError("catalog does not close the ARTIFACT work vocabulary")

    worker = require(
        "services/distributed-worker/src/object_stage.rs",
        [
            "prepare_artifacts",
            "materialize_artifact_object_store",
            "finalize_artifacts_object_store",
            "finalize_catalog_artifact_partitions",
            "artifact plan or work item differs from catalog truth",
            "commit_distributed_work",
            "commit_artifact_root",
        ],
    )
    if ".list(" in worker or "list_with_delimiter" in worker:
        raise RuntimeError("artifact service path contains forbidden object-store listing")
    data_put = worker.index("for artifact in &manifest.artifacts")
    manifest_put = worker.index("let manifest_key", data_put)
    if data_put >= manifest_put:
        raise RuntimeError("partition manifest may be published before its data")
    locator_put = worker.index("&locator_key", worker.index("let locator_key"))
    root_put = worker.index("&root_key", worker.index("let root_key", locator_put))
    if locator_put >= root_put:
        raise RuntimeError("artifact root may be published before its locator")

    operator = require(
        "services/distributed-operator/src/main.rs",
        [
            "Stage::ArtifactPlan",
            "Stage::Artifact",
            "Stage::ArtifactFinalize",
            "reconcile_artifact_barrier",
            "semantic-artifact-build",
            "distributed-artifact-root-object-key",
        ],
    )
    if operator.index("get_artifact_root") > operator.index("reasoner_args("):
        raise RuntimeError("operator does not gate reasoner arguments on the artifact root")

    require(
        "services/reference-worker/src/object_compile.rs",
        [
            "distributed source, artifact and serving roots must be supplied together",
            "get_artifact_root",
            "DistributedWorkKind::Artifact",
            "validate_artifact_equivalence",
            "distributed artifacts are not count-equivalent",
        ],
    )
    api = require("api/openapi.yaml", ["distributedArtifacts:", "distributedArtifactRoot:"])
    if "DistributedArtifactRoot:" not in api:
        raise RuntimeError("OpenAPI omits the artifact-root schema")

    for path in [
        "contracts/distributed-artifact-plan.schema.json",
        "contracts/distributed-artifact-root.schema.json",
    ]:
        schema = json.loads(require(path))
        if schema.get("additionalProperties") is not False:
            raise RuntimeError(f"{path} must reject unknown properties")

    platform = yaml.safe_load(require("charts/ngkg-platform/values.yaml"))
    stages = platform["distributedOperator"]["stages"]
    for name in ["artifact_plan", "artifact", "artifact_finalize"]:
        if name not in stages:
            raise RuntimeError(f"Helm values omit {name}")
    workloads = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    if "semantic_artifact_build_num_of_nodes" not in workloads["hpcNodeGroups"]:
        raise RuntimeError("RKE2 capacity model omits semantic artifact nodes")
    require("docs/phases/PHASE_17.md", ["Acceptance gate", "Intentional boundary"])
    require("scripts/qualify_phase17.sh", ["artifact-plan", "artifact-finalize"])
    require("verification/phase-17.json")
    print("Phase 17 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(f"phase 17 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
