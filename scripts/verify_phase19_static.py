#!/usr/bin/env python3
"""Fail-closed static inspection for the Phase 19 serving-root admission path."""

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
    migration = require(
        "migrations/0005_distributed_serving_root.sql",
        (
            "CREATE TABLE distributed_serving_root",
            "CREATE TABLE distributed_serving_certification",
            "serving_root_sha256 BYTEA NOT NULL",
            "binary_locator_sha256 BYTEA NOT NULL",
            "FORCE ROW LEVEL SECURITY",
            "distributed_serving_root_immutable",
            "distributed_serving_certification_immutable",
        ),
    )
    if "DROP TABLE" in migration:
        raise RuntimeError("Phase 19 migration contains a destructive table drop")

    catalog = require(
        "crates/ngkg-catalog/src/lib.rs",
        (
            "catalog migrations through version 9 are required",
            "commit_serving_root",
            "commit_serving_certification",
            "get_serving_certification",
            "serving_root_exists",
            "serving_root.binary_locator_sha256 != certification.binary_locator_sha256",
        ),
    )
    if catalog.index("load_serving_certification", catalog.index("serving_root_exists")) > catalog.index(
        "if matches!(operation.state", catalog.index("serving_root_exists")
    ):
        raise RuntimeError("reference certification can bypass serving evidence on retries")

    worker = require(
        "services/distributed-worker/src/object_stage.rs",
        (
            "prepare_serving_root_object_store",
            "compile_sharded_locator",
            "ServingRootManifest",
            "commit_serving_root",
        ),
    )
    if ".list(" in worker or "list_with_delimiter" in worker:
        raise RuntimeError("serving-root stage contains forbidden object-store listing")
    binary_put = worker.index("&binary_locator_object_key", worker.index("write_new(&serving_path"))
    root_put = worker.index("&serving_root_object_key", binary_put)
    if binary_put >= root_put:
        raise RuntimeError("serving root may be published before its binary locator")

    reference = require(
        "services/reference-worker/src/object_compile.rs",
        (
            "distributed source, artifact and serving roots must be supplied together",
            "certify_sharded_hydration",
            "qualified_entity_iris",
            "hydrate_sharded_payload",
            "canonical_reference_rows",
            "canonical_sharded_rows",
            "commit_serving_certification",
        ),
    )
    serving_commit = reference.index(
        "commit_serving_certification", reference.index("if let Some(certification) = serving_certification")
    )
    reference_commit = reference.index("commit_reference_certification", serving_commit)
    if serving_commit > reference_commit:
        raise RuntimeError("reference certification is committed before serving equivalence")

    operator = require(
        "services/distributed-operator/src/main.rs",
        (
            "Stage::ServingRoot",
            "prepare-serving-root-object-store",
            "distributed-serving-root-object-key",
            "hydration-worker-threads",
            '"OMP_NUM_THREADS"',
            '"OPENBLAS_NUM_THREADS"',
        ),
    )
    barrier = operator.index("reconcile_artifact_barrier")
    if operator.index("get_serving_root", barrier) > operator.index("reasoner_args(", barrier):
        raise RuntimeError("operator may construct reasoner arguments before loading the serving root")

    api = require(
        "api/openapi.yaml",
        ("distributedServingRoot", "distributedServingCertification", "ServingCertification:"),
    )
    if "distributed_serving_certification" not in require("services/api/src/main.rs"):
        raise RuntimeError("REST job response omits the serving certificate")

    for path in (
        "contracts/serving-root.schema.json",
        "contracts/serving-equivalence-report.schema.json",
    ):
        schema = json.loads(require(path))
        if schema.get("additionalProperties") is not False:
            raise RuntimeError(f"{path} must reject unknown properties")

    values = yaml.safe_load(require("charts/ngkg-platform/values.yaml"))
    stages = values["distributedOperator"]["stages"]
    serving = stages.get("serving_root")
    if not serving or serving["cpu"] != "1" or serving["maxParallelism"] != 1:
        raise RuntimeError("serving-root global barrier must truthfully request one CPU and one pod")
    if int(values["operator"]["reference"]["hydrationWorkerThreads"]) < 1:
        raise RuntimeError("hydration worker thread count must be positive")

    require("docs/phases/PHASE_19.md", ("Acceptance gate", "Intentional boundary", "80%"))
    require("scripts/qualify_phase19.sh", ("serving-root", "distributedServingCertification"))
    require("verification/phase-19.json")
    print("Phase 19 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(f"phase 19 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
