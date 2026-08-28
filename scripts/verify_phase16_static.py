#!/usr/bin/env python3
"""Fail-closed static inspection for the Phase 16 distributed artifact kernel."""

from __future__ import annotations

import json
import pathlib
import sys
import tomllib


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
    cargo = tomllib.loads(require("Cargo.toml"))
    members = set(cargo["workspace"]["members"])
    if "crates/ngkg-distributed-artifacts" not in members:
        raise RuntimeError("distributed artifact crate is absent from the workspace")

    library = require(
        "crates/ngkg-distributed-artifacts/src/lib.rs",
        [
            "materialize_artifact_partition",
            "finalize_artifact_partitions",
            "merge_sorted_unique",
            "create_new(true)",
            "artifact partition barrier is incomplete",
            "duplicate physical locator row across partitions",
            "global dictionary is missing",
            "semantic_content_sha256",
        ],
    )
    if "std::fs::read_to_string" in library or "bucket.list" in library:
        raise RuntimeError("artifact kernel contains a forbidden broad-discovery path")

    worker = require(
        "services/distributed-worker/src/main.rs",
        [
            '"materialize-artifact-partition"',
            '"finalize-artifact-partitions"',
            '"compare-artifact-roots"',
            '"partition-index"',
            '"row-group-rows"',
        ],
    )
    if "positive_usize(options, \"row-group-rows\")" not in worker:
        raise RuntimeError("row-group size is not enforced as a positive operator input")

    for path in [
        "contracts/artifact-partition-manifest.schema.json",
        "contracts/distributed-artifact-root.schema.json",
    ]:
        value = json.loads(require(path))
        if value.get("additionalProperties") is not False:
            raise RuntimeError(f"{path} must reject unknown top-level properties")

    require(
        "scripts/run_distributed_artifact_slice.sh",
        [
            "Cargo.lock is required",
            "materialize-artifact-partition",
            "compare-artifact-roots",
            "forward_order",
            "reverse_order",
        ],
    )
    matrix = json.loads(require("test-corpus/distributed/artifact-equivalence-v1.json"))
    if matrix.get("logicalPartitionCount") != 8 or len(matrix.get("executions", [])) < 2:
        raise RuntimeError("artifact equivalence matrix does not exercise two eight-partition orders")
    require("docs/phases/PHASE_16.md", ["Intentional boundary", "Acceptance gate"])
    require("verification/phase-16.json")
    print("Phase 16 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(f"phase 16 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
