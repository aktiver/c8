#!/usr/bin/env python3
"""Static Phase 15 contract checks; these do not simulate a cluster."""

from __future__ import annotations

import json
import pathlib
import sys
import tomllib

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def require(condition: bool, message: str, errors: list[str]) -> None:
    if not condition:
        errors.append(message)


def main() -> int:
    errors: list[str] = []
    required = [
        "crates/ngkg-distributed-build/src/lib.rs",
        "services/distributed-worker/src/main.rs",
        "services/distributed-worker/src/object_stage.rs",
        "services/distributed-operator/src/main.rs",
        "migrations/0003_distributed_build.sql",
        "contracts/distributed-source-plan.schema.json",
        "contracts/distributed-run-manifest.schema.json",
        "contracts/build-equivalence-matrix.schema.json",
        "test-corpus/distributed/build-equivalence-v1.json",
        "charts/ngkg-platform/templates/distributed-operator.yaml",
        "docs/phases/PHASE_15.md",
    ]
    for relative in required:
        require((ROOT / relative).is_file(), f"missing {relative}", errors)

    workspace = tomllib.loads(read("Cargo.toml"))
    members = set(workspace["workspace"]["members"])
    for member in [
        "crates/ngkg-distributed-build",
        "services/distributed-worker",
        "services/distributed-operator",
    ]:
        require(member in members, f"workspace omits {member}", errors)

    build = read("crates/ngkg-distributed-build/src/lib.rs")
    for token in [
        "parse_trig(",
        "parse_nquads(",
        "fact_hash",
        "merge_sorted_files",
        "DuplicatePolicy::Reject",
        "logical source partition coverage is incomplete",
        "compare_roots",
    ]:
        require(token in build, f"distributed build omits {token}", errors)
    require("split_at" not in build and "read_to_string" not in build,
            "distributed planner appears to split raw TriG text", errors)

    worker = read("services/distributed-worker/src/object_stage.rs")
    for token in [
        "get_distributed_work",
        "materialize_verified",
        "put_file_immutable",
        "commit_distributed_work",
        "commit_distributed_root",
        "projection.work_index % summary.reducer_count != index",
    ]:
        require(token in worker, f"object worker omits {token}", errors)
    require("list(" not in worker and "list_with_delimiter" not in worker,
            "object worker may list storage", errors)

    operator = read("services/distributed-operator/src/main.rs")
    for token in [
        'completion_mode: indexed.then(|| "Indexed".to_owned())',
        "backoff_limit_per_index: indexed.then_some(3)",
        "max_failed_indexes: indexed.then_some(0)",
        '"OMP_NUM_THREADS"',
        '"OPENBLAS_NUM_THREADS"',
        '"MKL_NUM_THREADS"',
        '"ngkg.io/network-plane"',
        "get_distributed_root",
        "kueue.x-k8s.io/queue-name",
        "ephemeral-storage",
    ]:
        require(token in operator, f"distributed operator omits {token}", errors)

    migration = read("migrations/0003_distributed_build.sql")
    for table in ["distributed_plan", "distributed_work", "distributed_root"]:
        require(f"CREATE TABLE {table}" in migration, f"migration omits {table}", errors)
        require(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY" in migration,
                f"{table} does not force RLS", errors)

    api = read("api/openapi.yaml")
    require("distributedBuild:" in api and "DistributedBuild:" in api,
            "OpenAPI omits distributed build status", errors)

    matrix = json.loads(read("test-corpus/distributed/build-equivalence-v1.json"))
    require(len(matrix["logicalPartitionCounts"]) >= 2, "matrix lacks topology variation", errors)
    require(len(matrix["reducerCounts"]) >= 2, "matrix lacks reducer variation", errors)
    for field, value in matrix["compare"].items():
        require(value is True, f"matrix comparison {field} is not mandatory", errors)

    platform = list(yaml.safe_load_all(read("charts/ngkg-platform/values.yaml")))[0]
    require(platform["distributedOperator"]["logicalPartitions"] >=
            platform["distributedOperator"]["reducerCount"],
            "reducerCount exceeds logicalPartitions", errors)
    responsibilities = {
        stage["queue"] for stage in platform["distributedOperator"]["stages"].values()
    }
    require(bool(responsibilities), "distributed stages have no Kueue queues", errors)

    result = {"phase": "15", "status": "failed" if errors else "passed", "errors": errors}
    print(json.dumps(result, indent=2))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
