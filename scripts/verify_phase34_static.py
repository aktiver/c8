#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 34 direct primary partitioning."""

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
    serving = require(
        "services/online-serving/src/main.rs",
        (
            "struct ValidatedFragmentSpool",
            "always_bound_variables",
            "fn validate(lease: FragmentResponseLease",
            "fn materialize(self, max_rows: usize)",
            "fn create_iter<L, R>",
            "fn spill_rows<I>",
            "fn create_primary_shuffle_spill",
            "direct_spool_to_primary_partition_v1",
            "bounded_owned_fallback_v1",
            "fragment_owned_rows",
            "ngkg_coordinator_direct_partition_fragments_total",
            "ngkg_coordinator_direct_partition_rows_total",
            "incremental_primary_partitioning_matches_owned_rows_and_cleans_source_failure",
        ),
    )
    distributed = serving.index("async fn execute_distributed_query")
    primary = serving.index("fn create_primary_shuffle_spill", distributed)
    shuffle = serving.index("async fn execute_partitioned_shuffle", primary)
    ingress = serving[distributed:primary]
    if ".into_batch()" in ingress or "fragment_bindings.push" in ingress:
        raise RuntimeError("distributed ingress still builds complete fragment binding vectors")
    validated = ingress.index("ValidatedFragmentSpool::validate")
    summarized = ingress.index("ValidatedFragmentSpool::summary", validated)
    dispatched = ingress.index("execute_partitioned_shuffle(", summarized)
    if not validated < summarized < dispatched:
        raise RuntimeError("fragment spools are not validated, summarized and dispatched in order")

    response_validation = serving.index("fn validate_shuffle_response_spool", primary)
    direct = serving[primary:response_validation]
    opened = direct.index("ValidatedFragmentSpoolSequence::new")
    iterator = direct.index("ShuffleSpillStage::create_iter", opened)
    if not opened < iterator or ".into_batch()" in direct:
        raise RuntimeError("primary partitioning is not driven by incremental fragment iterators")
    runtime = serving[shuffle:serving.index("async fn execute_shuffle_partition", shuffle)]
    created = runtime.index("create_primary_shuffle_spill(")
    streamed = runtime.index("write_shuffle_join_stream_iter", created)
    certified = runtime.index("partitioned shuffle did not execute", streamed)
    if not created < streamed < certified:
        raise RuntimeError("direct partitions can bypass streamed execution or worker checks")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    version = tuple(int(part) for part in str(openapi["info"]["version"]).split("."))
    if version != (1, 0, 0) and version < (1, 8, 0):
        raise RuntimeError("online OpenAPI was not advanced for direct partitioning")
    execution = openapi["components"]["schemas"]["Execution"]
    required = set(execution["required"])
    if not {"fragmentMaterializationMode", "fragmentOwnedRows"}.issubset(required):
        raise RuntimeError("public execution evidence omits fragment materialization")
    modes = set(execution["properties"]["fragmentMaterializationMode"]["enum"])
    if not {"none", "direct_spool_to_primary_partition_v1", "bounded_owned_fallback_v1"}.issubset(modes):
        raise RuntimeError("OpenAPI omits a materialization execution mode")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("RKE2 scaling target exceeds 80 percent")
    query = values["resources"]["query"]
    if query["requests"] != query["limits"] or "ephemeral-storage" not in query["requests"]:
        raise RuntimeError("query role lacks Guaranteed-QoS ephemeral storage")
    require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        ("fragment-response-spool", "shuffle-spill", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"),
    )
    require(
        "scripts/qualify_phase34.sh",
        (
            "direct_spool_to_primary_partition_v1",
            "fragmentOwnedRows == 0",
            "ngkg_coordinator_direct_partition_fragments_total",
            "ngkg_fragment_response_spool_active_bytes",
            "cmp",
        ),
    )
    require("docs/phases/PHASE_34.md", ("Acceptance criteria", "Honest boundary", "80 percent", "BLAS", "mmap", "Parquet"))
    require("verification/phase-34.json")
    print("Phase 34 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"phase 34 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
