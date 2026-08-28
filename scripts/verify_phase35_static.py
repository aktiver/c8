#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 35 out-of-core stage results."""

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
            "struct FragmentReplay",
            "struct ValidatedFragmentSpoolSequence",
            "impl Iterator for ValidatedFragmentSpoolSequence",
            "fragment spool replay differs from its validated row count",
            "fn validate_shuffle_response_spool",
            "fn materialize_fragment_spools",
            "coordinator_spooled_shuffle_responses",
            "ngkg_coordinator_spooled_shuffle_response_bytes_total",
            "partition_spool_sequence_v1",
            "assembled_intermediate_owned_rows",
            "validated_spool_sequence_replays_multiple_partitions_without_assembly",
            "shuffle_response_spool_validates_exact_multiset_and_partition",
        ),
    )
    shuffle = serving.index("async fn execute_partitioned_shuffle")
    worker = serving.index("async fn execute_shuffle_partition", shuffle)
    production = serving[shuffle:worker]
    if "read_bounded_response(response, max_response_bytes)" in production:
        raise RuntimeError("shuffle responses still accumulate complete byte vectors")
    if "let mut stage_left" in production or "stage_left.extend" in production:
        raise RuntimeError("shuffle stages still assemble one coordinator row vector")
    received = production.index(".receive(response, max_response_bytes)")
    validated = production.index("validate_shuffle_response_spool(", received)
    retained = production.index("spools: stage_spools", validated)
    materialized = production.index("materialize_fragment_spools", retained)
    if not received < validated < retained < materialized:
        raise RuntimeError("stage results are not spooled, validated, retained and finally materialized in order")

    sequence = serving.index("impl Iterator for ValidatedFragmentSpoolSequence")
    opened = serving.index("self.open_next()", sequence)
    counted = serving.index("replay.observed_rows.checked_add", opened)
    released = serving.index("drop(lease)", counted)
    if not sequence < opened < counted < released:
        raise RuntimeError("lazy spool replay bypasses opening, exact row count or lease release")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    version = tuple(int(part) for part in str(openapi["info"]["version"]).split("."))
    if version != (1, 0, 0) and version < (1, 9, 0):
        raise RuntimeError("online OpenAPI predates the Phase 35 stage-result spooling contract")
    execution = openapi["components"]["schemas"]["Execution"]
    required = set(execution["required"])
    evidence = {
        "shuffleResultIngressMode",
        "shuffleResultIngressBytes",
        "intermediateResultMode",
        "assembledIntermediateOwnedRows",
    }
    if not evidence.issubset(required):
        raise RuntimeError("public execution evidence omits stage-result boundaries")
    if "partition_spool_sequence_v1" not in execution["properties"]["intermediateResultMode"]["enum"]:
        raise RuntimeError("OpenAPI omits the implemented intermediate mode")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    online = values["onlineServing"]
    if int(online["maxShuffleExchangeBytes"]) > int(online["maxFragmentResponseSpoolBytes"]):
        raise RuntimeError("response spool cannot contain the admitted shuffle exchange")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("RKE2 scaling target exceeds 80 percent")
    require(
        "scripts/validate_helm_values.py",
        ("maxShuffleExchangeBytes cannot exceed maxFragmentResponseSpoolBytes",),
    )
    require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        ("fragment-response-spool", "shuffle-spill", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"),
    )
    require(
        "scripts/qualify_phase35.sh",
        (
            "fragmentCount >= 3",
            "shuffleResultIngressMode",
            "partition_spool_sequence_v1",
            "assembledIntermediateOwnedRows == 0",
            "ngkg_coordinator_spooled_shuffle_responses_total",
            "ngkg_fragment_response_spool_active_bytes",
            "cmp",
        ),
    )
    require("docs/phases/PHASE_35.md", ("Acceptance criteria", "Honest boundary", "80 percent", "BLAS", "mmap", "Parquet"))
    require("verification/phase-35.json")
    print("Phase 35 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"phase 35 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
