#!/usr/bin/env python3
"""Fail-closed structural gate for the Phase 40.13.11 cloud compiler handoff."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def require(path: str, tokens: tuple[str, ...]) -> None:
    text = (ROOT / path).read_text(encoding="utf-8")
    missing = [token for token in tokens if token not in text]
    if missing:
        raise RuntimeError(f"{path} lacks required Phase 40.13.11 tokens: {missing}")


def main() -> None:
    for relative in (
        "contracts/cloud-source-manifest.schema.json",
        "contracts/cloud-decode-plan.schema.json",
        "contracts/cloud-compiler-handoff.schema.json",
        "charts/ngkg-platform/values.schema.json",
    ):
        json.loads((ROOT / relative).read_text(encoding="utf-8"))

    require(
        "crates/ngkg-source-planner/src/lib.rs",
        (
            "whole-trig-lpt-v1",
            "plan_cloud_decode",
            "target_work_bytes",
            "work_items",
            "cloud_plan_is_deterministic_and_never_splits_an_object",
        ),
    )
    require(
        "services/reference-worker/src/cloud_decode.rs",
        (
            "RdfFormat::TriG",
            "RdfFormat::NQuads",
            "source_sha256",
            "blank_node_scope",
            "verify_remote",
            "CompilerHandoffPublished",
            "verified completion set does not cover every source object exactly once",
        ),
    )
    require(
        "services/operator/src/main.rs",
        (
            'completion_mode: indexed.then(|| "Indexed".to_owned())',
            "backoff_limit_per_index: indexed.then_some(3)",
            "max_failed_indexes: indexed.then_some(0)",
            '"kueue.x-k8s.io/queue-name"',
            '"ngkg.io/workload".to_owned(),',
            '"source-ingestion".to_owned(),',
            '"OMP_NUM_THREADS"',
            '"OPENBLAS_NUM_THREADS"',
            '"MKL_NUM_THREADS"',
        ),
    )
    require(
        "charts/ngkg-platform/templates/operator.yaml",
        (
            "NGKG_CLOUD_DECODE_MAX_PARALLELISM",
            "NGKG_CLOUD_DECODE_TARGET_WORK_BYTES",
            "NGKG_CLOUD_DECODE_MAX_FRAGMENT_BYTES",
        ),
    )
    forbidden = "https://" + "semkg.io/graph/"
    offenders = []
    for path in ROOT.rglob("*"):
        if path.is_file() and path.name != "FILE_MANIFEST_SHA256.txt":
            try:
                if forbidden in path.read_text(encoding="utf-8"):
                    offenders.append(str(path.relative_to(ROOT)))
            except UnicodeDecodeError:
                pass
    if offenders:
        raise RuntimeError(f"retired graph namespace remains in: {offenders}")
    require(
        "crates/ngkg-online-reasoning/src/lib.rs",
        (
            'const SEMKG_GRAPH_PREFIX: &str = "https://c8-next-generation.io/";',
            'Some("semkg") => Ok(OntologyGraphRole::AssertedOntology)',
            'Some("closure") => Ok(OntologyGraphRole::FiniteClosure)',
            'Some("provenance") => Ok(OntologyGraphRole::Provenance)',
        ),
    )
    print(json.dumps({"phase": "40.13.11", "status": "passed", "checks": 5}))


if __name__ == "__main__":
    main()
