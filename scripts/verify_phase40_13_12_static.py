#!/usr/bin/env python3
"""Fail-closed structural acceptance checks for Phase 40.13.12."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needles: list[str]) -> None:
    text = (ROOT / path).read_text(encoding="utf-8")
    for needle in needles:
        if needle not in text:
            raise SystemExit(f"{path}: missing {needle!r}")


def main() -> None:
    for path in [
        "contracts/semantic-compilation-root.schema.json",
        "contracts/semantic-partition.schema.json",
        "charts/ngkg-platform/values.schema.json",
    ]:
        json.loads((ROOT / path).read_text(encoding="utf-8"))

    require(
        "crates/ngkg-semantic-compiler/src/lib.rs",
        [
            "pub fn map_fragment(",
            "std::thread::scope",
            "EXTERNAL_MERGE_FAN_IN",
            "hierarchical_merge_unique",
            "pub fn finalize_dictionary(",
            "guid-dictionary.tsv",
            "pub fn reduce_partition(",
            "facts.parquet",
            "adjacency-forward.tsv",
            "adjacency-reverse.tsv",
            "semantic-index.tsv",
            'authorization_state: "unqualified"',
            'publication_state: "inactive"',
            "pending-owl2-dl-snapshot-qualification",
            "https://c8-next-generation.io/",
            "object-scoped-blank-node-v1",
        ],
    )
    require(
        "services/reference-worker/src/cloud_semantic.rs",
        [
            "execute_map",
            "execute_dictionary",
            "execute_partition",
            "execute_finalize",
            "verify_remote",
            "buffer_unordered",
            "semantic-compilation",
            "SemanticCompilationCompleteInactive",
        ],
    )
    require(
        "services/operator/src/main.rs",
        [
            "reconcile_import_semantic",
            "cloud-semantic-map",
            "cloud-semantic-dictionary",
            "cloud-semantic-partition",
            "cloud-semantic-finalize",
            '"Indexed"',
            '"semantic-projection"',
            "semantic-map-max-parallelism",
            "semantic-partition-max-parallelism",
        ],
    )
    require(
        "crates/ngkg-kube/src/lib.rs",
        [
            "semantic_map_job_name",
            "semantic_dictionary_sha256",
            "semantic_partition_job_name",
            "semantic_compilation_root_sha256",
            "compiled_fact_count",
        ],
    )
    require(
        "charts/ngkg-platform/templates/operator.yaml",
        [
            "NGKG_SEMANTIC_MAP_MAX_PARALLELISM",
            "NGKG_SEMANTIC_SCRATCH_SIZE",
            "NGKG_SEMANTIC_PARTITION_MAX_PARALLELISM",
            "NGKG_SEMANTIC_MAP_ROWS_IN_MEMORY",
            "NGKG_SEMANTIC_FINALIZE_CONCURRENCY",
        ],
    )
    root_schema = json.loads(
        (ROOT / "contracts/semantic-compilation-root.schema.json").read_text(
            encoding="utf-8"
        )
    )
    props = root_schema["properties"]
    assert props["authorizationState"] == {"const": "unqualified"}
    assert props["publicationState"] == {"const": "inactive"}
    assert props["qualificationState"] == {
        "const": "pending-owl2-dl-snapshot-qualification"
    }

    changed = [
        ROOT / "crates/ngkg-semantic-compiler/src/lib.rs",
        ROOT / "services/reference-worker/src/cloud_semantic.rs",
        ROOT / "services/operator/src/main.rs",
    ]
    forbidden = [
        "ontology alignment",
        "raw data mapping",
        "alignment graph",
        "activate_snapshot",
    ]
    for path in changed:
        lowered = path.read_text(encoding="utf-8").lower()
        for token in forbidden:
            if token in lowered:
                raise SystemExit(f"{path}: forbidden Phase 40.13.12 scope {token!r}")

    print(json.dumps({"phase": "40.13.12", "status": "passed", "checks": 8}))


if __name__ == "__main__":
    main()
