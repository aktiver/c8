#!/usr/bin/env python3
"""Fail-closed structural acceptance checks for Phase 40.13.13."""

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
    schemas = [
        "contracts/ontology-qualification-request.schema.json",
        "contracts/ontology-projection.schema.json",
        "contracts/ontology-assembly.schema.json",
        "contracts/ontology-qualification-root.schema.json",
        "contracts/owl-profile-qualification.schema.json",
        "contracts/owl-consistency-qualification.schema.json",
        "charts/ngkg-platform/values.schema.json",
    ]
    for path in schemas:
        json.loads((ROOT / path).read_text(encoding="utf-8"))

    require(
        "crates/ngkg-ontology-qualifier/src/lib.rs",
        [
            "pub fn validate_qualification_request(",
            "pub fn project_partition(",
            "pub fn assemble_snapshot_ontology(",
            "pub fn build_hermit_request(",
            "pub fn execute_hermit(",
            "pub fn finalize_qualification(",
            "complete_pinned_import_closure",
            "every ontology module must contain exactly one owl:Ontology header",
            "unresolved or unpinned owl:imports target",
            'reasoner_version != "1.4.5.519"',
            'qualification_state: "owl2-dl-qualified"',
            'publication_state: "inactive"',
            "https://c8-next-generation.io/",
        ],
    )
    require(
        "services/reference-worker/src/cloud_ontology.rs",
        [
            "execute_project",
            "execute_assemble",
            "execute_qualify",
            "materialize_verified",
            "buffer_unordered",
            "Owl2DlSnapshotQualifiedInactive",
            '!= "1.4.5.519"',
        ],
    )
    require(
        "services/operator/src/main.rs",
        [
            "reconcile_import_ontology",
            "cloud-ontology-project",
            "cloud-ontology-assemble",
            "cloud-ontology-qualify",
            '"ontology-project"',
            '"ontology-qualify"',
            '"reasoning"',
            "ontology-project-max-parallelism",
        ],
    )
    require(
        "crates/ngkg-kube/src/lib.rs",
        [
            "ontology_qualification_request_object_key",
            "ontology_projection_job_name",
            "ontology_assembly_sha256",
            "ontology_qualification_root_sha256",
        ],
    )
    require(
        "charts/ngkg-platform/templates/operator.yaml",
        [
            "NGKG_ONTOLOGY_PROJECT_MAX_PARALLELISM",
            "NGKG_ONTOLOGY_REASONER_HEAP_MIB",
            "NGKG_ONTOLOGY_REASONER_TIMEOUT_SECONDS",
            "NGKG_ONTOLOGY_MAX_NAMED_INDIVIDUALS",
        ],
    )
    values = (ROOT / "charts/ngkg-platform/values.yaml").read_text(encoding="utf-8")
    for needle in [
        "projectMaxParallelism: '256'",
        "reasonerHeapMiB: '98304'",
        "reasonerTimeoutSeconds: '14400'",
    ]:
        if needle not in values:
            raise SystemExit(f"production values missing {needle!r}")

    pom = (ROOT / "adapters/hermit-reasoner/pom.xml").read_text(encoding="utf-8")
    if "<version>1.4.5.519</version>" not in pom:
        raise SystemExit("HermiT is not pinned to 1.4.5.519")

    request_schema = json.loads(
        (ROOT / "contracts/ontology-qualification-request.schema.json").read_text(
            encoding="utf-8"
        )
    )
    graph_pattern = request_schema["$defs"]["assertedGraphIri"]["pattern"]
    if "c8-next-generation" not in graph_pattern or "semkg" not in graph_pattern:
        raise SystemExit("qualification request does not constrain asserted semkg graphs")

    root_schema = json.loads(
        (ROOT / "contracts/ontology-qualification-root.schema.json").read_text(
            encoding="utf-8"
        )
    )
    assert root_schema["properties"]["publicationState"] == {"const": "inactive"}
    assert root_schema["properties"]["profileValid"] == {"const": True}
    assert root_schema["properties"]["consistent"] == {"const": True}

    api = (ROOT / "api/openapi.yaml").read_text(encoding="utf-8").lower()
    for forbidden in ["alignmentrules", "mappingrules", "rawdatamapping"]:
        if forbidden in api:
            raise SystemExit(f"forbidden alignment/mapping API surface: {forbidden}")

    print(json.dumps({"phase": "40.13.13", "status": "passed", "checks": 10}))


if __name__ == "__main__":
    main()
