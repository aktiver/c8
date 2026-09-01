#!/usr/bin/env python3
"""Fail-closed source/contract gate for Enterprise Stabilization Phase 4."""
from __future__ import annotations

import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]


def text(path: str) -> str:
    value = (ROOT / path).read_text(encoding="utf-8")
    if not value:
        raise RuntimeError(f"empty required file: {path}")
    return value


def require(path: str, *needles: str) -> None:
    value = text(path)
    for needle in needles:
        if needle not in value:
            raise RuntimeError(f"{path}: missing {needle!r}")


def main() -> int:
    require(
        "migrations/0010_runtime_correctness.sql",
        "CREATE TABLE orchestration_stage",
        "CREATE TABLE source_upload_reservation",
        "measured_cpu_time_millis",
        "participating_pod_uids",
        "autoscaling_events",
    )
    require(
        "services/operator/src/main.rs",
        "reserve_orchestration_stage",
        "complete_orchestration_stage",
        "SOURCE_IMPORT_FINALIZER",
        "cleanup_import_runtime",
        '.owns(jobs.clone(), watcher::Config::default())',
        'persistent_volume_reclaim_policy: Some("Delete".to_owned())',
    )
    require(
        "crates/ngkg-kube/src/lib.rs",
        "source_import_status_apply_document",
        "owned_camel_case_fields",
    )
    for path in (
        "services/reference-worker/src/cloud_import.rs",
        "services/reference-worker/src/cloud_decode.rs",
        "services/reference-worker/src/cloud_semantic.rs",
        "services/reference-worker/src/cloud_ontology.rs",
        "services/reference-worker/src/cloud_offline.rs",
        "services/reference-worker/src/cloud_activate.rs",
    ):
        require(path, "Patch::Apply", "source_import_status_apply_document")
    require(
        "services/api/src/main.rs",
        "reserve_source_upload",
        "publish_source_upload",
        "put_file_immutable",
    )
    require(
        "crates/ngkg-artifact-store/src/lib.rs",
        "MicrosoftAzureBuilder",
        "GoogleCloudStorageBuilder",
        "copy_if_not_exists",
        "PutMode::Create",
    )
    require(
        "services/online-serving/src/main.rs",
        "cgroup_resource_sample",
        "COORDINATOR_CGROUP_INTERVAL",
        "sha256_path_off_thread",
        "select_sparql_solution_format(&headers)?",
    )
    require(
        "crates/ngkg-federation/src/lib.rs",
        "to_ipv4_mapped",
        "clients: Arc<Mutex",
        "0x0064",
    )
    require(
        "crates/ngkg-locator/src/lib.rs",
        "MmapOptions::new().len(bytes_len).map",
        "read-only",
    )
    require(
        "crates/ngkg-reference/src/direct_exact.rs",
        "ngkg-graph-scoped-blank-node-v1",
        "exact_lane_uses_snapshot_graph_scoped_blank_identity",
    )
    require(
        "crates/ngkg-reference/src/datatype_policy.rs",
        "OWL_DIRECT_DATATYPE_POLICY_ID",
    )
    policy = json.loads(text("policies/owl-direct-datatype-policy.json"))
    if policy["policyId"] != "ngkg-owl2-direct-datatype-policy-v1":
        raise RuntimeError("datatype policy version drift")
    iris = [entry["iri"] for entry in policy["supportedDatatypes"]]
    if iris != sorted(set(iris)):
        raise RuntimeError("datatype map is not sorted and duplicate-free")
    print("Enterprise Stabilization Phase 4 source/contract gate passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, ValueError, RuntimeError) as error:
        print(f"Phase 4 gate failed: {error}", file=sys.stderr)
        raise SystemExit(1)
